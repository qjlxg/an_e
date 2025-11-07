import pandas as pd
import requests
from bs4 import BeautifulSoup
import os
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- 依赖的库: requests, pandas, beautifulsoup4 ---
OUTPUT_FILE = 'fund_details.csv'
INPUT_FILE = 'result_z.txt'
# 保持较低的并发数，避免被爬虫目标网站封锁
MAX_WORKERS = 5 

# 🚨 修正后的 URL 结构：指向基金档案 (F10) 的基本概况页
BASE_URL = "https://fundf10.eastmoney.com/jbgk_{fund_code}.html"


def fetch_fund_info(fund_code):
    """
    爬取基金详情页面，使用 BeautifulSoup 提取完整的基金基本信息。
    采用新的解析策略：直接从“基本概况”表格中提取所有信息。
    """
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 正在查询代码: {fund_code}")
    
    url = BASE_URL.format(fund_code=fund_code)
    
    # 模拟 HTTP 请求头
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    }

    # 默认值
    details = {
        '基金代码': fund_code,
        '基金名称': 'N/A',
        '基金管理人': 'N/A',
        '基金经理': 'N/A',
        '成立日期': 'N/A',
        '基金托管人': 'N/A',
        '更新时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

    try:
        # 增加超时时间到 20 秒，提高请求稳定性
        response = requests.get(url, headers=headers, timeout=20)
        response.raise_for_status() # 检查 HTTP 状态码
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # --- 1. 提取基本概况表格数据 (最详细可靠的来源) ---
        # 定位到“基本概况”下的信息表格
        info_table = soup.select_one('div.boxitem table.info')
        
        if info_table:
            info_map = {}
            # 提取表格中所有 key-value 对
            cells = info_table.find_all('td')
            
            # 遍历单元格，按两两一组（key/value）提取
            i = 0
            while i < len(cells):
                key_cell = cells[i]
                key = key_cell.text.strip()
                
                # 检查下一个单元格是否存在
                if i + 1 < len(cells):
                    value_cell = cells[i+1]
                    value = value_cell.text.strip()
                    info_map[key] = value

                    # 如果 value 单元格有 colspan="3" 属性，说明它跨越了三列（占了 key 和 value 的位置）
                    # 在这种情况下，下一次迭代应该跳过三个单元格，即 i += 4
                    # 实际观察中，表格是两列结构，所以我们总是跳过 i += 2
                    i += 2 
                else:
                    break
            
            # 使用提取的 map 填充 details
            details['基金名称'] = info_map.get('基金全称', details['基金名称'])
            details['基金管理人'] = info_map.get('基金管理人', details['基金管理人'])
            details['基金托管人'] = info_map.get('基金托管人', details['基金托管人'])
            
            # 成立日期和规模在一起，需要分割并清洗
            if '成立日期/规模' in info_map:
                # 示例: 2021年12月02日 / 1.182亿份
                date_str = info_map['成立日期/规模'].split('/')[0].strip()
                # 清洗格式为 YYYY-MM-DD
                details['成立日期'] = date_str.replace('年', '-').replace('月', '-').replace('日', '').strip('-').strip()
            
            # 基金经理可能在 '基金经理人' 字段中
            details['基金经理'] = info_map.get('基金经理人', details['基金经理'])
        
        
        # --- 2. 补充/覆盖 基金经理 (从快速概览中获取，防止表格缺失) ---
        # 查找快速概览区域的基金经理 a 标签 (更直接)
        manager_tag = soup.select_one('.bs_gl label:has(a[href*="manager"]) a')
        if manager_tag:
            details['基金经理'] = manager_tag.text.strip()


        # --- 3. 补充 基金名称 (如果表格中没有基金全称，则从页面大标题获取) ---
        if details['基金名称'] == 'N/A' or details['基金名称'] == '':
            title_tag = soup.select_one('.basic-new .col-left .title a')
            if title_tag:
                full_name_text = title_tag.text.strip()
                # 格式如: 汇添富中证芯片产业指数增强发起式... (014194)
                details['基金名称'] = full_name_text.split('(')[0].strip()

        
        time.sleep(0.2) # 保持短延迟，降低爬虫频率
        return details
        
    except requests.exceptions.RequestException as e:
        print(f"基金代码 {fund_code} 请求失败: {e}")
        time.sleep(1) 
        # 发生网络错误时，返回错误信息
        return {
            '基金代码': fund_code,
            '基金名称': '网络请求失败',
            '基金管理人': 'N/A',
            '基金经理': 'N/A',
            '成立日期': 'N/A',
            '基金托管人': 'N/A',
            '更新时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }


def main():
    
    # 1. 读取基金代码
    print(f"尝试读取文件: {INPUT_FILE}")
    try:
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            fund_codes = [line.strip() for line in f if line.strip()]
        
        fund_codes = list(dict.fromkeys(fund_codes))
        print(f"成功读取 {len(fund_codes)} 个基金代码。")
        
    except FileNotFoundError:
        print(f"错误: 找不到输入文件 {INPUT_FILE}")
        return
    
    # 2. 批量并行获取基金信息
    all_fund_details = []
    print(f"开始并行获取基金基本信息，最大线程数: {MAX_WORKERS}...")
    
    if 'pd' not in globals():
        print("致命错误：缺少 pandas 库。请检查依赖安装步骤。")
        return

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_code = {executor.submit(fetch_fund_info, code): code for code in fund_codes}
        
        for future in as_completed(future_to_code):
            try:
                data = future.result()
                all_fund_details.append(data)
            except Exception as exc:
                print(f'一个线程执行发生错误: {exc}')

    print("所有基金信息获取和处理完成。")
    
    # 3. 转换为 DataFrame 并保存为 CSV
    if not all_fund_details:
        print("没有获取到任何有效数据，跳过文件保存。")
        return

    df = pd.DataFrame(all_fund_details)
    df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
        
    print(f"所有基金信息已保存到 CSV 文件: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
