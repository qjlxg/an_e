import pandas as pd
import requests
from bs4 import BeautifulSoup
import os
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- ⚠️ 依赖的库: requests, pandas, beautifulsoup4 ---
OUTPUT_FILE = 'fund_details.csv'
INPUT_FILE = 'result_z.txt'
# 降低并发数，保护接口，防止被封
MAX_WORKERS = 5 

# 东方财富基金详情页 URL 结构
BASE_URL = "https://fund.eastmoney.com/{fund_code}.html"


def fetch_fund_info(fund_code):
    """
    爬取基金详情页面，使用 BeautifulSoup 提取基本信息。
    """
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 正在查询代码: {fund_code}")
    
    url = BASE_URL.format(fund_code=fund_code)
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status() 
        
        # 使用 BeautifulSoup 解析 HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 提取基金名称 (位于头部)
        title_tag = soup.find('div', class_='fundTitle')
        fund_name = title_tag.find('h4').text.strip() if title_tag and title_tag.find('h4') else 'N/A'
        
        # 提取基本信息表格 (如成立日期、基金管理人等)
        info_dict = {}
        info_div = soup.find('div', class_='info')
        if info_div:
            # 找到所有 li 标签，提取信息
            list_items = info_div.find_all('li')
            for item in list_items:
                text = item.text.strip().replace('\xa0', ' ')
                if '成立日期' in text:
                    info_dict['成立日期'] = text.split('：')[-1].split('(')[0].strip()
                elif '基金管理人' in text:
                    info_dict['基金管理人'] = text.split('：')[-1].strip()
                elif '基金托管人' in text:
                    info_dict['基金托管人'] = text.split('：')[-1].strip()

        # 提取现任基金经理
        manager_name = 'N/A'
        manager_div = soup.find('div', class_='manager-table')
        if manager_div:
            # 找到表格中第一个基金经理的姓名
            name_tag = manager_div.find('a', target='_blank')
            manager_name = name_tag.text.strip() if name_tag else 'N/A'


        # 整理数据
        details = {
            '基金代码': fund_code,
            '基金名称': fund_name,
            '基金管理人': info_dict.get('基金管理人', 'N/A'),
            '基金经理': manager_name,
            '成立日期': info_dict.get('成立日期', 'N/A'),
            '基金托管人': info_dict.get('基金托管人', 'N/A'),
            '更新时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        time.sleep(0.5) # 增加延迟，降低爬虫频率
        return details
        
    except requests.exceptions.RequestException as e:
        print(f"基金代码 {fund_code} 请求失败: {e}")
        time.sleep(1) 
        return {
            '基金代码': fund_code,
            '基金名称': '网络请求/解析失败',
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
