import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import os
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- 配置 ---
# 设置请求头，模拟浏览器访问，防止被反爬
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
BASE_URL = "http://fundf10.eastmoney.com/jjjl_{code}.html"
FUND_CODES_FILE = "C类.txt"
# 设置上海时区
SHA_TZ = timezone(timedelta(hours=8), name='Asia/Shanghai')
# 设置并行线程数，通常10-20个线程对于网络抓取是一个不错的起点
MAX_WORKERS = 10 

def get_shanghai_time():
    """获取当前的上海时区时间"""
    return datetime.now(SHA_TZ)

def scrape_fund_data(fund_code):
    """
    抓取单个基金的详细数据
    返回 (基金代码, [数据列表]) 或 (基金代码, None)
    """
    url = BASE_URL.format(code=fund_code)
    print(f"-> 正在抓取基金代码: {fund_code}")
    
    try:
        # 使用超时设置防止单个请求卡住
        response = requests.get(url, headers=HEADERS, timeout=15) 
        response.encoding = 'utf-8' 
        if response.status_code != 200:
            print(f"   请求失败，状态码: {response.status_code} - {fund_code}")
            return fund_code, None
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 1. 抓取基本资料
        basic_info = {'基金代码': fund_code}
        bs_gl_div = soup.find('div', class_='bs_gl')
        if bs_gl_div:
            labels = bs_gl_div.find_all('label')
            for label in labels:
                text = label.get_text(strip=True)
                if '：' in text:
                    match = re.search(r'(.+?)：\s*(.+)', text)
                    if match:
                        key = match.group(1).strip()
                        value = match.group(2).strip()
                        
                        if key == '资产规模':
                             # 特殊处理资产规模和截止日期
                            scale_match = re.search(r'(.+?)（截止至：(.+?)）', value)
                            if scale_match:
                                basic_info['资产规模'] = scale_match.group(1).strip()
                                basic_info['截止至'] = scale_match.group(2).strip()
                            else:
                                basic_info['资产规模'] = value
                                basic_info['截止至'] = ''
                        elif key:
                            basic_info[key] = value

        # 2. 抓取基金经理变动一览
        manager_changes_list = []
        manager_table_container = soup.find('div', class_='boxitem w790')
        if manager_table_container:
            manager_table = manager_table_container.find('table', class_='comm jloff')
            if manager_table:
                tbody = manager_table.find('tbody')
                if tbody:
                    rows = tbody.find_all('tr')
                    for row in rows:
                        cols = row.find_all(['td', 'th'])
                        if len(cols) >= 5:
                            change_entry = {
                                '起始期': cols[0].get_text(strip=True),
                                '截止期': cols[1].get_text(strip=True),
                                '基金经理(变动)': cols[2].get_text(strip=True),
                                '任职期间': cols[3].get_text(strip=True),
                                '任职回报': cols[4].get_text(strip=True)
                            }
                            manager_changes_list.append(change_entry)

        # 3. 合并数据
        base_data = {
            '成立日期': basic_info.get('成立日期', ''),
            '基金经理(现任)': basic_info.get('基金经理', ''),
            '类型': basic_info.get('类型', ''),
            '管理人': basic_info.get('管理人', ''),
            '资产规模': basic_info.get('资产规模', ''),
            '截止至': basic_info.get('截止至', '')
        }
        
        fund_records = []
        if manager_changes_list:
            for change in manager_changes_list:
                record = {'基金代码': fund_code}
                record.update(base_data)
                record.update(change)
                fund_records.append(record)
        else:
            # 没有变动数据，只返回一条基本信息记录
            record = {'基金代码': fund_code}
            record.update(base_data)
            fund_records = [record]

        print(f"   ✅ 成功抓取 {fund_code}")
        return fund_code, fund_records
        
    except Exception as e:
        print(f"   ❌ 抓取基金代码 {fund_code} 时发生错误: {e}")
        return fund_code, None

def main():
    """主函数，读取基金代码并生成总表"""
    if not os.path.exists(FUND_CODES_FILE):
        print(f"错误: 找不到基金代码文件 {FUND_CODES_FILE}。请创建此文件并填入基金代码。")
        return

    with open(FUND_CODES_FILE, 'r', encoding='utf-8') as f:
        fund_codes = [line.strip() for line in f if line.strip() and not line.startswith('#')]
    
    if not fund_codes:
        print(f"文件 {FUND_CODES_FILE} 中没有找到有效的基金代码。")
        return

    all_fund_data = []
    
    # 使用 ThreadPoolExecutor 实现并行抓取
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # 提交所有抓取任务
        future_to_code = {executor.submit(scrape_fund_data, code): code for code in fund_codes}
        
        # 处理完成的任务结果
        for future in as_completed(future_to_code):
            code = future_to_code[future]
            try:
                # 获取 scrape_fund_data 的返回值: (code, data)
                _, data = future.result() 
                if data:
                    all_fund_data.extend(data)
            except Exception as exc:
                print(f"基金 {code} 的处理生成了一个异常: {exc}")

    if not all_fund_data:
        print("\n未抓取到任何基金数据，无法生成总表。")
        return

    # 4. 生成CSV总表
    df = pd.DataFrame(all_fund_data)
    
    # 定义最终需要的列的顺序
    final_columns = [
        '基金代码', '成立日期', '基金经理(现任)', '类型', '管理人', '资产规模', '截止至',
        '起始期', '截止期', '基金经理(变动)', '任职期间', '任职回报'
    ]
    
    df = df.reindex(columns=final_columns, fill_value='')

    # 获取上海时区时间用于命名和路径
    now_sha = get_shanghai_time()
    timestamp = now_sha.strftime("%Y%m%d_%H%M%S")

    # 构建输出路径: 年/月/
    output_dir = now_sha.strftime("%Y/%m")
    # 构建输出文件名: 总表_YYYYMMDD_HHMMSS.csv
    output_filename = f"总表_{timestamp}.csv"
    
    # 创建目录
    os.makedirs(output_dir, exist_ok=True)
    
    # 完整输出路径
    output_path = os.path.join(output_dir, output_filename)
    
    # 保存CSV
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    print(f"\n==========================================")
    print(f"✅ 数据抓取完成，总表已保存到: {output_path}")
    print(f"抓取基金数量: {len(fund_codes)}, 成功记录数量: {len(all_fund_data)}")
    print(f"==========================================")

if __name__ == "__main__":
    main()
