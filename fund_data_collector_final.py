# fund_data_collector_final.py (最强鲁棒性并行模式)

import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import os
import time
import json
import random 
from multiprocessing.dummy import Pool as ThreadPool 
import threading

# --- 配置 ---
FUND_CODES_FILE = "C类.txt"
# 核心数据接口，用于获取持仓数据
BASE_DATA_URL = "http://fundf10.eastmoney.com/FundArchivesDatas.aspx?type=ccmx&code={fund_code}&qdii=&sdate=&edate=&rt={timestamp}"

# 核心配置：低并发，长延迟
MAX_WORKERS = 3        # 线程数：降至 3
DELAY_MIN = 3.0        # 最小延迟：3.0 秒
DELAY_MAX = 7.0        # 最大延迟：7.0 秒

# 线程锁
lock = threading.Lock() 

# --- 工具函数 ---

def get_output_dir():
    """返回当前的年/月目录 (上海时区)"""
    cst_time = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8)))
    return os.path.join(cst_time.strftime("%Y"), cst_time.strftime("%m"))

def fetch_holding_data(fund_code):
    """抓取指定基金代码的持仓表格 HTML 内容，返回HTML片段或 None。"""
    timestamp = time.time() * 1000
    url = BASE_DATA_URL.format(fund_code=fund_code, timestamp=timestamp)
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Referer': f'http://fundf10.eastmoney.com/ccmx_{fund_code}.html' # 使用您提供的页面类型作为Referer
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.encoding = 'utf-8'

        if response.status_code == 200:
            text = response.text.strip()
            
            if text.startswith('var apidata='):
                json_str = text.split('=', 1)[1].rstrip(';')
                
                try:
                    data = json.loads(json_str)
                    return data.get('content')
                except json.JSONDecodeError:
                    print(f"[{fund_code}] 错误: 无法解析返回的 JSON 内容 (JSONDecodeError)。")
                    return None
            else:
                print(f"[{fund_code}] 错误: 数据接口返回格式不正确 (未以 'var apidata=' 开头)。")
                return None
        else:
            print(f"[{fund_code}] 抓取失败，状态码: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"[{fund_code}] 抓取过程中发生网络错误: {e}")
        return None

def parse_and_save_data(fund_code, html_content):
    """解析 HTML 内容，提取持仓表格数据，并保存为 CSV 文件。"""
    if not html_content:
        return

    soup = BeautifulSoup(html_content, 'html.parser')
    tables = soup.find_all('table', class_='w780')
    
    if not tables:
        print(f"[{fund_code}] 未找到持仓表格。")
        return

    # 只处理第一个表格（最新季度数据）
    table = tables[0]
    
    try:
        # 1. 提取标题和日期
        title_tag = table.find_previous_sibling('h4')
        title = f"{fund_code}_股票投资明细"
        if title_tag:
            raw_title = title_tag.text.strip().replace('\r\n', ' ').replace('\n', ' ').replace('\xa0', ' ')
            
            parts = raw_title.split(' ')
            if len(parts) >= 3 and ('季度' in parts[-2] or '年度' in parts[-2]):
                 title = f"{parts[0]}_{parts[-3]}{parts[-2]}{parts[-1]}"
            else:
                 title = raw_title.replace(' ', '_')
        
        # 2. 提取行数据
        data_rows = []
        for row in table.find_all('tr')[1:]:
            cols = [col.text.strip().replace('\xa0', '').replace('\n', '').replace(' ', '') for col in row.find_all(['td'])]
            
            if len(cols) == 10:
                data_rows.append([cols[0], cols[1], cols[2], cols[3], cols[4], cols[6], cols[7], cols[8], cols[9]])
            elif len(cols) == 8:
                data_rows.append([cols[0], cols[1], cols[2], '', '', cols[4], cols[5], cols[6], cols[7]])
            else:
                continue

        final_headers = ['序号', '股票代码', '股票名称', '最新价', '涨跌幅', '占净值比例', '持股数（万股）', '持仓市值（万元）']
        
        df = pd.DataFrame(data_rows, columns=final_headers)
        
        # 生成时间戳和文件名
        cst_time = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8)))
        timestamp = cst_time.strftime("%Y%m%d%H%M%S")
        
        output_dir = get_output_dir()
        os.makedirs(output_dir, exist_ok=True)
        
        filename = f"{title}_{timestamp}.csv"
        filepath = os.path.join(output_dir, filename)
        
        df.to_csv(filepath, index=False, encoding='utf_8_sig')
        print(f"[{fund_code}] 成功保存数据到: {filepath}")
        
    except Exception as e:
        print(f"[{fund_code}] 解析或保存表格时发生错误: {e}")

# 并行处理函数
def process_fund(fund_code):
    """单个基金代码的完整处理流程，包括一次失败重试。"""
    
    # 第一次尝试：并行 + 随机延迟
    delay = random.uniform(DELAY_MIN, DELAY_MAX)
    time.sleep(delay) 
    
    print(f"[{fund_code}] (第一次尝试) 开始抓取... (延迟 {delay:.2f}s)")
    html_content = fetch_holding_data(fund_code)
    
    if html_content:
        parse_and_save_data(fund_code, html_content)
    else:
        # 抓取失败，进入重试逻辑
        print(f"[{fund_code}] 第一次抓取失败，开始超慢速重试...")
        
        # 第二次尝试：串行回退策略，强制超长等待
        retry_delay = 10.0 + random.uniform(0.0, 3.0) # 10到13秒延迟
        time.sleep(retry_delay)
        
        print(f"[{fund_code}] (第二次尝试/重试) 开始抓取... (延迟 {retry_delay:.2f}s)")
        html_content_retry = fetch_holding_data(fund_code)
        
        if html_content_retry:
            print(f"[{fund_code}] 第二次重试成功！")
            parse_and_save_data(fund_code, html_content_retry)
        else:
            print(f"[{fund_code}] 第二次抓取仍然失败，跳过该基金。")

# 主运行逻辑
def main():
    print(f"--- 开始运行基金数据收集脚本 (最鲁棒并行模式，线程数: {MAX_WORKERS}, 延迟: {DELAY_MIN}-{DELAY_MAX}s) ---")
    
    # 1. 读取基金代码
    fund_codes = []
    try:
        with open(FUND_CODES_FILE, 'r') as f:
            fund_codes = [line.strip() for line in f if line.strip() and line.strip() != 'code']
        
        if not fund_codes:
            print("错误: 基金代码文件为空或只包含标题行。")
            return

        print(f"读取到 {len(fund_codes)} 个基金代码。")
    except FileNotFoundError:
        print(f"错误: 基金代码文件 '{FUND_CODES_FILE}' 未找到。")
        return

    # 2. 并行处理
    pool = ThreadPool(MAX_WORKERS)
    pool.map(process_fund, fund_codes)
    pool.close()
    pool.join()
    
    print("\n--- 脚本运行结束 ---")

if __name__ == "__main__":
    main()
