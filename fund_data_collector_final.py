# fund_data_collector_final.py (并行版)

import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import os
import time
import json
from multiprocessing.dummy import Pool as ThreadPool # 使用线程池进行网络I/O密集型任务

# --- 配置 ---
FUND_CODES_FILE = "C类.txt"
# 直接请求数据接口，该接口返回包含所有持仓表格的HTML片段
BASE_DATA_URL = "http://fundf10.eastmoney.com/FundArchivesDatas.aspx?type=ccmx&code={fund_code}&qdii=&sdate=&edate=&rt={timestamp}"
# 设置并发线程数。通常设置为CPU核数或更高（针对I/O密集型任务）
MAX_WORKERS = 10 

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
        'Referer': f'http://fundf10.eastmoney.com/ccmx_{fund_code}.html'
    }
    
    try:
        # 设置超时
        response = requests.get(url, headers=headers, timeout=15)
        response.encoding = 'utf-8'

        if response.status_code == 200:
            text = response.text.strip()
            if text.startswith('var apidata='):
                # 提取 JSON 字符串部分
                json_str = text.split('=', 1)[1].rstrip(';')
                data = json.loads(json_str)
                return data.get('content')
            else:
                print(f"[{fund_code}] 错误: 数据接口返回格式不正确。")
                return None
        else:
            print(f"[{fund_code}] 抓取失败，状态码: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"[{fund_code}] 抓取过程中发生错误: {e}")
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
        # 1. 提取标题
        title_tag = table.find_previous_sibling('h4')
        title = f"{fund_code}_股票投资明细"
        if title_tag:
            raw_title = title_tag.text.strip().replace('\r\n', ' ').replace('\n', ' ').replace('\xa0', ' ')
            
            # 提取基金名和季度信息
            parts = raw_title.split(' ')
            if len(parts) >= 3 and ('季度' in parts[-2] or '年度' in parts[-2]):
                 title = f"{parts[0]}_{parts[-3]}{parts[-2]}{parts[-1]}"
            else:
                 title = raw_title.replace(' ', '_')

        
        # 2. 提取表头
        headers = [th.text.strip().replace('\xa0', '').replace('\n', '') for th in table.find('tr').find_all('th')]
        
        # 3. 提取行数据
        data_rows = []
        for row in table.find_all('tr')[1:]:
            cols = [col.text.strip().replace('\xa0', '').replace('\n', '').replace(' ', '') for col in row.find_all(['td'])]
            
            # 标准化数据列
            if len(cols) == 10: # 含 最新价/涨跌幅
                # 序号, 代码, 名称, 最新价, 涨跌幅, 占净值比例, 持股数, 持仓市值
                data_rows.append([cols[0], cols[1], cols[2], cols[3], cols[4], cols[6], cols[7], cols[8], cols[9]])
            elif len(cols) == 8: # 不含 最新价/涨跌幅
                # 填充 '最新价' 和 '涨跌幅' 为空字符串
                data_rows.append([cols[0], cols[1], cols[2], '', '', cols[4], cols[5], cols[6], cols[7]])
            else:
                # 无法识别的列数，跳过本行
                continue

        # 调整表头以匹配我们最终保留的列
        final_headers = ['序号', '股票代码', '股票名称', '最新价', '涨跌幅', '占净值比例', '持股数（万股）', '持仓市值（万元）']
        
        df = pd.DataFrame(data_rows, columns=final_headers)
        
        # 生成时间戳和文件名
        cst_time = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8)))
        timestamp = cst_time.strftime("%Y%m%d%H%M%S")
        
        # 确保目录存在
        output_dir = get_output_dir()
        os.makedirs(output_dir, exist_ok=True)
        
        # 文件名示例: 汇添富中证芯片产业指数增强发起式C_2025年3季度股票投资明细_20251101201500.csv
        filename = f"{title}_{timestamp}.csv"
        filepath = os.path.join(output_dir, filename)
        
        df.to_csv(filepath, index=False, encoding='utf_8_sig')
        print(f"[{fund_code}] 成功保存数据到: {filepath}")
        
    except Exception as e:
        print(f"[{fund_code}] 解析或保存表格时发生错误: {e}")

# 并行处理函数
def process_fund(fund_code):
    """单个基金代码的完整处理流程。"""
    # 增加日志
    print(f"[{fund_code}] 开始抓取...")
    html_content = fetch_holding_data(fund_code)
    
    if html_content:
        parse_and_save_data(fund_code, html_content)
    else:
        print(f"[{fund_code}] 抓取失败，跳过解析。")
    # 线程并行，不需要 time.sleep

# 主运行逻辑
def main():
    print("--- 开始运行基金数据收集脚本 (并行模式) ---")
    
    # 1. 读取基金代码
    fund_codes = []
    try:
        with open(FUND_CODES_FILE, 'r') as f:
            fund_codes = [line.strip() for line in f if line.strip() and line.strip() != 'code']
        print(f"读取到 {len(fund_codes)} 个基金代码，将使用 {MAX_WORKERS} 个线程并行处理。")
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
