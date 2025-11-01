import requests
import re
import json
import threading
import queue
import time
import random
import pandas as pd
import os
from io import StringIO
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

# --- 配置区 ---
# 线程数量 (请根据您的环境调整，过高可能触发反爬)
MAX_THREADS = 10 
# 博客示例中查询的固定季度
REPORT_YEAR = 2021
REPORT_MONTH = 3 
# 文件名
FUND_CODES_FILE = 'C类.txt' 

# 动态生成输出文件名和路径，以便匹配 GitHub Actions 的提交逻辑
CURRENT_TIME_STR = datetime.now().strftime('%Y%m%d%H%M%S')
CURRENT_YEAR_DIR = datetime.now().strftime('%Y')
CURRENT_MONTH_DIR = datetime.now().strftime('%m')
OUTPUT_DIR = os.path.join(CURRENT_YEAR_DIR, CURRENT_MONTH_DIR)
OUTPUT_CSV_FILE = os.path.join(OUTPUT_DIR, f'fund_data_{CURRENT_TIME_STR}.csv')


# 全局数据列表，用于收集所有线程的结果
all_fund_data = []
data_lock = threading.Lock() # 线程安全锁

# 模拟请求头
HEADER = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Referer': 'http://fund.eastmoney.com/'
}

# --- 核心函数实现 ---

def get_fund_code():
    """
    获取所有基金代码，来自 fund.eastmoney.com/js/fundcode_search.js
    """
    print("--- 1. 正在获取所有基金代码... ---")
    url = "http://fund.eastmoney.com/js/fundcode_search.js"
    
    try:
        req = requests.get(url, timeout=5, headers=HEADER)
        req.encoding = 'utf-8'

        match = re.search(r'var r = (\[.+?\]);', req.text)
        if match:
            fund_data_list = json.loads(match.group(1))
            fund_codes = [data[0] for data in fund_data_list]
            
            print(f"✅ 成功获取 {len(fund_codes)} 个基金代码。")
            return fund_codes
        
        print("❌ 无法从 fundcode_search.js 中解析基金代码。")
        return []

    except requests.RequestException as e:
        print(f"❌ 获取基金代码列表失败: {e}")
        return []


def get_fund_data(fund_code_queue):
    """
    多线程工作函数，从队列中取出代码并爬取数据。
    """
    while not fund_code_queue.empty():
        try:
            fund_code = fund_code_queue.get(timeout=0.1)
        except queue.Empty:
            break
            
        code_str = str(fund_code)
        
        # 爬取结果的基础结构
        result = {'code': code_str, 'name': 'N/A', 'gz_date': 'N/A', 'error': ''}
        
        proxies = {} # 代理已注释

        try:
            # 1. 获取基金详情 (实时净值/估值)
            gz_url = f"http://fundgz.1234567.com.cn/js/{code_str}.js"
            req_gz = requests.get(gz_url, proxies=proxies, timeout=3, headers=HEADER)
            req_gz.encoding = 'utf-8'
            
            gz_match = re.search(r'jsonpgz\((.+)\);', req_gz.text)
            if gz_match:
                gz_data = json.loads(gz_match.group(1))
                result['name'] = gz_data.get('name', 'N/A')
                result['gsz'] = gz_data.get('gsz', 'N/A')
                result['gszzl'] = gz_data.get('gszzl', 'N/A')
                result['gz_date'] = gz_data.get('gztime', 'N/A')
            else:
                result['error'] += "无法解析实时估值; "

            # 2. 获取持仓股票信息 (固定为 2021年1季度)
            cc_url = (
                f"http://fundf10.eastmoney.com/FundArchivesDatas.aspx?type=jjcc&code={code_str}"
                f"&topline=10&year={REPORT_YEAR}&month={REPORT_MONTH}"
            )
            # 增加重试机制来应对 'Read timed out' 错误
            MAX_RETRIES = 2
            req_cc = None
            for attempt in range(MAX_RETRIES):
                try:
                    req_cc = requests.get(cc_url, proxies=proxies, timeout=5, headers=HEADER)
                    req_cc.raise_for_status() # 检查HTTP错误
                    req_cc.encoding = 'utf-8'
                    break # 成功则跳出重试循环
                except requests.exceptions.RequestException as e:
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(random.uniform(1, 3)) # 重试前休眠
                    else:
                        result['error'] += f"持仓请求最终失败: {e}; "
                        req_cc = None
            
            html_content = None
            if req_cc and req_cc.status_code == 200:
                cc_match = re.search(r'var apidata=(.+);', req_cc.text)
                if cc_match:
                    try:
                        # FIX: 隔离 eval() 调用，防止其内部的 NameError 破坏外部流程
                        cc_data = eval(cc_match.group(1)) 
                        html_content = cc_data.get('content')
                    except NameError as ne:
                        # 捕获由 HTML 内嵌 JS 导致的 'name 'content' is not defined' 错误
                        result['error'] += f"持仓数据解析失败 (NameError: {ne}); "
                    except Exception as e:
                        # 捕获其他解析错误 (如 SyntaxError)
                        result['error'] += f"持仓数据解析失败: {e}; "
                else:
                    result['error'] += "无法从持仓响应中提取 apidata; "
            
            
            # 3. 解析持仓表格
            if html_content:
                # 使用 pandas 读取 HTML 表格数据
                tables = pd.read_html(StringIO(html_content), encoding='utf-8')
                if tables:
                    # 持仓表格通常是第一个
                    holdings_df = tables[0]
                    # 将持仓数据转为字典列表
                    result['holdings'] = holdings_df.to_dict('records')
                else:
                    result['error'] += "无法解析持仓表格; "
            elif '持仓数据解析失败' not in result['error'] and '持仓请求最终失败' not in result['error']:
                 # 仅在解析没有失败的情况下，报告内容为空
                result['error'] += "持仓HTML内容为空; "


        except requests.RequestException as e:
            result['error'] += f"请求发生错误: {e}; "
        except Exception as e:
            result['error'] += f"其他处理失败: {e}; "

        # 4. 将结果添加到全局列表 (线程安全)
        # ... (数据写入逻辑保持不变)
        with data_lock:
            if 'holdings' in result:
                for holding in result['holdings']:
                    row = {
                        '基金代码': result['code'],
                        '基金名称': result['name'],
                        '实时估值': result.get('gsz'),
                        '估值日期': result.get('gz_date'),
                        '持仓股票代码': holding.get('股票代码'),
                        '持仓股票名称': holding.get('股票名称'),
                        '占净值比例': holding.get('占净值比例'),
                        '持仓市值(万元)': holding.get('持仓市值(万元)'),
                        '爬取错误信息': result['error']
                    }
                    all_fund_data.append(row)
            else:
                 # 如果没有持仓数据，也记录一条基础信息
                row = {
                    '基金代码': result['code'],
                    '基金名称': result['name'],
                    '实时估值': result.get('gsz'),
                    '估值日期': result.get('gz_date'),
                    '持仓股票代码': 'N/A',
                    '持仓股票名称': 'N/A',
                    '占净值比例': 'N/A',
                    '持仓市值(万元)': 'N/A',
                    '爬取错误信息': result['error']
                }
                all_fund_data.append(row)


        fund_code_queue.task_done()
        time.sleep(random.uniform(0.1, 0.5)) 
        print(f"Processed: {code_str} (Queue size: {fund_code_queue.qsize()})")

# --- 主程序 ---

if __name__ == "__main__":
    
    # 1. 创建输出目录
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 2. 读取文件中的基金代码
    fund_codes_to_process = []
    try:
        with open(FUND_CODES_FILE, 'r') as f:
            fund_codes_to_process = [line.strip() for line in f if line.strip()]
        print(f"✅ 从 {FUND_CODES_FILE} 中读取到 {len(fund_codes_to_process)} 个基金代码。")
    except FileNotFoundError:
        print(f"⚠️ 文件 {FUND_CODES_FILE} 未找到，尝试获取所有基金代码作为处理列表。")
        fund_codes_to_process = get_fund_code()
        
    if not fund_codes_to_process:
        print("❌ 没有基金代码可以处理，程序退出。")
    else:
        # 3. 构造队列
        fund_code_queue = queue.Queue(len(fund_codes_to_process))
        for code in fund_codes_to_process:
            fund_code_queue.put(code)

        # 4. 开启多线程爬取
        print(f"--- 2. 启动 {MAX_THREADS} 个线程开始爬取数据... ---")
        
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            for i in range(MAX_THREADS):
                executor.submit(get_fund_data, fund_code_queue)

        fund_code_queue.join()
        
        print("--- 3. 所有爬虫线程任务完成。 ---")

        # 5. 保存结果到 CSV 文件
        if all_fund_data:
            df_output = pd.DataFrame(all_fund_data)
            
            final_columns = [
                '基金代码', '基金名称', '实时估值', '估值日期', 
                '持仓股票代码', '持仓股票名称', '占净值比例', 
                '持仓市值(万元)', '爬取错误信息'
            ]
            df_output = df_output.reindex(columns=final_columns)
            
            df_output.to_csv(OUTPUT_CSV_FILE, index=False, encoding='utf-8-sig')
            
            print("\n==============================================")
            print(f"✅ 数据成功保存至 {OUTPUT_CSV_FILE}")
            print(f"共计 {len(df_output)} 条持仓/估值记录。")
            print("==============================================")
        else:
            print("❌ 未能获取到任何有效数据。")
