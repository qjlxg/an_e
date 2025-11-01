import requests
from bs4 import BeautifulSoup
import concurrent.futures
import json
from datetime import datetime
import os
from pathlib import Path
import time
import pytz  # 需要安装pytz来处理时区

# 设置上海时区
shanghai_tz = pytz.timezone('Asia/Shanghai')

def fetch_holdings(fund_code):
    """
    抓取单个基金的持仓数据
    """
    url = f"http://fundf10.eastmoney.com/ccmx_{fund_code}.html"  # 持仓明细页
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 找到持仓表格（假设是第一个class="boxitem"的表格）
        table = soup.find('table', class_='w782 comm ccmx')
        if not table:
            return fund_code, []  # 无数据返回空列表
        
        holdings = []
        rows = table.find_all('tr')[1:]  # 跳过表头
        for row in rows:
            cols = row.find_all('td')
            if len(cols) >= 2:
                stock_code = cols[1].text.strip()  # 股票代码
                stock_name = cols[2].text.strip()  # 股票名称
                if stock_code and stock_name:
                    holdings.append({"stock_code": stock_code, "stock_name": stock_name})
        
        time.sleep(1)  # 延迟1秒，避免频繁请求
        return fund_code, holdings
    except Exception as e:
        print(f"Error fetching {fund_code}: {e}")
        return fund_code, []

def main():
    # 读取C类.txt
    fund_codes = []
    with open('C类.txt', 'r', encoding='utf-8') as f:
        for line in f:
            code = line.strip()
            if code:
                fund_codes.append(code)
    
    # 并行抓取
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:  # 限制并发10个
        future_to_code = {executor.submit(fetch_holdings, code): code for code in fund_codes}
        for future in concurrent.futures.as_completed(future_to_code):
            code, holdings = future.result()
            results[code] = holdings
    
    # 获取当前上海时间
    now = datetime.now(shanghai_tz)
    year_month_dir = now.strftime('%Y/%m')
    timestamp = now.strftime('%Y%m%d%H%M%S')
    filename = f"fund_holdings_{timestamp}.json"
    
    # 创建目录
    Path(year_month_dir).mkdir(parents=True, exist_ok=True)
    
    # 保存JSON
    filepath = os.path.join(year_month_dir, filename)
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=4)
    
    print(f"Data saved to {filepath}")

if __name__ == "__main__":
    main()
