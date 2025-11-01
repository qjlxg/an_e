# fund_data_collector_final.py

import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import os
import time

# 定义目标基金代码文件和基础URL
FUND_CODES_FILE = "C类.txt"
BASE_URL = "http://fundf10.eastmoney.com/ccmx_{fund_code}.html"
# 目标文件夹（例如 2025/11/）
OUTPUT_BASE_DIR = os.path.join(datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).strftime("%Y"), datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).strftime("%m"))

# 抓取页面的函数
def fetch_holding_data(fund_code):
    """
    抓取天天基金网指定基金代码的持仓页面内容。
    """
    url = BASE_URL.format(fund_code=fund_code)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    print(f"尝试抓取基金代码: {fund_code}，URL: {url}")
    try:
        # 设置超时
        response = requests.get(url, headers=headers, timeout=15)
        response.encoding = 'utf-8' # 确保正确解析中文
        if response.status_code == 200:
            return response.text
        else:
            print(f"抓取失败，状态码: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"抓取过程中发生错误: {e}")
        return None

# 解析页面并提取数据的函数
def parse_and_save_data(fund_code, html_content):
    """
    解析 HTML 内容，提取持仓表格数据，并保存为 CSV 文件。
    """
    if not html_content:
        print(f"没有内容可供解析，跳过基金 {fund_code}")
        return

    soup = BeautifulSoup(html_content, 'html.parser')
    
    # 查找所有持仓表格（通常按季度列出）
    tables = soup.find_all('table', class_='w780')
    
    if not tables:
        print(f"在页面中未找到持仓表格，跳过基金 {fund_code}")
        return

    for table in tables:
        try:
            # 提取表格标题/季度信息，用于文件名
            title_tag = table.find_previous_sibling('h4')
            if not title_tag:
                 # 有些页面的结构可能不同，尝试找 h3/div 等
                title_tag = soup.find('div', class_='box').find('h3') 

            if title_tag:
                title = title_tag.text.strip().replace(' ', '_').replace('\n', '')
                # 尝试从标题中提取季度和年份信息，例如 "2025年3季度"
                # 这里我们假设第一个找到的表格是我们想要的（最新的）
                if '股票投资明细' in title:
                    # 提取表头
                    headers = [th.text.strip() for th in table.find('tr').find_all('th')]
                    
                    # 提取所有行数据
                    data_rows = []
                    for row in table.find_all('tr')[1:]: # 跳过表头行
                        # 清理数据：移除不必要的空格和换行
                        cols = [col.text.strip().replace('\xa0', '').replace('\n', '') for col in row.find_all(['td', 'th'])]
                        data_rows.append(cols)
                    
                    # 创建 DataFrame
                    df = pd.DataFrame(data_rows, columns=headers)
                    
                    # 生成时间戳和文件名
                    timestamp = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).strftime("%Y%m%d%H%M%S")
                    filename = f"{fund_code}_{title}_{timestamp}.csv"
                    filepath = os.path.join(OUTPUT_BASE_DIR, filename)
                    
                    # 确保目录存在
                    os.makedirs(OUTPUT_BASE_DIR, exist_ok=True)
                    
                    # 保存为 CSV
                    df.to_csv(filepath, index=False, encoding='utf_8_sig')
                    print(f"成功保存数据到: {filepath}")
                    
                    # 假设只需要最新的一个季度数据，抓取后即可退出循环
                    return

        except Exception as e:
            print(f"解析或保存基金 {fund_code} 的表格时发生错误: {e}")

# 主运行逻辑
def main():
    """
    主函数：读取代码，循环抓取并处理数据。
    """
    print("--- 开始运行基金数据收集脚本 ---")
    
    # 1. 读取基金代码
    fund_codes = []
    try:
        with open(FUND_CODES_FILE, 'r') as f:
            # 过滤空行，去除空白字符
            fund_codes = [line.strip() for line in f if line.strip()]
        print(f"读取到 {len(fund_codes)} 个基金代码。")
    except FileNotFoundError:
        print(f"错误: 基金代码文件 '{FUND_CODES_FILE}' 未找到。")
        return

    # 2. 循环处理每个基金代码
    for i, fund_code in enumerate(fund_codes):
        print(f"\n--- 正在处理第 {i+1} 个基金: {fund_code} ---")
        
        # 目标代码是 014194，其他代码跳过或自行修改抓取逻辑
        if fund_code == '014194': 
            html = fetch_holding_data(fund_code)
            parse_and_save_data(fund_code, html)
        else:
            # 仅处理题目中指定的基金代码，其他跳过
            print(f"跳过基金 {fund_code} (只处理 '014194')") 
        
        # 为了避免对服务器造成压力，每次抓取后暂停1-3秒
        if i < len(fund_codes) - 1:
            wait_time = 1  # 可以增加随机性 time.sleep(random.uniform(1, 3))
            print(f"等待 {wait_time} 秒...")
            time.sleep(wait_time)

    print("\n--- 脚本运行结束 ---")

if __name__ == "__main__":
    main()
