# fund_data_collector_final.py (修正版)

import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import os
import time
import random

# 定义目标基金代码文件和新的数据接口基础URL
FUND_CODES_FILE = "C类.txt"
# 直接请求数据接口，该接口返回包含所有持仓表格的HTML片段
BASE_DATA_URL = "http://fundf10.eastmoney.com/FundArchivesDatas.aspx?type=ccmx&code={fund_code}&qdii=&sdate=&edate=&rt={timestamp}"
# 目标文件夹（例如 2025/11/）
# 注意：GitHub Actions 默认使用 UTC，这里确保使用上海时区 (CST, UTC+8)
def get_output_dir():
    # 使用时区感知时间
    cst_time = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8)))
    return os.path.join(cst_time.strftime("%Y"), cst_time.strftime("%m"))

# 抓取页面的函数
def fetch_holding_data(fund_code):
    """
    抓取天天基金网指定基金代码的持仓表格 HTML 内容。
    """
    # 使用精确到毫秒的时间戳作为 rt 参数，防止缓存
    timestamp = time.time() * 1000
    url = BASE_DATA_URL.format(fund_code=fund_code, timestamp=timestamp)
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        # 模拟浏览器发起的请求
        'Referer': f'http://fundf10.eastmoney.com/ccmx_{fund_code}.html'
    }
    print(f"尝试抓取基金代码: {fund_code}，数据接口 URL: {url}")
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.encoding = 'utf-8'

        if response.status_code == 200:
            # 数据接口返回的格式是：var apidata={content:"<div class='box'><div class='boxitem'>...</div></div>"};
            # 需要提取 content 字段的内容
            text = response.text.strip()
            if text.startswith('var apidata='):
                import json
                # 提取 JSON 字符串部分
                json_str = text.split('=', 1)[1].rstrip(';')
                data = json.loads(json_str)
                return data.get('content')
            else:
                print("错误: 数据接口返回格式不正确。")
                return None
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
    
    # 查找所有持仓表格（class='w780'）
    tables = soup.find_all('table', class_='w780')
    
    if not tables:
        print(f"在页面中未找到持仓表格，跳过基金 {fund_code}")
        return

    # 只处理第一个表格（通常是最新季度数据）
    table = tables[0]
    
    try:
        # 1. 提取表格标题/季度信息，用于文件名
        # 标题通常在表格前的 <h4> 标签中。数据接口返回的HTML中，标题是 h4
        title_tag = table.find_previous_sibling('h4')
        # 寻找 h4 失败，则寻找更上级的 div
        if not title_tag:
            parent_div = table.find_parent('div', class_='boxitem')
            if parent_div:
                title_tag = parent_div.find_previous_sibling('h4')

        title = f"{fund_code}_股票投资明细"
        if title_tag:
            # 清理标题文本，移除不必要的空格和换行
            title = title_tag.text.strip().replace('\r\n', ' ').replace('\n', ' ').replace('\xa0', ' ')
            
        # 进一步清理标题，仅保留基金名和季度信息
        # 示例: "汇添富中证芯片产业指数增强发起式C 2025年3季度股票投资明细"
        parts = title.split(' ')
        if len(parts) >= 3 and ('季度' in parts[-2] or '年度' in parts[-2]):
             title = f"{parts[-3]}{parts[-2]}{parts[-1]}"
        elif len(parts) >= 2 and ('季度' in parts[-2] or '年度' in parts[-2]):
             # 针对题目中提供的格式 "汇添富中证芯片产业指数增强发起式C  2025年3季度股票投资明细"
             title = f"{parts[0]}_{parts[1]}"
        else:
             title = title.replace(' ', '_')

        
        # 2. 提取表头
        headers = [th.text.strip().replace('\xa0', '').replace('\n', '') for th in table.find('tr').find_all('th')]
        
        # 3. 提取所有行数据
        data_rows = []
        for row in table.find_all('tr')[1:]: # 跳过表头行
            # 清理数据：移除不必要的空格和换行
            cols = [col.text.strip().replace('\xa0', '').replace('\n', '').replace(' ', '') for col in row.find_all(['td'])]
            
            # 过滤掉“相关资讯”和“变动详情股吧行情”等不可用的列，只保留数值和名称列
            # 2025年3季度表格有10列，其他季度有8列，需要动态调整。
            # 关键是保留：序号、代码、名称、比例、股数、市值
            
            # 检查列数，并只保留核心数据列
            if len(cols) == 10: # 针对 3 季度表格（含最新价和涨跌幅）
                 # 序号, 股票代码, 股票名称, 最新价, 涨跌幅, 相关资讯, 占净值比例, 持股数, 持仓市值
                data_rows.append([cols[0], cols[1], cols[2], cols[3], cols[4], cols[6], cols[7], cols[8], cols[9]])
            elif len(cols) == 8: # 针对 1/2 季度表格（不含最新价和涨跌幅）
                 # 序号, 股票代码, 股票名称, 相关资讯, 占净值比例, 持股数, 持仓市值
                 # 填充 '最新价' 和 '涨跌幅' 为空字符串，保持结构一致
                data_rows.append([cols[0], cols[1], cols[2], '', '', cols[4], cols[5], cols[6], cols[7]])
            else:
                # 无法识别的列数，直接跳过本行
                continue

        # 调整表头以匹配我们最终保留的列
        final_headers = ['序号', '股票代码', '股票名称', '最新价', '涨跌幅', '占净值比例', '持股数（万股）', '持仓市值（万元）']
        
        df = pd.DataFrame(data_rows, columns=final_headers)
        
        # 生成时间戳和文件名
        cst_time = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8)))
        timestamp = cst_time.strftime("%Y%m%d%H%M%S")
        
        # 文件名示例: 014194_汇添富中证芯片产业指数增强发起式C_2025年3季度股票投资明细_20251101201500.csv
        filename = f"{title}_{timestamp}.csv"
        filepath = os.path.join(get_output_dir(), filename)
        
        # 确保目录存在
        os.makedirs(get_output_dir(), exist_ok=True)
        
        # 保存为 CSV
        df.to_csv(filepath, index=False, encoding='utf_8_sig')
        print(f"成功保存数据到: {filepath}")
        
        # 假设只需要最新的一个季度数据，抓取后即可退出
        return

    except Exception as e:
        print(f"解析或保存基金 {fund_code} 的表格时发生错误: {e}")

# 主运行逻辑
def main():
    print("--- 开始运行基金数据收集脚本 ---")
    
    # 1. 读取基金代码
    fund_codes = []
    try:
        with open(FUND_CODES_FILE, 'r') as f:
            # 过滤空行，去除空白字符和标题行 "code"
            fund_codes = [line.strip() for line in f if line.strip() and line.strip() != 'code']
        print(f"读取到 {len(fund_codes)} 个基金代码。")
    except FileNotFoundError:
        print(f"错误: 基金代码文件 '{FUND_CODES_FILE}' 未找到。")
        return

    # 2. 循环处理每个基金代码
    for i, fund_code in enumerate(fund_codes):
        print(f"\n--- 正在处理第 {i+1} 个基金: {fund_code} ---")
        
        # 仅处理题目中指定的基金代码
        if fund_code == '014194': 
            html_content = fetch_holding_data(fund_code)
            parse_and_save_data(fund_code, html_content)
        else:
            print(f"跳过基金 {fund_code} (只处理 '014194')") 
        
        # 每次抓取后暂停1-3秒，避免被封禁
        if i < len(fund_codes) - 1:
            wait_time = random.uniform(1, 3)
            print(f"等待 {wait_time:.2f} 秒...")
            time.sleep(wait_time)

    print("\n--- 脚本运行结束 ---")

if __name__ == "__main__":
    main()
