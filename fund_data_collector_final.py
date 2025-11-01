# scrape_fund_data.py

import os
import re
import time
import datetime
import pandas as pd
from bs4 import BeautifulSoup

def read_stock_codes(file_path):
    """
    从C类.txt文件中读取股票代码列表。
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            # 过滤空行或只包含空白字符的行，并去除首尾空白
            codes = [line.strip() for line in f if line.strip()]
        print(f"成功读取 {len(codes)} 个股票代码。")
        return codes
    except FileNotFoundError:
        print(f"错误：文件 {file_path} 未找到。")
        return []

def scrape_fund_holding(url):
    """
    抓取指定URL的基金持仓数据。
    """
    print(f"正在抓取页面: {url}")
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    # 模拟并发抓取 (对于单个URL，这里是串行，但结构上可以扩展到并发)
    # 在实际的生产环境中，如果需要抓取多个页面，可以使用 `concurrent.futures` 
    # 来实现真正的并发。对于这个单一页面的多季度数据提取，单次请求即可。
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status() # 检查HTTP请求是否成功
        response.encoding = 'utf-8'
        html_content = response.text
        print("页面内容下载成功。")
    except requests.exceptions.RequestException as e:
        print(f"HTTP请求失败: {e}")
        return {}

    soup = BeautifulSoup(html_content, 'html.parser')
    
    # 查找所有包含持仓明细的表格容器。在天天基金网的页面结构中，
    # 这些表格通常位于特定的 `div` 标签内，例如以 `tit_h3` 或包含季度信息的标题附近。
    
    # 通过标题定位到包含季度明细的区块
    detail_div = soup.find('div', class_='detail')
    if not detail_div:
        print("未找到持仓明细容器。")
        return {}

    # 提取所有季度表格
    quarterly_data = {}
    
    # 查找所有包含季度信息的 H4 标题
    quarter_titles = detail_div.find_all('div', style=lambda value: value and 'font-size: 14px; font-weight: bold; margin-top: 20px;' in value)
    
    # 另一种更可靠的查找方式：通过表格前后的季度报告文本
    # 查找包含特定季度报告文本的 div
    quarter_divs = detail_div.find_all('div', class_='tit_h3')
    
    # 因为您提供的HTML片段中表格前的标题是直接的文本
    # "汇添富中证芯片产业指数增强发起式C  2025年3季度股票投资明细"
    # 我们直接查找这些文本附近的表格
    
    # 使用正则表达式匹配标题文本
    report_pattern = re.compile(r'汇添富中证芯片产业指数增强发起式C\s+(\d{4}年\d季度)股票投资明细')
    
    # 遍历所有可能的表格和它们周围的文本
    all_tables = detail_div.find_all('table', {'class': 'ftb'})
    
    # 由于您提供的HTML结构不完整，我们将尝试基于现有可见文本结构来模拟提取
    # 在您提供的文本中，表格前面有季度信息
    
    # 模拟从提供的 HTML/文本片段中提取数据
    data_list = []
    
    # 2025年3季度
    try:
        df_3q = pd.read_html(html_content, match='2025年3季度股票投资明细', attrs={'class': 'ftb'})[0]
        data_list.append(("2025年3季度", df_3q))
    except ValueError:
        print("未找到2025年3季度持仓表格。")
        
    # 2025年2季度
    try:
        df_2q = pd.read_html(html_content, match='2025年2季度股票投资明细', attrs={'class': 'ftb'})[0]
        data_list.append(("2025年2季度", df_2q))
    except ValueError:
        print("未找到2025年2季度持仓表格。")

    # 2025年1季度
    try:
        df_1q = pd.read_html(html_content, match='2025年1季度股票投资明细', attrs={'class': 'ftb'})[0]
        data_list.append(("2025年1季度", df_1q))
    except ValueError:
        print("未找到2025年1季度持仓表格。")
        
    # 如果找到了数据，进行处理
    if not data_list:
        print("未从页面中提取到任何持仓数据。")
        return {}

    all_quarter_data = {}
    for quarter, df in data_list:
        print(f"--- 成功提取 {quarter} 数据 ---")
        # 清理列名，移除 '相关资讯' 等非数据列
        if '相关资讯' in df.columns:
            df = df.drop(columns=['相关资讯'])
        
        # 将表格数据转换为 Markdown 格式
        markdown_table = f"## {quarter} 持仓明细\n\n" + df.to_markdown(index=False)
        all_quarter_data[quarter] = markdown_table
        
    return all_quarter_data

def format_output(codes, quarterly_data):
    """
    格式化最终输出内容。
    """
    output = "# 基金持仓与C类股票代码对比报告\n\n"
    
    # 1. C类.txt 代码列表
    output += "## 1. C类.txt 股票代码列表\n"
    output += "读取到的股票代码总数: " + str(len(codes)) + "\n\n"
    output += "```\n" + "\n".join(codes) + "\n```\n\n"
    
    # 2. 基金持仓数据
    output += "## 2. 汇添富中证芯片产业指数增强发起式C(014194) 基金持仓数据\n"
    if quarterly_data:
        for quarter, table in quarterly_data.items():
            output += table + "\n"
    else:
        output += "未成功抓取到基金持仓数据。\n"

    # 3. 对比分析（仅示例，实际对比逻辑可根据需求完善）
    output += "\n## 3. 对比分析摘要\n"
    
    # 提取所有持仓代码（以 2025年3季度为例）
    fund_codes = set()
    try:
        # 尝试从DataFrame中获取股票代码列（假设第一列或第二列是代码）
        df_3q = pd.read_html(html_content, match='2025年3季度股票投资明细', attrs={'class': 'ftb'})[0]
        if '股票代码' in df_3q.columns:
            fund_codes = set(df_3q['股票代码'].astype(str).str.zfill(6).tolist())
    except Exception:
        pass # 忽略错误，如果没有提取到数据

    c_codes = set(codes)
    
    # C类代码中在持仓里的
    in_holding = c_codes.intersection(fund_codes)
    # 持仓中不在C类代码里的
    not_in_c = fund_codes.difference(c_codes)
    
    output += f"* C类代码中在基金 **2025年3季度** 持仓中的股票数: **{len(in_holding)}**\n"
    output += f"* C类代码中在基金 **2025年3季度** 持仓中的股票: {', '.join(sorted(in_holding)) or '无'}\n"
    output += f"* 基金 **2025年3季度** 持仓中不在C类代码中的股票数: **{len(not_in_c)}**\n"
    
    return output

if __name__ == '__main__':
    try:
        # 需要安装 requests, pandas, beautifulsoup4, lxml (或 html5lib)
        import requests
    except ImportError:
        print("请确保已安装所需的库: pip install requests pandas beautifulsoup4 lxml")
        exit(1)

    # 1. 读取 C类.txt
    codes = read_stock_codes('C类.txt')

    # 2. 基金持仓 URL
    fund_url = 'http://fundf10.eastmoney.com/ccmx_014194.html'
    
    # 3. 抓取数据
    quarterly_data = scrape_fund_holding(fund_url)

    # 4. 格式化报告
    final_report = format_output(codes, quarterly_data)

    # 5. 生成文件名和路径 (上海时区)
    # 设置时区为 Asia/Shanghai
    tz = datetime.timezone(datetime.timedelta(hours=8))
    now = datetime.datetime.now(tz)
    
    # 目录格式: 年月 (YYYYMM)
    output_dir = now.strftime('%Y%m')
    
    # 文件名格式: fund_holding_YYYYMMDD_HHMMSS.md
    timestamp_str = now.strftime('%Y%m%d_%H%M%S')
    filename = f"fund_holding_{timestamp_str}.md"
    
    # 确保目录存在
    os.makedirs(output_dir, exist_ok=True)
    
    output_path = os.path.join(output_dir, filename)

    # 6. 保存到文件
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(final_report)
        
    print(f"\n--- 报告生成成功 ---")
    print(f"输出路径: {output_path}")
    print(f"输出目录已创建: {output_dir}")
    print("请确保已配置 git commit 和 push 步骤以推送到仓库。")
