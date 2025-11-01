# fund_data_collector_final.py

import os
import re
import time
import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup

# 常量定义
BASE_URL_TEMPLATE = 'http://fundf10.eastmoney.com/ccmx_{}.html'
C_CLASS_FILE = 'C类.txt'

def read_codes_from_file(file_path):
    """
    从C类.txt文件中读取基金代码列表。
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 使用正则表达式匹配6位数字的代码
        codes = re.findall(r'\b\d{6}\b', content)
        
        # 移除重复代码
        unique_codes = sorted(list(set(codes)))
        
        print(f"成功从 {file_path} 读取 {len(unique_codes)} 个基金代码。")
        return unique_codes
    except FileNotFoundError:
        print(f"错误：文件 {file_path} 未找到。")
        return []
    except Exception as e:
        print(f"读取文件时发生错误: {e}")
        return []

def scrape_fund_holding(fund_code):
    """
    抓取指定基金代码的持仓数据。
    """
    url = BASE_URL_TEMPLATE.format(fund_code)
    print(f"\n--- 正在抓取基金 {fund_code} 的页面: {url} ---")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        response.encoding = 'utf-8' 
        html_content = response.text
        print("页面内容下载成功。")
    except requests.exceptions.RequestException as e:
        print(f"HTTP请求失败: {e}")
        return {}
    
    quarterly_data = {}
    
    # 尝试使用 pandas.read_html 读取页面中所有基金持仓表格
    try:
        # 使用 attrs={'class': 'ftb'} 确保只抓取持仓表格
        all_tables = pd.read_html(html_content, attrs={'class': 'ftb'})
    except ValueError:
        print(f"基金 {fund_code} 页面未找到任何基金持仓表格。")
        return {}
    except Exception as e:
        print(f"读取表格时发生未知错误: {e}")
        return {}

    # 使用 BeautifulSoup 查找表格前的标题，提取季度信息
    soup = BeautifulSoup(html_content, 'html.parser')
    table_elements = soup.find_all('table', {'class': 'ftb'})
    
    for index, table_element in enumerate(table_elements):
        if index >= len(all_tables):
            break # 防止越界

        # 尝试从表格前的最近标题中提取季度信息
        prev_sibling = table_element.find_previous_sibling()
        quarter_label = f"未知季度-{index + 1}"

        while prev_sibling:
            text = prev_sibling.get_text(strip=True)
            # 匹配 2025年3季度 这样的格式
            match = re.search(r'(\d{4}年\d季度)', text)
            if match:
                quarter_label = match.group(0)
                break
            prev_sibling = prev_sibling.find_previous_sibling()

        df = all_tables[index]
        print(f"--- 成功提取 {quarter_label} 数据 ---")
        
        # 清理列名
        cols_to_drop = ['相关资讯', '变动详情', '股吧', '行情']
        for col in cols_to_drop:
            if col in df.columns:
                df = df.drop(columns=[col])

        # 确保股票代码格式正确
        if '股票代码' in df.columns:
            df['股票代码'] = df['股票代码'].astype(str).str.zfill(6)
        
        # 存储 DataFrame 和 Markdown 文本
        markdown_table = f"## {quarter_label} 股票持仓明细\n\n" + df.to_markdown(index=False)
        quarterly_data[quarter_label] = (df, markdown_table)
            
    return quarterly_data

def format_output(fund_code, all_fund_codes, quarterly_data):
    """
    格式化最终输出内容。
    """
    output = f"# 基金 {fund_code} 持仓报告 (与 C类基金列表对比)\n\n"
    
    # 1. C类基金代码列表
    output += f"## 1. C类文件 ({C_CLASS_FILE}) 中的基金代码列表\n"
    output += f"读取到的基金代码总数: {len(all_fund_codes)}\n\n"
    output += "```\n" + "\n".join(all_fund_codes) + "\n```\n\n"
    
    # 2. 基金持仓数据
    output += f"## 2. 基金 {fund_code} 股票持仓数据\n"
    
    if quarterly_data:
        # 遍历数据，从最新的季度开始
        for quarter_label, (df, table) in quarterly_data.items():
            output += table + "\n"
    else:
        output += "未成功抓取到基金持仓数据。\n"

    # 3. 对比分析（说明C类代码是基金代码，不进行集合比对）
    output += "\n## 3. 对比分析摘要\n"
    output += "请注意: C类文件中的代码为**基金代码**，本报告的持仓数据为**股票代码**，因此不进行代码集合的直接比对。\n"
    output += f"当前基金代码 {fund_code} 位于 C类列表中共 {len(all_fund_codes)} 个基金代码中。\n"
    
    return output

def main():
    # 1. 检查依赖
    try:
        global requests
        import requests
    except ImportError:
        print("请确保已安装所需的库: pip install requests pandas beautifulsoup4 lxml")
        exit(1)

    # 2. 读取 C类.txt 中的基金代码
    fund_codes_to_scrape = read_codes_from_file(C_CLASS_FILE)
    
    if not fund_codes_to_scrape:
        print("未读取到任何基金代码，脚本退出。")
        return

    # 3. 设置时区 (上海时区 UTC+8)
    tz = datetime.timezone(datetime.timedelta(hours=8))
    now = datetime.datetime.now(tz)
    
    # 目录格式: 年月 (YYYYMM)
    output_dir = now.strftime('%Y%m')
    
    # 确保目录存在
    os.makedirs(output_dir, exist_ok=True)
    
    all_reports_paths = []
    
    # 4. 循环处理每个基金代码
    for fund_code in fund_codes_to_scrape:
        # 每次循环都使用当前时间生成报告，以保证文件名中的时间戳唯一性
        
        # 抓取数据
        quarterly_data_map = scrape_fund_holding(fund_code) 

        # 格式化报告
        final_report = format_output(fund_code, fund_codes_to_scrape, quarterly_data_map)

        # 生成文件名和路径
        timestamp_str = now.strftime('%Y%m%d_%H%M%S')
        # 文件名格式: FUNDCODE_holding_YYYYMMDD_HHMMSS.md
        filename = f"{fund_code}_holding_{timestamp_str}.md"
        output_path = os.path.join(output_dir, filename)

        # 保存到文件
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(final_report)
            all_reports_paths.append(output_path)
            print(f"--- 基金 {fund_code} 报告生成成功 ---")
            print(f"输出路径: {output_path}")
        except Exception as e:
            print(f"保存基金 {fund_code} 报告时发生错误: {e}")
            
    print("\n\n======== 所有基金报告处理完成 ========")
    print(f"共生成 {len(all_reports_paths)} 份报告。")

if __name__ == '__main__':
    main()
