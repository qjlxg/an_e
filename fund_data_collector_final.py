# fund_data_collector_final.py

import os
import re
import time
import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup
import io
import concurrent.futures # <-- 实现并行的关键库

# 常量定义
BASE_URL_TEMPLATE = 'http://fundf10.eastmoney.com/ccmx_{}.html'
C_CLASS_FILE = 'C类.txt'
MAX_WORKERS = 10 # 最大并发线程数，可根据GitHub Actions的性能进行调整

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
        print(f"基金 {fund_code} 页面内容下载成功。")
    except requests.exceptions.RequestException as e:
        print(f"基金 {fund_code} HTTP请求失败: {e}")
        return {}
    
    quarterly_data = {}
    
    try:
        # 使用 io.StringIO 包装 html_content，遵循 pandas 最新规范
        all_tables = pd.read_html(io.StringIO(html_content), attrs={'class': 'ftb'})
    except ValueError:
        # 如果页面没有包含匹配的表格，pandas 会抛出 ValueError
        print(f"基金 {fund_code} 页面未找到任何基金持仓表格。")
        return {}
    except Exception as e:
        print(f"基金 {fund_code} 读取表格时发生未知错误: {e}")
        return {}

    # 使用 BeautifulSoup 查找表格前的标题，提取季度信息
    soup = BeautifulSoup(html_content, 'html.parser')
    table_elements = soup.find_all('table', {'class': 'ftb'})
    
    for index, table_element in enumerate(table_elements):
        if index >= len(all_tables):
            break

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
        print(f"基金 {fund_code} - 成功提取 {quarter_label} 数据。")
        
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

def process_fund(fund_code, all_fund_codes, output_dir, timestamp_str):
    """
    处理单个基金代码的抓取、格式化和保存，用于并发执行。
    """
    try:
        # 1. 抓取数据
        quarterly_data_map = scrape_fund_holding(fund_code) 

        # 2. 格式化报告
        final_report = format_output(fund_code, all_fund_codes, quarterly_data_map)

        # 3. 生成文件名和路径
        filename = f"{fund_code}_holding_{timestamp_str}.md"
        output_path = os.path.join(output_dir, filename)

        # 4. 保存到文件
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(final_report)
        
        return f"基金 {fund_code} 报告生成成功。路径: {output_path}"
    
    except Exception as e:
        return f"处理基金 {fund_code} 时发生致命错误: {e}"

def main():
    # 1. 检查依赖
    try:
        global requests
        import requests
    except ImportError:
        print("请确保已安装所需的库: pip install requests pandas beautifulsoup4 lxml")
        return

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
    timestamp_str = now.strftime('%Y%m%d_%H%M%S') # 统一时间戳
    
    # 确保目录存在
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"\n--- 准备以 {MAX_WORKERS} 个并发线程抓取 {len(fund_codes_to_scrape)} 个基金 ---")
    
    # 4. 并发处理所有基金代码
    results = []
    
    # 使用 ThreadPoolExecutor 实现并发抓取 (网络IO密集型任务，线程更高效)
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # 提交所有任务到线程池
        future_to_code = {
            executor.submit(process_fund, code, fund_codes_to_scrape, output_dir, timestamp_str): code
            for code in fund_codes_to_scrape
        }
        
        # 收集结果并打印
        for future in concurrent.futures.as_completed(future_to_code):
            code = future_to_code[future]
            try:
                result = future.result()
                results.append(result)
                print(result) # 实时打印每个基金的处理结果
            except Exception as exc:
                print(f'基金 {code} 在处理过程中产生异常: {exc}')

    print("\n\n======== 所有基金报告处理完成 ========")
    success_count = len([r for r in results if '成功' in r])
    print(f"成功生成 {success_count} 份报告。")

if __name__ == '__main__':
    main()
