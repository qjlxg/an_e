# fund_scraper.py
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import time

# 定义请求头，模拟浏览器访问，提高成功率
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

def get_fund_codes(filepath="C类.txt"):
    """
    从C类.txt文件中读取基金代码列表，并跳过可能的标题行。
    """
    if not os.path.exists(filepath):
        print(f"错误: 找不到文件 {filepath}")
        return []
    
    with open(filepath, 'r', encoding='utf-8') as f:
        # 读取所有行，去除首尾空白，并过滤掉空行和标题行 (e.g., 'code')
        lines = [line.strip() for line in f.readlines()]
    
    # 假设第一行可能是标题'code'
    codes = [line for line in lines if line and line.lower() != 'code']
    
    # 仅保留长度为6或更多的有效代码
    return [code for code in codes if len(code) >= 6]

def scrape_fund_info(fund_code):
    """
    抓取单个基金的基本概况信息。
    """
    base_url = "https://fundf10.eastmoney.com/jbgk_{}.html"
    url = base_url.format(fund_code)
    
    print(f"-> 正在抓取基金代码: {fund_code}")
    
    try:
        # 设置短延时和超时，避免请求过快被屏蔽
        time.sleep(1)
        response = requests.get(url, headers=HEADERS, timeout=15)
        
        if response.status_code != 200:
            print(f"   警告: 基金 {fund_code} 状态码 {response.status_code}. 跳过.")
            return None

        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 目标表格 class 是 "info"
        info_table = soup.find('table', class_='info')
        
        if not info_table:
            print(f"   警告: 基金 {fund_code} 页面未找到基本概况信息表. 跳过.")
            return None

        fund_data = {'基金代码': fund_code}
        
        # 表格结构是 <th>Key</th><td>Value</td><th>Key</th><td>Value</td>...
        # 提取所有 th 和 td 的文本
        cells = [elem.get_text(strip=True) for elem in info_table.find_all(['th', 'td'])]
        
        # 由于是 th 和 td 交替出现，可以每两个元素一组进行处理
        for i in range(0, len(cells), 2):
            if i + 1 < len(cells):
                key = cells[i].strip('：') # 移除键后面的冒号
                value = cells[i+1]
                
                # 清理值中的冗余信息 (例如：基金代码后面的“（前端）”或链接文本)
                if key in ['基金代码', '最高申购费率', '最高赎回费率', '最高认购费率']:
                    # 移除括号及内部内容，例如 "014194（前端）" -> "014194"
                    value = value.split('（')[0].strip()
                    # 移除可能包含的优惠信息 "1.50%前端天天基金优惠费率：0.15%" -> "1.50%"
                    if '优惠费率' in value:
                        value = value.split('天天基金优惠费率')[0].strip()
                
                # 对于包含链接的字段，BeautifulSoup strip=True 已经提取了链接文本，但为了保险，
                # 对于规模和分红等字段，检查并移除可能残余的链接提示文本
                if key in ['份额规模', '成立来分红', '资产规模']:
                    # 移除截止日期提示，例如 "3.85亿元（截止至：2025年09月30日）"
                    if '（截止至：' in value:
                        value = value.split('（截止至：')[0].strip()
                    # 移除分红的次数提示，例如 "每份累计0.00元（0次）"
                    elif '（' in value and '次' in value:
                        value = value.split('（')[0].strip()
                
                fund_data[key] = value

        return fund_data

    except requests.RequestException as e:
        print(f"   错误: 基金 {fund_code} 抓取请求失败: {e}. 跳过.")
        return None
    except Exception as e:
        print(f"   错误: 基金 {fund_code} 抓取过程中发生未知错误: {e}. 跳过.")
        return None

def main():
    fund_codes = get_fund_codes()
    if not fund_codes:
        print("未找到任何基金代码，脚本退出。")
        return

    print(f"共找到 {len(fund_codes)} 个基金代码，开始抓取...")
    all_fund_data = []
    
    for code in fund_codes:
        data = scrape_fund_info(code)
        if data:
            all_fund_data.append(data)

    if not all_fund_data:
        print("未成功抓取任何基金数据，未生成CSV文件。")
        return

    # 将数据转换为DataFrame
    df = pd.DataFrame(all_fund_data)
    
    # 确保'基金代码'是第一列
    cols = ['基金代码'] + [col for col in df.columns if col != '基金代码']
    df = df[cols]

    # 保存为CSV文件，使用 utf_8_sig 编码以确保 Excel 中文显示正常
    output_filename = 'fund_data.csv'
    df.to_csv(output_filename, index=False, encoding='utf_8_sig')
    
    print(f"\n✅ 数据抓取完成，已保存到文件: {output_filename}")
    print(f"   共抓取 {len(all_fund_data)} 条数据。")

if __name__ == "__main__":
    main()
