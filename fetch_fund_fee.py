import os
import re
import pandas as pd
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor

# --- 配置常量 ---
# 天天基金网 F10 档案中费率信息的 URL 模板
BASE_URL = "http://fundf10.eastmoney.com/jjfl_{}.html"
# 输出目录和文件名
OUTPUT_DIR = "fund_data"
OUTPUT_FILE = "C类基金费率数据_前10只.csv"
# 读取基金代码的文件名
FUND_CODES_FILE = "C类.txt"
# 调试抓取的基金数量
LIMIT_FUNDS = 10 

# --- 1. 抓取与解析函数 ---

def parse_fund_fees(html_content, fund_code):
    """
    解析天天基金网基金费率页面（jjfl_<code>.html）的HTML内容，提取关键费率数据。
    """
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # 提取基金名称 (位于页面的标题或基本信息区域)
        name_tag = soup.find('h4', class_='title')
        fund_name = name_tag.text.split('(')[0].strip() if name_tag else f"基金({fund_code})"

        # 提取运作费用 (管理费, 托管费, 销售服务费)
        op_fees_data = {}
        op_fees_table = soup.find('h4', string=re.compile("运作费用")).find_next('table')
        if op_fees_table:
            # 找到所有运作费用的行
            rows = op_fees_table.find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                if len(cols) == 6: # 确保是包含三对费率的行
                    # 抓取：管理费率/托管费率/销售服务费率
                    op_fees_data['管理费率（每年）'] = cols[1].text.strip()
                    op_fees_data['托管费率（每年）'] = cols[3].text.strip()
                    op_fees_data['销售服务费率（每年）'] = cols[5].text.strip()
                    break # 只需要第一行

        # 提取赎回费率 (Redemption Fees)
        redemption_fees = {}
        redemption_table = soup.find('h4', string=re.compile("赎回费率")).find_next('table')
        if redemption_table:
            # 找到所有赎回费率行
            rows = redemption_table.find('tbody').find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                if len(cols) == 3:
                    # 适用期限 (e.g., 小于7天, 大于等于7天)
                    term = cols[1].text.strip()
                    # 费率
                    rate = cols[2].text.strip()
                    
                    if '小于7天' in term:
                        redemption_fees['赎回费率（小于7天）'] = rate
                    elif '大于等于7天' in term:
                        redemption_fees['赎回费率（大于等于7天）'] = rate
        
        # 提取申购费率（C类基金通常为0）
        # 尝试查找包含“申购费率（前端）”或“认购费率（前端）”的表格
        sub_fee_rate = 'N/A'
        try:
            # 对于C类基金，通常只有一条优惠费率0.00%
            sg_table = soup.find('h4', string=re.compile("申购费率（前端）")).find_next('table')
            if sg_table:
                # 寻找包含“优惠费率”的列，并取其费率值
                rate_col = sg_table.find('th', class_='speciacol')
                if rate_col:
                    # 获取表格主体，通常费率在 tbody 的第三个 td 中
                    rate_td = sg_table.find('tbody').find('tr').find_all('td')[-1].text.strip()
                    sub_fee_rate = rate_td
        except Exception:
            pass # 找不到说明可能没有申购费率表或结构有变，保持 N/A

        # 整合数据
        data = {
            '基金代码': fund_code,
            '基金名称': fund_name,
            '申购费率（前端，优惠）': sub_fee_rate,
            **op_fees_data,
            **redemption_fees
        }

        # 检查关键字段是否缺失，对于C类基金申购费率应为0
        if not data.get('管理费率（每年）') or not data.get('赎回费率（小于7天）'):
             print(f"警告：基金 {fund_code} 抓取数据不完整，可能网站结构已变。")
             return None

        return data

    except Exception as e:
        print(f"处理基金 {fund_code} 时发生错误: {e}")
        return None

def fetch_fund_data(fund_code):
    """
    从天天基金网获取单个基金的费率页面并解析数据。
    """
    url = BASE_URL.format(fund_code)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        # 发起 HTTP GET 请求
        response = requests.get(url, headers=headers, timeout=10)
        response.encoding = 'utf-8'
        response.raise_for_status() # 检查HTTP请求是否成功

        # 解析 HTML 并提取数据
        data = parse_fund_fees(response.text, fund_code)
        
        if data:
            print(f"成功抓取并解析基金 {fund_code}: {data.get('基金名称', '')}")
        else:
            print(f"未能解析基金 {fund_code} 的数据。")
        
        return data

    except requests.exceptions.HTTPError as e:
        print(f"抓取基金 {fund_code} 失败: HTTP 错误 {e.response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"抓取基金 {fund_code} 失败: 请求错误 {e}")
    except Exception as e:
        print(f"抓取基金 {fund_code} 发生未知错误: {e}")
        
    return None

# --- 2. 主执行逻辑 ---

def main():
    """
    读取基金代码，并发抓取数据，并保存到 CSV 文件。
    """
    # 1. 读取基金代码
    fund_codes = []
    try:
        with open(FUND_CODES_FILE, 'r', encoding='utf-8') as f:
            # 跳过第一行（'code'）
            fund_codes = [line.strip() for line in f.readlines() if line.strip() and line.strip() != 'code']
    except FileNotFoundError:
        print(f"错误: 未找到文件 {FUND_CODES_FILE}。请确保文件存在。")
        return

    if not fund_codes:
        print("错误: 文件中未找到基金代码。")
        return
    
    # 取前 LIMIT_FUNDS 只进行调试
    codes_to_fetch = fund_codes[:LIMIT_FUNDS]
    print(f"成功读取 {len(fund_codes)} 个代码。开始抓取前 {len(codes_to_fetch)} 只基金的费率数据...")

    # 2. 并行抓取数据
    all_data = []
    # 使用 ThreadPoolExecutor 进行线程池并行抓取，最大线程数设置为10
    with ThreadPoolExecutor(max_workers=10) as executor:
        # 提交任务到线程池
        futures = [executor.submit(fetch_fund_data, code) for code in codes_to_fetch]
        
        # 获取结果
        for future in futures:
            result = future.result()
            if result:
                all_data.append(result)

    if not all_data:
        print("\n未能成功抓取任何数据。请检查网络连接或网站结构是否发生变化。")
        return

    # 3. 数据处理与保存
    df = pd.DataFrame(all_data)
    
    # 重新排序列，使关键信息在前
    columns_order = [
        '基金代码', '基金名称', '管理费率（每年）', '托管费率（每年）', 
        '销售服务费率（每年）', '申购费率（前端，优惠）', '赎回费率（小于7天）', 
        '赎回费率（大于等于7天）'
    ]
    # 确保只包含 DataFrame 中已有的列
    final_columns = [col for col in columns_order if col in df.columns]
    df = df[final_columns]

    # 创建目录并保存 CSV
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
    
    # 使用 utf_8_sig 编码以确保 Excel 打开时中文不乱码
    df.to_csv(output_path, index=False, encoding='utf_8_sig') 

    print("\n" + "="*50)
    print(f"✅ 抓取完成！共成功抓取 {len(all_data)} 只基金的数据。")
    print(f"文件已保存到: {output_path}")
    print("="*50)

if __name__ == '__main__':
    main()
