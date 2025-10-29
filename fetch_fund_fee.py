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
OUTPUT_FILE = "fund_fee_result.csv"
# 读取基金代码的文件名
FUND_CODES_FILE = "C类.txt"
# 调试抓取的基金数量 (GitHub Actions运行时，如需抓取全部，请将此值设为 None 或一个很大的数)
LIMIT_FUNDS = 10 

# --- 1. 抓取与解析函数 ---

def parse_fund_fees(html_content, fund_code):
    """
    解析天天基金网基金费率页面（jjfl_<code>.html）的HTML内容，提取关键费率数据。
    """
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # 1. 提取基金名称: 使用更可靠的定位方式
        fund_name = f"基金({fund_code})"
        try:
            # 尝试查找页面的主标题区域
            name_tag = soup.find('div', class_='fundDetail-main').find('div', class_='box_p')
            if name_tag:
                 # 从 h4 标签中提取名称，去除代码和多余空格
                 name_link = name_tag.find('h4').find('a')
                 if name_link:
                    fund_name = name_link.text.split('(')[0].strip()
        except Exception:
            pass # 无法获取名称时使用默认值
        
        # 2. 提取运作费用 (管理费, 托管费, 销售服务费)
        op_fees_data = {}
        # 使用 lambda 函数进行更灵活的h4匹配，确保找到包含"运作费用"的h4
        op_h4 = soup.find('h4', string=lambda t: t and "运作费用" in t)
        op_fees_table = op_h4.find_next('table') if op_h4 else None
        
        if op_fees_table:
            rows = op_fees_table.find_all('tr')
            if rows:
                cols = rows[0].find_all('td')
                if len(cols) >= 6: 
                    # 假定运作费用总是第一个表格的第一行中的第2, 4, 6个 td
                    op_fees_data['管理费率（每年）'] = cols[1].text.strip()
                    op_fees_data['托管费率（每年）'] = cols[3].text.strip()
                    op_fees_data['销售服务费率（每年）'] = cols[5].text.strip()
        
        # 3. 提取赎回费率 (Redemption Fees)
        redemption_fees = {}
        sh_h4 = soup.find('h4', string=lambda t: t and "赎回费率" in t)
        redemption_table = sh_h4.find_next('table') if sh_h4 else None
        
        if redemption_table:
            # 找到 tbody
            tbody = redemption_table.find('tbody')
            rows = tbody.find_all('tr') if tbody else redemption_table.find_all('tr')
            
            for row in rows:
                cols = row.find_all('td')
                if len(cols) == 3:
                    term = cols[1].text.strip()
                    rate = cols[2].text.strip()
                    
                    if '小于7天' in term:
                        redemption_fees['赎回费率（小于7天）'] = rate
                    elif '大于等于7天' in term:
                        redemption_fees['赎回费率（大于等于7天）'] = rate
        
        # 4. 提取申购费率（C类基金通常为0）
        sub_fee_rate = '0.00%' # C类基金申购费率通常为0，简化逻辑
        
        # 5. 整合数据
        data = {
            '基金代码': fund_code,
            '基金名称': fund_name,
            '申购费率（前端，优惠）': sub_fee_rate,
            **op_fees_data,
            **redemption_fees
        }

        # 检查关键字段是否成功获取
        if not data.get('管理费率（每年）'):
             print(f"警告：基金 {fund_code} 抓取数据不完整（缺少管理费率），请检查网站结构是否再次变化。")
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
        response = requests.get(url, headers=headers, timeout=10)
        response.encoding = 'utf-8'
        response.raise_for_status() 

        data = parse_fund_fees(response.text, fund_code)
        
        if data:
            print(f"✅ 成功抓取并解析基金 {fund_code}: {data.get('基金名称', '')}")
        else:
            print(f"❌ 未能解析基金 {fund_code} 的数据。")
        
        return data

    except requests.exceptions.HTTPError:
        print(f"❌ 抓取基金 {fund_code} 失败: HTTP 错误 {response.status_code}")
    except requests.exceptions.RequestException:
        print(f"❌ 抓取基金 {fund_code} 失败: 请求错误")
    except Exception:
        print(f"❌ 抓取基金 {fund_code} 发生未知错误")
        
    return None

# --- 2. 主执行逻辑 ---

def main():
    fund_codes = []
    try:
        with open(FUND_CODES_FILE, 'r', encoding='utf-8') as f:
            fund_codes = [line.strip() for line in f.readlines() if line.strip() and line.strip() != 'code']
    except FileNotFoundError:
        print(f"错误: 未找到文件 {FUND_CODES_FILE}。请确保文件存在。")
        return

    if not fund_codes:
        print("错误: 文件中未找到基金代码。")
        return
    
    # 根据 LIMIT_FUNDS 变量确定抓取的代码列表
    codes_to_fetch = fund_codes[:LIMIT_FUNDS] if LIMIT_FUNDS else fund_codes
    print(f"成功读取 {len(fund_codes)} 个代码。开始并行抓取前 {len(codes_to_fetch)} 只基金的费率数据...")

    all_data = []
    # 使用 ThreadPoolExecutor 进行线程池并行抓取
    # 减少 max_workers 可以降低被目标网站拒绝的风险，但会延长总时间
    with ThreadPoolExecutor(max_workers=5) as executor: 
        futures = [executor.submit(fetch_fund_data, code) for code in codes_to_fetch]
        
        for future in futures:
            result = future.result()
            if result:
                all_data.append(result)

    if not all_data:
        print("\n未能成功抓取任何数据。")
        # 即使抓取失败，也尝试创建空 DataFrame 或空文件，防止 Git 步骤因文件不存在而失败
        df = pd.DataFrame(columns=[
            '基金代码', '基金名称', '管理费率（每年）', '托管费率（每年）', 
            '销售服务费率（每年）', '申购费率（前端，优惠）', '赎回费率（小于7天）', 
            '赎回费率（大于等于7天）'
        ])
        
        # 确保目录存在，并写入一个空CSV（带表头）
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
        df.to_csv(output_path, index=False, encoding='utf_8_sig')
        print(f"\n警告：抓取失败，已创建一个空文件 (含表头): {output_path}，请稍后检查网站结构。")
        return # 抓取失败，但创建了空文件，Git 步骤可以继续运行

    # 3. 数据处理与保存
    df = pd.DataFrame(all_data)
    
    columns_order = [
        '基金代码', '基金名称', '管理费率（每年）', '托管费率（每年）', 
        '销售服务费率（每年）', '申购费率（前端，优惠）', '赎回费率（小于7天）', 
        '赎回费率（大于等于7天）'
    ]
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
