import pandas as pd
import requests
import os
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- ⚠️ 依赖的库: requests 和 pandas ---
OUTPUT_FILE = 'fund_details.csv'
INPUT_FILE = 'result_z.txt'
MAX_WORKERS = 10 

# 假设的公开基金数据接口 (此URL仅为演示结构，您需要替换为实际可用的数据源)
# ⚠️ 爬虫存在风险，请确保您的爬取行为遵守目标网站的使用条款。
BASE_URL = "https://example.com/api/fund/basic?code=" 

def fetch_fund_info(fund_code):
    """
    使用 requests 库尝试获取单个基金的基本信息。
    """
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 正在查询代码: {fund_code}")
    
    # 模拟 HTTP 请求头，避免被简单的反爬虫机制拦截
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    url = f"https://fund.eastmoney.com/{fund_code}.html" # 换用东方财富的基金详情页URL作为演示

    try:
        # ⚠️ 实际爬取需要解析 HTML 或找到一个公开的 JSON 接口
        # 为了演示，我们使用模拟数据结构来代替复杂的爬虫解析
        
        # 模拟网络请求
        response = requests.get(BASE_URL + fund_code, headers=headers, timeout=5)
        response.raise_for_status() # 检查 HTTP 状态码
        
        # 假设接口返回JSON数据
        # data = response.json().get('data', {})
        
        # ---------------- 模拟成功获取的数据结构 ----------------
        if fund_code.startswith('0'):
            data = {
                'code': fund_code,
                'name': f'某某基金-{fund_code}',
                'manager': f'经理 B{fund_code[-2:]}',
                'company': '某某基金管理公司',
                'establish_date': '2021-03-01',
                'risk_level': '中',
            }
        else:
            data = {}
        # ---------------------------------------------------------

        if not data:
            return {
                'code': fund_code,
                'name': '数据暂无/代码错误',
                'reason': 'API/爬取返回空',
                'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
        
        # 整理数据
        details = {
            'code': data.get('code', fund_code),
            'name': data.get('name', 'N/A'),
            'manager': data.get('manager', 'N/A'),
            'company': data.get('company', 'N/A'),
            'establish_date': data.get('establish_date', 'N/A'),
            'risk_level': data.get('risk_level', 'N/A'),
            'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        time.sleep(0.1) # 轻微延迟，防止爬取过于频繁
        return details
        
    except requests.exceptions.RequestException as e:
        print(f"基金代码 {fund_code} 请求失败: {e}")
        time.sleep(1) 
        return {
            'code': fund_code,
            'name': '网络请求失败',
            'reason': str(e),
            'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }


def main():
    
    # 1. 读取基金代码
    print(f"尝试读取文件: {INPUT_FILE}")
    try:
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            fund_codes = [line.strip() for line in f if line.strip()]
        
        fund_codes = list(dict.fromkeys(fund_codes))
        print(f"成功读取 {len(fund_codes)} 个基金代码。")
        
    except FileNotFoundError:
        print(f"错误: 找不到输入文件 {INPUT_FILE}")
        return
    
    # 2. 批量并行获取基金信息
    all_fund_details = []
    print(f"开始并行获取基金基本信息，最大线程数: {MAX_WORKERS}...")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # 提交所有任务
        future_to_code = {executor.submit(fetch_fund_info, code): code for code in fund_codes}
        
        # 收集完成的结果
        for future in as_completed(future_to_code):
            try:
                data = future.result()
                all_fund_details.append(data)
            except Exception as exc:
                # 捕获线程执行时的意外错误
                print(f'一个线程执行发生错误: {exc}')

    print("所有基金信息获取和处理完成。")
    
    # 3. 转换为 DataFrame 并保存为 CSV
    if not all_fund_details:
        print("没有获取到任何有效数据，跳过文件保存。")
        return

    df = pd.DataFrame(all_fund_details)
    
    # 确保 CSV 文件的编码和分隔符兼容性
    df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
        
    print(f"所有基金信息已保存到 CSV 文件: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
