import pandas as pd
import requests
import os
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- ⚠️ 依赖的库: requests 和 pandas ---
OUTPUT_FILE = 'fund_details.csv'
INPUT_FILE = 'result_z.txt'
# 降低并发数，保护接口，防止被封
MAX_WORKERS = 5 

# 实际公开的基金基础信息查询接口 (数据来源于东方财富网公开接口)
# F=基金代码|基金名称|成立日期|基金公司|现任基金经理
BASE_URL = "http://fundgz.1234567.com.cn/js/{fund_code}.js?rt={timestamp}"


def fetch_fund_info(fund_code):
    """
    爬取基金基础信息。
    """
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 正在查询代码: {fund_code}")
    
    url = BASE_URL.format(fund_code=fund_code, timestamp=int(time.time() * 1000))
    
    # 模拟 HTTP 请求头
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Referer': 'http://fundgz.1234567.com.cn/' # 伪造来源，提高成功率
    }

    try:
        response = requests.get(url, headers=headers, timeout=5)
        response.raise_for_status() # 检查 HTTP 状态码
        
        # 接口返回的数据是 JavaScript 格式：jsonpgz(...);
        text = response.text
        
        # 提取括号内的 JSON 字符串
        if text.startswith('jsonpgz(') and text.endswith(');'):
            json_str = text[8:-2]
            data = pd.read_json(json_str, typ='series')
            
            # 从返回的数据中提取基本信息
            details = {
                '基金代码': data.get('fundcode', fund_code),
                '基金名称': data.get('name', 'N/A'),
                '单位净值估算': data.get('gsz', 'N/A'), # 估算净值
                '估算日期': data.get('gztime', 'N/A'), # 估算时间
                '今日估算涨幅': data.get('gszzl', 'N/A'),
                '更新时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # 注意：这个公开接口主要提供净值估算，不直接提供成立日期、经理等信息。
            # 如果需要更详细信息，需要爬取更复杂的页面，或使用付费API。
            
            time.sleep(0.2) # 轻微延迟，防止爬取过于频繁
            return details
            
        else:
            print(f"基金代码 {fund_code} 返回格式错误或无数据。")
            return {
                '基金代码': fund_code,
                '基金名称': '数据获取失败',
                '单位净值估算': 'N/A',
                '估算日期': 'N/A',
                '今日估算涨幅': 'N/A',
                '更新时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
        
    except requests.exceptions.RequestException as e:
        print(f"基金代码 {fund_code} 请求失败: {e}")
        time.sleep(1) 
        return {
            '基金代码': fund_code,
            '基金名称': '网络请求异常',
            '单位净值估算': 'N/A',
            '估算日期': 'N/A',
            '今日估算涨幅': 'N/A',
            '更新时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
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
    
    # 检查 pandas 是否可用
    if 'pd' not in globals():
        print("错误：缺少 pandas 库。")
        return

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_code = {executor.submit(fetch_fund_info, code): code for code in fund_codes}
        
        for future in as_completed(future_to_code):
            try:
                data = future.result()
                all_fund_details.append(data)
            except Exception as exc:
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
