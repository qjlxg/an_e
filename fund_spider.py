import pandas as pd
import requests
import os
import time
import asyncio
import aiohttp
from aiohttp import ClientSession
from bs4 import BeautifulSoup
import re
import math
from datetime import datetime, timedelta
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_message
import concurrent.futures
import json5 
import logging
from functools import partial 

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 定义文件路径和目录
INPUT_FILE = 'C类.txt' 
OUTPUT_DIR = 'fund_data'
# 仅保留净值 API
BASE_URL_NET_VALUE = "http://fundf10.eastmoney.com/F10DataApi.aspx?type=lsjz&code={fund_code}&page={page_index}&per=20"

# 设置请求头
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Referer': 'http://fund.eastmoney.com/',
}

REQUEST_TIMEOUT = 30
REQUEST_DELAY = 3.5 
MAX_CONCURRENT = 15 
MAX_FUNDS_PER_RUN = 0 # 
PAGE_SIZE = 20

# --------------------------------------------------------------------------------------
# 文件和代码读取 
# --------------------------------------------------------------------------------------

def get_all_fund_codes(file_path):
    """从 C类.txt 文件中读取基金代码"""
    print(f"尝试读取基金代码文件: {file_path}")
    if not os.path.exists(file_path):
        print(f"[错误] 文件 {file_path} 不存在。")
        return []

    if os.path.getsize(file_path) == 0:
        print(f"[错误] 文件 {file_path} 为空。")
        return []

    encodings_to_try = ['utf-8', 'utf-8-sig', 'gbk', 'latin-1']
    df = None

    for encoding in encodings_to_try:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                codes = [line.strip() for line in f if line.strip()]
            
            df = pd.DataFrame(codes, columns=['code'])
            print(f"  -> 成功使用 {encoding} 编码读取文件，找到 {len(df)} 个基金代码。")
            break
        except UnicodeDecodeError as e:
            continue
        except Exception as e:
            continue

    if df is None or df.empty:
        print("[错误] 无法读取文件，请检查文件格式和编码。")
        return []

    codes = df['code'].dropna().astype(str).str.strip().unique().tolist()
    valid_codes = [code for code in codes if re.match(r'^\d{6}$', code)]
    if not valid_codes:
        print("[错误] 没有找到有效的6位基金代码。")
        return []
    print(f"  -> 找到 {len(valid_codes)} 个有效基金代码。")
    return valid_codes

# --------------------------------------------------------------------------------------
# 基金净值抓取核心逻辑 
# --------------------------------------------------------------------------------------

def load_latest_date(fund_code):
    """
    从本地 CSV 文件中读取现有最新日期，增加鲁棒性以避免读取错误。
    """
    output_path = os.path.join(OUTPUT_DIR, f"{fund_code}.csv")
    if os.path.exists(output_path):
        encodings_to_try = ['utf-8', 'gbk', 'utf-8-sig'] 
        
        for encoding in encodings_to_try:
            try:
                df = pd.read_csv(
                    output_path, 
                    usecols=['date'], 
                    parse_dates=['date'], 
                    encoding=encoding
                )
                
                if not df.empty and 'date' in df.columns:
                    df.dropna(subset=['date'], inplace=True)
                    if not df.empty:
                        latest_date = df['date'].max().to_pydatetime().date()
                        logger.info(f"  -> 基金 {fund_code} 现有最新日期: {latest_date.strftime('%Y-%m-%d')} (使用 {encoding} 编码)")
                        return latest_date
            except Exception as e:
                continue

        logger.warning(f"  -> 基金 {fund_code} [重要警告]：无法准确读取本地 CSV 文件中的最新日期，将从头开始抓取！")
    
    return None

async def fetch_page(session, url):
    """异步请求页面"""
    async with session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT) as response:
        if response.status != 200:
            if response.status == 514:
                raise aiohttp.ClientError("Frequency Capped (HTTP 514)")
            raise aiohttp.ClientError(f"HTTP 错误: {response.status}")
        return await response.text()

async def fetch_net_values(fund_code, session, semaphore, executor):
    """使用“最新日期”作为停止条件，实现智能增量更新 (已修复增量停止逻辑)"""
    print(f"-> [START] 基金代码 {fund_code}")
    
    async with semaphore:
        all_records = []
        page_index = 1
        total_pages = 1
        first_run = True
        dynamic_delay = REQUEST_DELAY
        
        latest_date = await asyncio.get_event_loop().run_in_executor(executor, load_latest_date, fund_code)
        
        if latest_date:
            print(f"    基金 {fund_code} 开启增量更新模式，将抓取 {latest_date.strftime('%Y-%m-%d')} 之后的数据。")
        
        latest_api_date = None 

        while page_index <= total_pages:
            url = BASE_URL_NET_VALUE.format(fund_code=fund_code, page_index=page_index)
            
            try:
                if page_index > 1:
                    await asyncio.sleep(dynamic_delay)
                
                text = await fetch_page(session, url)
                
                soup = BeautifulSoup(text, 'lxml')
                
                if first_run:
                    total_pages_match = re.search(r'pages:(\d+)', text)
                    total_pages = int(total_pages_match.group(1)) if total_pages_match else 1
                    records_match = re.search(r'records:(\d+)', text)
                    total_records = int(records_match.group(1)) if records_match else '未知'
                    logger.info(f"    基金 {fund_code} 信息：总页数 {total_pages}，总记录数 {total_records}。")
                    
                    if total_records == '未知' or (isinstance(total_records, int) and total_records == 0):
                        logger.warning(f"    基金 {fund_code} [跳过]：API 返回总记录数为 0 或未知。")
                        return fund_code, "API返回记录数为0或代码无效"
                        
                    first_run = False
                
                
                table = soup.find('table')
                if not table:
                    if page_index == 1 and total_pages == 1 and total_records == '未知':
                        logger.warning(f"    基金 {fund_code} [警告]：页面 {page_index} 无表格数据且记录数未知。")
                        return fund_code, "API返回记录数为0或代码无效"
                    elif page_index > 1:
                         logger.info(f"    基金 {fund_code} [停止]：第 {page_index} 页无表格数据。提前停止。")
                    break

                rows = table.find_all('tr')[1:]
                if not rows:
                    logger.info(f"    基金 {fund_code} 第 {page_index} 页无数据行。停止抓取。")
                    break

                page_records = []
                stop_paging = False 
                
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) < 7:
                        continue
                        
                    date_str = cols[0].text.strip()
                    net_value_str = cols[1].text.strip()
                    cumulative_net_value = cols[2].text.strip()
                    daily_growth_rate = cols[3].text.strip()
                    purchase_status = cols[4].text.strip()
                    redemption_status = cols[5].text.strip()
                    dividend = cols[6].text.strip()
                    
                    if not date_str or not net_value_str:
                        continue
                        
                    try:
                        date = datetime.strptime(date_str, '%Y-%m-%d').date()
                        if not latest_api_date or date > latest_api_date:
                            latest_api_date = date
                        
                        # 增量停止逻辑：如果当前记录日期 <= 本地最新日期
                        if latest_date and date <= latest_date:
                            stop_paging = True
                            break
                            
                        # 如果是新数据 (date > latest_date)，则加入记录
                        page_records.append({
                            'date': date_str,
                            'net_value': net_value_str,
                            'cumulative_net_value': cumulative_net_value,
                            'daily_growth_rate': daily_growth_rate,
                            'purchase_status': purchase_status,
                            'redemption_status': redemption_status,
                            'dividend': dividend
                        })
                    except ValueError:
                        continue
                
                if latest_api_date and page_index == 1:
                    logger.info(f"    基金 {fund_code} API 返回最新日期: {latest_api_date.strftime('%Y-%m-%d')}")
                
                all_records.extend(page_records)
                
                if stop_paging:
                    print(f"    基金 {fund_code} [增量停止]：页面 {page_index} 遇到旧数据 ({latest_date.strftime('%Y-%m-%d')})，停止翻页。")
                    break
                
                if page_index < total_pages and len(rows) < PAGE_SIZE:
                    logger.warning(f"    基金 {fund_code} [警告]：页面 {page_index} 数据量异常，提前停止翻页。")
                    break
                    
                page_index += 1
                dynamic_delay = max(REQUEST_DELAY * 0.5, dynamic_delay * 0.9) 

            except aiohttp.ClientError as e:
                if "Frequency Capped" in str(e):
                    dynamic_delay = min(dynamic_delay * 2.5, 10.0) 
                    print(f"    基金 {fund_code} [警告]：频率限制，延迟调整为 {dynamic_delay:.2f} 秒，重试第 {page_index} 页")
                    continue
                print(f"    基金 {fund_code} [错误]：请求 API 时发生网络错误 (超时/连接) 在第 {page_index} 页: {e}")
                return fund_code, f"网络错误: {e}"
            except Exception as e:
                # ----------------------- 【修改点：增加详细日志】 -----------------------
                print(f"    基金 {fund_code} [错误]：处理数据时发生意外错误在第 {page_index} 页: {e}")
                logger.exception(f"    基金 {fund_code} [详细错误]：数据处理或解析失败在第 {page_index} 页。") 
                # ----------------------------------------------------------------------
                return fund_code, f"数据处理错误: {e}"
            
        print(f"-> [COMPLETE] 基金 {fund_code} 数据抓取完毕，共获取 {len(all_records)} 条新记录。")
        if not all_records:
            if latest_date:
                if latest_api_date and latest_date >= latest_api_date:
                     return fund_code, f"数据已是最新 ({latest_date.strftime('%Y-%m-%d')})，无新数据"
                else:
                    return fund_code, "未获取到新数据（可能是API未更新或基金已停售）"
            else:
                return fund_code, "未获取到新数据"

        return fund_code, all_records

def save_to_csv(fund_code, data):
    """
    【修复后的 save_to_csv 函数】
    - 增强读取现有数据的编码鲁棒性。
    - 确保数据合并和去重逻辑的正确性。
    """
    output_path = os.path.join(OUTPUT_DIR, f"{fund_code}.csv")
    if not isinstance(data, list) or not data:
        return False, 0

    new_df = pd.DataFrame(data)

    try:
        # 数据类型转换和清洗
        new_df['net_value'] = pd.to_numeric(new_df['net_value'], errors='coerce').round(4)
        new_df['cumulative_net_value'] = pd.to_numeric(new_df['cumulative_net_value'], errors='coerce').round(4)
        
        def clean_growth_rate(rate_str):
            rate_str = str(rate_str).strip()
            if rate_str in ['--', '']:
                return 0.0
            try:
                return float(rate_str.rstrip('%')) / 100.0
            except ValueError:
                return None 
        
        new_df['daily_growth_rate'] = new_df['daily_growth_rate'].apply(clean_growth_rate)
        new_df['date'] = pd.to_datetime(new_df['date'], errors='coerce', format='%Y-%m-%d')
        new_df.dropna(subset=['date', 'net_value', 'daily_growth_rate'], inplace=True)
        
        if new_df.empty:
            return False, 0
    except Exception as e:
        print(f"    基金 {fund_code} 数据转换失败: {e}")
        return False, 0
    
    old_record_count = 0
    existing_df = None

    if os.path.exists(output_path):
        # **【修复点】增强读取现有数据的编码鲁棒性**
        encodings_to_try = ['utf-8', 'gbk', 'utf-8-sig'] 
        
        for encoding in encodings_to_try:
            try:
                existing_df = pd.read_csv(
                    output_path, 
                    parse_dates=['date'], 
                    encoding=encoding
                )
                if not existing_df.empty and 'date' in existing_df.columns:
                     existing_df.dropna(subset=['date'], inplace=True)
                
                if not existing_df.empty:
                    break 
                else:
                    existing_df = None
            except Exception:
                continue

        if existing_df is not None:
            try:
                old_record_count = len(existing_df)
                combined_df = pd.concat([new_df, existing_df], ignore_index=True)
            except Exception as e:
                print(f"    基金 {fund_code} 读取旧数据成功但合并失败: {e}。仅保存新数据。")
                combined_df = new_df
        else:
            print(f"    基金 {fund_code} [警告]：无法读取现有 CSV 文件，可能是编码或格式问题。仅保存新数据。")
            combined_df = new_df
    else:
        combined_df = new_df
        
    # 去重和排序逻辑
    combined_df['date'] = pd.to_datetime(combined_df['date'], errors='coerce')
    combined_df.dropna(subset=['date'], inplace=True)

    final_df = combined_df.drop_duplicates(subset=['date'], keep='first')
    final_df = final_df.sort_values(by='date', ascending=False)
    final_df['date'] = final_df['date'].dt.strftime('%Y-%m-%d')
    
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        # 写入时使用 UTF-8 编码，以保证跨平台兼容性
        final_df.to_csv(output_path, index=False, encoding='utf-8') 
        new_record_count = len(final_df)
        newly_added = new_record_count - old_record_count
        print(f"    -> 基金 {fund_code} [保存完成]：总记录数 {new_record_count} (新增 {max(0, newly_added)} 条)。")
        return True, max(0, newly_added)
    except Exception as e:
        print(f"    基金 {fund_code} 保存 CSV 文件 {output_path} 失败: {e}")
        return False, 0

async def fetch_all_funds(fund_codes):
    """异步获取所有基金数据，并在任务完成时立即保存数据"""
    print("\n======== 开始基金净值数据抓取（动态数据）========\n")

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    loop = asyncio.get_event_loop()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count() * 2 + 1) as executor:

        connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT + 5) 

        async with ClientSession(connector=connector) as session:
            fetch_tasks = [fetch_net_values(fund_code, session, semaphore, executor) for fund_code in fund_codes]
            
            success_count = 0
            total_new_records = 0
            failed_codes = set() 
            
            save_executor = partial(loop.run_in_executor, executor, save_to_csv)

            for future in asyncio.as_completed(fetch_tasks):
                print("-" * 30)
                fund_code = "UNKNOWN"
                try:
                    result = await future
                    if isinstance(result, tuple) and len(result) == 2:
                        fund_code, net_values = result
                    else:
                        raise Exception("Fetch task returned unexpected format.")
                except Exception as e:
                    print(f"[错误] 处理基金数据时发生顶级异步错误: {e}")
                    failed_codes.add(fund_code if fund_code != "UNKNOWN" else "UNKNOWN_ERROR")
                    continue


                if isinstance(net_values, list):
                    try:
                        success, new_records = await save_executor(fund_code, net_values) 
                        if success:
                            success_count += 1
                            total_new_records += new_records
                        else:
                            failed_codes.add(fund_code)
                    except Exception as e:
                        print(f"[错误] 基金 {fund_code} 的保存任务在线程中发生错误: {e}")
                        failed_codes.add(fund_code)
                else:
                    if not str(net_values).startswith('数据已是最新'):
                        failed_codes.add(fund_code)

        return success_count, total_new_records, list(failed_codes) 

def main():
    """主函数：执行动态净值抓取"""
    print(f"加速设置：并发数={MAX_CONCURRENT}，基础延迟={REQUEST_DELAY}秒")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"确保输出目录存在: {OUTPUT_DIR}")

    start_time = time.time()
    
    fund_codes = get_all_fund_codes(INPUT_FILE)
    if not fund_codes:
        print("[错误] 没有可处理的基金代码，脚本结束。")
        return

    if MAX_FUNDS_PER_RUN > 0 and len(fund_codes) > MAX_FUNDS_PER_RUN:
        print(f"限制本次运行最多处理 {MAX_FUNDS_PER_RUN} 个基金。")
        processed_codes = fund_codes[:MAX_FUNDS_PER_RUN]
    else:
        processed_codes = fund_codes
    print(f"本次处理 {len(processed_codes)} 个基金。")

    try:
        if os.name == 'nt':
            try:
                loop = asyncio.ProactorEventLoop()
                asyncio.set_event_loop(loop)
            except Exception:
                loop = asyncio.get_event_loop()
        else:
             loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    success_count, total_new_records, failed_codes = loop.run_until_complete(fetch_all_funds(processed_codes))
    
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"\n======== 本次更新总结 ========")
    print(f"总耗时: {duration:.2f} 秒")
    print(f"成功处理 {success_count} 个基金，新增/更新 {total_new_records} 条记录，失败 {len(failed_codes)} 个基金。")
    if failed_codes:
        print(f"失败的基金代码: {', '.join(failed_codes)}")
    if total_new_records == 0:
        print("[警告] 未新增任何记录，可能是数据已是最新，或 API 无新数据。")
    print(f"==============================")

if __name__ == "__main__":
    main()
