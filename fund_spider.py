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
from datetime import datetime, date
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_message
import concurrent.futures
# 假设您使用 json5 来处理东财返回的非严格 JSON 数据
import json5 
import logging
# import jsbeautifier # 不再需要，移除
from functools import partial 

# 配置日志
# 确保日志级别能显示 INFO 和 WARNING
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
# 优化 2: 减少基础延迟，提高请求频率 (原 3.5)
REQUEST_DELAY = 3.5 
# 优化 1: 增加最大并发数 (原 5)
MAX_CONCURRENT_TASKS = 5 
MAX_FUNDS_PER_RUN = 20 # 0 表示不限制

# =========================================================================
#                    【新增】增量更新辅助函数
# =========================================================================

def to_date_object(date_str):
    """将'YYYY-MM-DD'格式的字符串转换为 datetime.date 对象"""
    if date_str:
        try:
            return datetime.strptime(date_str, '%Y-%m-%d').date()
        except ValueError:
            logger.warning(f"日期字符串解析失败: {date_str}")
    return None

def get_latest_local_date(fund_code):
    """尝试从本地CSV文件中读取最新的日期"""
    # 确保输出目录存在
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    file_name = os.path.join(OUTPUT_DIR, f"{fund_code}.csv")
    
    # 如果文件不存在，则返回 None，表示需要全量更新
    if not os.path.exists(file_name):
        return None 

    try:
        # 只读取第一行数据（假设数据是按日期降序排列的）
        # 强制指定 'date' 列为日期类型
        df = pd.read_csv(file_name, nrows=1, parse_dates=['date']) 
        
        if not df.empty and 'date' in df.columns:
            # 返回最新的日期对象（使用 .date() 仅获取日期部分）
            latest_date = df['date'].iloc[0]
            if pd.isna(latest_date):
                 return None
            return latest_date.date()
        return None
    except Exception as e:
        logger.warning(f"基金 {fund_code} 读取本地文件 {file_name} 失败: {e}。将执行全量下载。")
        return None

# =========================================================================
#                       API 数据解析 (抽象化处理)
# =========================================================================

def parse_fund_data_from_html(html_text):
    """
    【注意】此函数为东财F10 API响应的解析抽象。
    请确保您能够正确从 API 响应中提取总页数和记录列表。
    返回: total_page_count (int), records (list of dicts)
    """
    total_page_count = 0
    records = []
    
    try:
        # 查找总页数 (var pages = X;)
        page_match = re.search(r'var pages = (\d+);', html_text)
        if page_match:
            total_page_count = int(page_match.group(1))
        
        # 查找数据记录 (var datas = [...];)
        data_match = re.search(r'var datas = (\[.*?\]);', html_text, re.DOTALL)
        if data_match:
            # 使用 json5.loads 处理 Eastmoney 经常返回的非严格 JSON
            raw_data = json5.loads(data_match.group(1))
            
            # 字段映射：确保字段名与您的CSV文件头一致
            for item in raw_data:
                records.append({
                    'date': item.get('FSRQ'), # 日期
                    'net_value': item.get('DWJZ'), # 单位净值
                    'cumulative_net_value': item.get('LJJZ'), # 累计净值
                    'daily_growth_rate': item.get('JZZZL'), # 日增长率
                    'purchase_status': item.get('SHZT'), # 申购状态
                    'redemption_status': item.get('SGZT'), # 赎回状态
                    'dividend': item.get('FHJZ') # 分红
                })
        
        return total_page_count, records
    except Exception as e:
        logger.error(f"解析 API 数据失败: {e}")
        return 0, []


# =========================================================================
#                       数据抓取核心逻辑 (已修改)
# =========================================================================

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=60),
       retry=retry_if_exception_message(message="RETRY_REQUIRED"))
async def fetch_fund_data(fund_code, session, latest_local_date=None):
    """异步抓取单个基金的历史净值数据，支持增量更新。"""
    
    all_records = []
    
    logger.info(f"基金 {fund_code}: 开始抓取。本地最新日期: {latest_local_date}")

    # 1. 首次请求：获取总页数和第一页数据
    url = BASE_URL_NET_VALUE.format(fund_code=fund_code, page_index=1)
    try:
        async with session.get(url, timeout=REQUEST_TIMEOUT, headers=HEADERS) as response:
            response.raise_for_status()
            html_text = await response.text()
            total_page_count, first_page_data = parse_fund_data_from_html(html_text)
    except Exception as e:
        logger.error(f"基金 {fund_code}: 首次请求 API 失败: {e}")
        raise Exception("RETRY_REQUIRED") # 触发 tenacity 重试
        
    if not first_page_data:
        logger.warning(f"基金 {fund_code}: API 第一页数据为空或解析失败。")
        return []

    # 2. 增量更新逻辑判断 (核心逻辑)
    if latest_local_date:
        api_latest_date = to_date_object(first_page_data[0].get('date')) 

        if not api_latest_date:
             logger.warning(f"基金 {fund_code}: 无法从API获取最新日期，将尝试全量更新。")
        elif api_latest_date and latest_local_date >= api_latest_date:
            # 满足您的需求：本地日期 >= API 最新日期，跳过更新
            logger.info(f"基金 {fund_code}: 本地数据 {latest_local_date} 已是最新，API 最新为 {api_latest_date}，跳过更新。")
            return [] 
        else:
            logger.info(f"基金 {fund_code}: 本地数据 {latest_local_date} 落后于 API 最新 {api_latest_date}，开始增量更新...")
            
        # 过滤第一页中日期小于等于本地最新日期的数据
        newly_available_records = [
            record for record in first_page_data 
            if to_date_object(record.get('date')) and to_date_object(record.get('date')) > latest_local_date
        ]
        all_records.extend(newly_available_records)
    else:
        # 全量下载或首次下载
        all_records.extend(first_page_data)
        logger.info(f"基金 {fund_code}: 本地文件不存在或无最新日期，将执行全量下载 ({total_page_count} 页)。")

    # 3. 抓取后续页面 (从第 2 页开始)
    if total_page_count > 1:
        for page_index in range(2, total_page_count + 1):
            await asyncio.sleep(REQUEST_DELAY) 

            url = BASE_URL_NET_VALUE.format(fund_code=fund_code, page_index=page_index)
            
            try:
                async with session.get(url, timeout=REQUEST_TIMEOUT, headers=HEADERS) as response:
                    response.raise_for_status()
                    html_text = await response.text()
                    _, new_records = parse_fund_data_from_html(html_text)
            except Exception as e:
                logger.error(f"基金 {fund_code}: 抓取第 {page_index} 页失败: {e}")
                continue 
            
            should_stop = False
            
            if latest_local_date:
                # 遍历当前页的数据，查找停止点
                for record in new_records:
                    current_date = to_date_object(record.get('date'))
                    
                    if current_date and current_date <= latest_local_date:
                        should_stop = True
                        logger.info(f"基金 {fund_code}: 抓取到本地最新日期 {latest_local_date}，停止抓取，总共抓取了 {page_index} 页。")
                        
                        # 仅保留比本地最新日期更新的数据
                        newly_available_records = [
                            r for r in new_records 
                            if to_date_object(r.get('date')) and to_date_object(r.get('date')) > latest_local_date
                        ]
                        all_records.extend(newly_available_records)
                        break
                
                if not should_stop:
                    # 如果本页所有数据都比本地新，则全部加入
                    all_records.extend(new_records)

            else:
                # 全量下载模式，直接添加所有记录
                all_records.extend(new_records)
            
            if should_stop:
                break
                
    return all_records

# =========================================================================
#                       数据保存逻辑 (已修改)
# =========================================================================

def save_to_csv(fund_code, new_records, latest_local_date=None):
    """
    将数据保存到CSV文件，处理增量合并或全量写入。
    
    :param latest_local_date: 本地CSV文件的最新日期 (datetime.date 对象)。
    """
    
    if not new_records:
        return 0 # 没有新数据，无需保存
    
    output_file = os.path.join(OUTPUT_DIR, f"{fund_code}.csv")
    new_df = pd.DataFrame(new_records)
    
    # 清理和转换数据类型
    new_df['date'] = pd.to_datetime(new_df['date'], errors='coerce')
    new_df = new_df.dropna(subset=['date']) 

    # 如果是增量更新
    if latest_local_date and os.path.exists(output_file):
        try:
            old_df = pd.read_csv(output_file, parse_dates=['date'])
            old_df = old_df.dropna(subset=['date']) 
            
            # 过滤新数据中已存在的记录
            new_df_filtered = new_df[new_df['date'].dt.date > latest_local_date]
            
            # 1. 合并新旧数据
            combined_df = pd.concat([new_df_filtered, old_df])
            
            # 2. 去重：以 'date' 为主键，保留新的记录 (keep='first')
            combined_df = combined_df.drop_duplicates(subset=['date'], keep='first')
            
            # 3. 排序：按日期降序排列
            combined_df = combined_df.sort_values(by='date', ascending=False)
            
            # 覆盖写入文件
            combined_df.to_csv(output_file, index=False)
            
            added_count = len(new_df_filtered)
            logger.info(f"基金 {fund_code}: 增量更新完成，新增 {added_count} 条记录。")
            return added_count
            
        except Exception as e:
            logger.error(f"基金 {fund_code} 增量合并失败: {e}，将尝试全量覆盖。")
            # 回退到全量覆盖逻辑
            
    # 全量覆盖逻辑 (或首次创建文件，或增量合并失败的回退)
    final_df = new_df.sort_values(by='date', ascending=False)
    final_df.to_csv(output_file, index=False)
    logger.info(f"基金 {fund_code}: 全量写入完成，共 {len(final_df)} 条记录。")
    return len(final_df)


# =========================================================================
#                          主循环函数 (已修改)
# =========================================================================

def get_all_fund_codes(input_file):
    """从文件中读取所有基金代码，如果文件不存在则返回空列表"""
    if not os.path.exists(input_file):
        logger.error(f"输入文件 {input_file} 不存在。")
        return []
    try:
        df = pd.read_csv(input_file, encoding='utf-8')
        # 假设文件只有一列 'code'，并确保其格式为 6 位字符串
        codes = df['code'].astype(str).str.zfill(6).tolist()
        return [c for c in codes if len(c) == 6]
    except Exception as e:
        logger.error(f"读取基金代码文件失败: {e}")
        return []


async def fetch_all_funds(fund_codes):
    """主抓取协程函数，处理所有基金代码的并发抓取和保存。"""
    
    success_count = 0
    total_new_records = 0
    failed_codes = []
    
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_TASKS)
    
    async with ClientSession(connector=connector) as session:
        tasks = []
        for fund_code in fund_codes:
            # 1. 【新增】获取本地最新日期
            latest_local_date = get_latest_local_date(fund_code)
            
            # 2. 创建异步抓取任务，将本地日期传递进去
            task = asyncio.create_task(
                fetch_fund_data(fund_code, session, latest_local_date)
            )
            tasks.append((fund_code, task, latest_local_date))

        # 3. 等待所有任务完成并处理结果
        for fund_code, task, latest_local_date in tasks:
            try:
                new_records = await task
                
                # 4. 【修改】保存数据，将本地日期传递进去进行合并/覆盖
                if new_records:
                    added_count = save_to_csv(fund_code, new_records, latest_local_date)
                    success_count += 1
                    total_new_records += added_count
                else:
                    if latest_local_date:
                        logger.info(f"基金 {fund_code}: 数据已是最新，无需保存。")
                        success_count += 1
                    else:
                        logger.warning(f"基金 {fund_code}: 未抓取到任何数据，可能基金刚成立或 API 异常。")
                        failed_codes.append(fund_code)
                        
            except Exception as e:
                logger.error(f"基金 {fund_code} 处理失败: {e}")
                failed_codes.append(fund_code)
                
    return success_count, total_new_records, failed_codes

def main():
    """主执行函数"""
    print(f"\n======== 基金净值爬虫 (增量更新版) ========")
    start_time = time.time()
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
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
        # 兼容不同系统的 EventLoop 设置
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
    print(f"成功处理基金数: {success_count}/{len(processed_codes)}")
    print(f"新增记录总数: {total_new_records}")
    if failed_codes:
        print(f"失败基金代码: {', '.join(failed_codes)}")
    
if __name__ == '__main__':
    main()
