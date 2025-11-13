# stock_analysis_local_data_V1_FINAL.py - 从本地文件读取数据进行分析

import pandas as pd
import pandas_ta as ta
from datetime import datetime, timedelta
import pytz
from concurrent.futures import ThreadPoolExecutor
import time
import akshare as ak # 仅保留导入，但不再使用其数据获取功能
import logging
from pathlib import Path
from tqdm import tqdm
import warnings
import os # 新增导入 os

warnings.filterwarnings("ignore")
warnings.simplefilter(action='ignore', category=FutureWarning)


# --- AkShare 全局配置 (保留但不执行数据获取) ---
# try:
#     ak.set_time_out(30) # 移除 AkShare 配置，因为不再依赖网络获取
# except Exception as e:
#     print(f"警告：设置 AkShare 全局超时失败：{e}")


# --- 常量和配置 ---
shanghai_tz = pytz.timezone('Asia/Shanghai')
# 修改为本地数据文件目录和分析结果输出目录
INPUT_DIR = "stock_data" # 假设原始数据文件放在此目录
OUTPUT_DIR = "analyzed_data" # 分析结果输出到此目录

DEFAULT_START_DATE = '1990-01-01' # 扩大起始日期以确保分析历史完整性
INDICATOR_LOOKBACK_DAYS = 30
LOCK_FILE = "stock_analysis.lock"

MAX_WORKERS = 1
MAX_RETRIES = 0 # 不再需要重试

# --- 指数列表及代码结构 (保持不变) ---
INDEX_LIST = {
    '000001': {'name': '上证指数', 'market': 1},
    '399001': {'name': '深证成指', 'market': 0},
    '399006': {'name': '创业板指', 'market': 0},
    '000016': {'name': '上证50', 'market': 1},
    '000300': {'name': '沪深300', 'market': 1},
    '000905': {'name': '中证500', 'market': 1},
    '000852': {'name': '中证1000', 'market': 1},
    '000688': {'name': '科创50', 'market': 1},
    '399300': {'name': '沪深300(深)', 'market': 0},
    '000991': {'name': '中证全指', 'market': 1},
    '000906': {'name': '中证800', 'market': 1},
    '399005': {'name': '中小板指', 'market': 0},
    '399330': {'name': '深证100', 'market': 0},
    '000010': {'name': '上证180', 'market': 1},
    '000015': {'name': '红利指数', 'market': 1},
    '000011': {'name': '上证基金指数', 'market': 1},
    '399305': {'name': '深证基金指数', 'market': 0},
    '399306': {'name': '深证ETF指数', 'market': 0},
}
SW_INDUSTRY_DICT = {'801010':'农林牧渔','801020':'采掘','801030':'化工','801040':'钢铁','801050':'有色金属','801080':'电子','801110':'家用电器','801120':'食品饮料','801130':'纺织服装','801140':'轻工制造','801150':'医药生物','801160':'公用事业','801170':'交通运输','801180':'房地产','801200':'商业贸易','801210':'休闲服务','801230':'综合','801710':'建筑材料','801720':'建筑装饰','801730':'电气设备','801740':'国防军工','801750':'计算机','801760':'传媒','801770':'通信','801780':'银行','801790':'非银金融','801880':'汽车','801890':'机械设备','801060':'建筑建材','801070':'机械设备','801090':'交运设备','801190':'金融服务','801100':'信息设备','801220':'信息服务'}
CS_INDUSTRY_DICT = {}
WIND_INDUSTRY_DICT = {}

def get_pytdx_market(code):
    code = str(code)
    if code.startswith('00') or code.startswith('88') or code.startswith('801') or code.startswith('CI005'):
        return 1
    elif code.startswith('399'):
        return 0
    return 1

def merge_industry_indexes(index_list, industry_dict, prefix=""):
    for code, name in industry_dict.items():
        pytdx_code = code.split('.')[0]
        if pytdx_code not in index_list:
            index_list[pytdx_code] = {
                'name': f'{prefix}{name}',
                'market': get_pytdx_market(pytdx_code)
            }
    return index_list

INDEX_LIST = merge_industry_indexes(INDEX_LIST, SW_INDUSTRY_DICT, prefix="申万一级_")
INDEX_LIST = merge_industry_indexes(INDEX_LIST, CS_INDUSTRY_DICT, prefix="中信一级_")
INDEX_LIST = merge_industry_indexes(INDEX_LIST, WIND_INDUSTRY_DICT, prefix="万得一级_")

# --- 日志系统 (保持不变) ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler("stock_analysis_local.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# --- 指标计算函数 (保持不变) ---
def calculate_full_technical_indicators(df):
    if df.empty: return df
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    price_cols = ['open', 'close', 'high', 'low', 'volume']
    for col in price_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 检查是否有足够的行进行指标计算
    if len(df) < max(20, 14, 9): # 简单检查所需的最小长度
        logger.warning(f"    - 数据量不足 ({len(df)} 行)，跳过技术指标计算。")
        return df.reset_index()

    df.ta.sma(length=5, append=True, col_names=('MA5',))
    df.ta.sma(length=20, append=True, col_names=('MA20',))
    df.ta.rsi(length=14, append=True, col_names=('RSI14',))
    df.ta.stoch(k=9, d=3, smooth_k=3, append=True); df = df.rename(columns={'STOCHk_9_3_3': 'K', 'STOCHd_9_3_3': 'D', 'STOCHj_9_3_3': 'J'})
    df.ta.macd(append=True); df = df.rename(columns={'MACD_12_26_9': 'MACD', 'MACDh_12_26_9': 'MACDh', 'MACDs_12_26_9': 'MACDs'})
    df.ta.bbands(length=20, std=2, append=True); df = df.rename(columns={'BBL_20_2.0': 'BB_lower', 'BBM_20_2.0': 'BB_middle', 'BBU_20_2.0': 'BB_upper', 'BBB_20_2.0': 'BB_bandwidth', 'BBP_20_2.0': 'BB_percent'})
    df.ta.atr(length=14, append=True); df = df.rename(columns={'ATRr_14': 'ATR14'})
    df.ta.cci(length=20, append=True); df = df.rename(columns={'CCI_20_0.015': 'CCI20'})
    df.ta.obv(append=True)
    return df.reset_index()

def aggregate_and_analyze(df_raw_slice, freq, prefix):
    if df_raw_slice.empty: return pd.DataFrame()
    # 假设本地数据中没有换手率，或者不用于周/月线合成，这里保持原样
    if 'turnover_rate' not in df_raw_slice.columns:
        df_raw_slice['turnover_rate'] = float('nan') 
        
    df_raw_slice.index = pd.to_datetime(df_raw_slice.index)
    agg_df = df_raw_slice.resample(freq).agg({
        'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last',
        'volume': 'sum', 'turnover_rate': 'mean'
    }).dropna(subset=['close'])
    if not agg_df.empty:
        agg_df = agg_df.reset_index().rename(columns={'index': 'date'})
        agg_df['date'] = agg_df['date'].dt.date
        agg_df = calculate_full_technical_indicators(agg_df)
        cols_to_keep = agg_df.columns.drop(['date', 'open', 'close', 'high', 'low', 'volume', 'turnover_rate'])
        agg_df = agg_df.rename(columns={col: f'{col}_{prefix}' for col in cols_to_keep})
        agg_df.set_index('date', inplace=True)
    return agg_df

# --- 新增：读取本地历史数据文件 ---
def load_local_history_data(code):
    """从本地 CSV 文件读取指数历史数据"""
    file_path = Path(INPUT_DIR) / f"{code.replace('.', '_')}.csv"
    logger.info(f"    - 尝试从本地文件读取 {code}：{file_path.name}")
    
    if not file_path.exists():
        logger.error(f"    - 错误：本地文件 {file_path} 不存在。")
        return pd.DataFrame()
        
    try:
        # 尝试读取本地 CSV
        # 假设本地 CSV 文件的列名包含: 日期, 股票代码, 开盘, 收盘, 最高, 最低, 成交量, 成交额, 振幅, 涨跌幅, 涨跌额, 换手率
        # 需要进行列名和日期格式的清洗，使其与 AkShare 接口的输出兼容
        df = pd.read_csv(
            file_path, 
            dtype={'股票代码': str},
            parse_dates=['日期']
        )
        
        # 清洗列名
        df.rename(columns={
            '日期': 'date', '开盘': 'open', '收盘': 'close',
            '最高': 'high', '最低': 'low', 
            '成交量': 'volume', '成交额': 'amount', 
            '换手率': 'turnover_rate' # 增加换手率支持
        }, inplace=True)
        
        # 仅保留必需的列
        required_cols = ['date', 'open', 'close', 'high', 'low', 'volume', 'turnover_rate']
        df = df[[c for c in required_cols if c in df.columns]].copy()
        
        # 数据类型转换和清洗
        df['date'] = pd.to_datetime(df['date']).dt.date # 转换为 date 对象
        df.set_index('date', inplace=True)
        for col in ['open', 'close', 'high', 'low', 'volume']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
        df.dropna(subset=['close'], inplace=True)
        df.sort_index(inplace=True)
        
        logger.info(f"    - ✅ {code} 本地文件读取成功。总行数: {len(df)}")
        return df
        
    except Exception as e:
        logger.error(f"    - 错误：读取本地文件 {file_path.name} 失败。错误: {e}")
        return pd.DataFrame()


# --- 增量数据获取与分析核心函数 (修改为从本地文件获取) ---
def get_and_analyze_data_slice(code, start_date_to_process):
    try:
        # 1. 从本地文件加载所有历史数据
        df_full = load_local_history_data(code)

        if df_full.empty:
            logger.warning(f"    - {code} 未获取到有效数据。")
            return None

        # 2. 从指定日期（包含回溯期）开始进行分析
        start_date = pd.to_datetime(start_date_to_process).date()
        df_raw = df_full[df_full.index >= start_date].copy()

        if df_raw.empty:
             logger.warning(f"    - {code} 在指定起始日期 {start_date} 之后无新数据，无需重新分析。")
             return None
             
        # 3. 日线指标计算
        # 准备日线计算所需格式
        df_raw_processed = df_raw.reset_index().rename(columns={'index': 'date'})
        df_raw_processed['date'] = pd.to_datetime(df_raw_processed['date'])
        
        # 计算日线指标
        df_daily = calculate_full_technical_indicators(df_raw_processed.copy())
        
        # 准备周/月/年线计算所需格式
        df_raw.index = pd.to_datetime(df_raw.index)
        
        # 重命名日线列
        daily_cols = df_daily.columns.drop(['date', 'open', 'close', 'high', 'low', 'volume', 'turnover_rate'])
        df_daily = df_daily.rename(columns={col: f'{col}_D' for col in daily_cols})
        df_daily.set_index('date', inplace=True)
        
        # 4. 周/月/年线聚合和指标计算
        df_weekly = aggregate_and_analyze(df_raw.copy(), 'W', 'W')
        df_monthly = aggregate_and_analyze(df_raw.copy(), 'M', 'M')
        # df_yearly = aggregate_and_analyze(df_raw.copy(), 'Y', 'Y') # 年线数据量太少，暂时跳过或保留
        df_yearly = aggregate_and_analyze(df_raw.copy(), 'Y', 'Y') 

        # 5. 合并结果
        results = df_daily.copy()
        results = results.join(df_weekly, how='left').join(df_monthly, how='left').join(df_yearly, how='left')
        results.index.name = 'date'
        
        logger.info(f"    - {code} 成功分析 {len(results)} 行数据切片 (从 {start_date} 开始)。")
        return results.sort_index()
        
    except Exception as e:
        logger.error(f"    - 错误：处理指数 {code} 失败。最终错误: {e}")
        return None

# --- 主处理函数 (修改了目录名) ---
def process_single_index(code_map):
    code = code_map['code']
    name = code_map['name']
    logger.info(f"-> 正在处理指数: {code} ({name})")
    
    # 定义输出文件路径
    file_name = f"{code.replace('.', '_')}.csv"
    output_path = Path(OUTPUT_DIR) / file_name
    
    # 确定要从哪个日期开始重新计算指标 (包含回溯期)
    start_date_to_process = DEFAULT_START_DATE
    df_old = pd.DataFrame()

    if output_path.exists():
        try:
            # 读取旧的分析结果
            df_old = pd.read_csv(output_path, index_col='date', parse_dates=True)
            if not df_old.empty:
                latest_date_in_repo = df_old.index.max()
                # 确定本次需要重新计算的开始日期 (包含指标回溯期)
                start_date_for_calc = latest_date_in_repo - timedelta(days=INDICATOR_LOOKBACK_DAYS)
                start_date_to_process = start_date_for_calc.strftime('%Y-%m-%d')
                
                # 确保不会比默认起始日期更早
                if start_date_for_calc.strftime('%Y-%m-%d') < DEFAULT_START_DATE:
                    start_date_to_process = DEFAULT_START_DATE
                
                logger.info(f"    - 检测到旧分析结果，最新日期为 {latest_date_in_repo.strftime('%Y-%m-%d')}。本次分析从 {start_date_to_process} 开始的切片（含重叠）。")
            else:
                logger.warning(f"    - 旧分析文件 {output_path.name} 为空，将重新全量分析。")
        except Exception as e:
            logger.error(f"    - 警告：读取旧分析文件 {output_path.name} 失败 ({e})，将重新全量分析。")
    else:
        logger.info(f"    - 分析文件不存在，将全量分析本地数据。")
        
    # 获取数据并分析 (从本地文件获取)
    df_new_analyzed = get_and_analyze_data_slice(code, start_date_to_process)
    
    # 异常或无新数据处理逻辑
    if df_new_analyzed is None:
        is_today_updated = False
        if not df_old.empty and pd.api.types.is_datetime64_any_dtype(df_old.index):
             today = datetime.now(shanghai_tz).date()
             is_today_updated = df_old.index.max().date() == today
        
        if is_today_updated:
            logger.info(f"    - {code} 数据已是今天最新，跳过保存。")
        elif not df_old.empty:
             logger.warning(f"    - {code} 本地文件未更新或分析失败，保持原分析文件。")
        else:
             logger.error(f"    - {code} 本地文件不存在或分析失败，无法生成文件。")
        return False
        
    # 合并新旧数据
    if not df_old.empty:
        # 确保索引为日期对象，方便比较
        df_old.index = pd.to_datetime(df_old.index)
        df_new_analyzed.index = pd.to_datetime(df_new_analyzed.index)

        # 保留比新分析结果起始日期更早的旧数据
        old_data_to_keep = df_old[df_old.index.date < df_new_analyzed.index.min().date()]
    else:
        old_data_to_keep = pd.DataFrame()
        
    df_combined = pd.concat([old_data_to_keep, df_new_analyzed])
    
    # 去重并排序
    results_to_save = df_combined[~df_combined.index.duplicated(keep='last')]
    results_to_save = results_to_save.sort_index()
    
    logger.info(f"    - ✅ {code} 成功更新。总行数: {len(results_to_save)}")
    
    # 保存结果
    results_to_save.to_csv(output_path, encoding='utf-8')
    return True

def main():
    start_time = time.time()
    
    # 确保本地数据输入目录和分析结果输出目录存在
    input_path = Path(INPUT_DIR)
    output_path = Path(OUTPUT_DIR)
    
    # 创建输入目录 (如果不存在，提醒用户将数据放入)
    if not input_path.exists():
         input_path.mkdir(exist_ok=True)
         logger.warning(f"注意：本地数据输入目录 {input_path.resolve()} 不存在，已创建。请将原始 CSV 文件放入此目录。")
         # 如果输入目录是空的，直接退出
         if not os.listdir(input_path):
             logger.error("数据目录为空，无法进行分析。请将原始数据文件放入 `stock_data` 目录。")
             return
             
    output_path.mkdir(exist_ok=True)
    
    # 锁文件逻辑保持不变
    lock_file_path = Path(LOCK_FILE)
    if lock_file_path.exists():
        logger.warning("检测到锁文件，脚本可能正在运行或上次异常退出。终止本次运行。")
        return
    lock_file_path.touch()
    
    logger.info("—" * 50)
    logger.info("🚀 脚本开始运行 (本地数据分析模式)")
    logger.info(f"分析结果将保存到专用目录: {output_path.resolve()}")
    
    try:
        logger.info(f"准备串行处理 {len(INDEX_LIST)} 个指数...")
        successful = 0
        failed = 0
        
        # 过滤掉本地不存在原始文件的指数，提高效率
        available_jobs = []
        for code, data in INDEX_LIST.items():
            file_name = f"{code.replace('.', '_')}.csv"
            file_path = input_path / file_name
            if file_path.exists():
                available_jobs.append({'code': code, **data})
            else:
                logger.warning(f"    - 跳过指数 {code} ({data['name']})：本地数据文件 {file_name} 不存在于 {INPUT_DIR} 目录。")
                
        jobs = available_jobs

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(process_single_index, job): job for job in jobs}
            for future in tqdm(futures, desc="处理指数", unit="个", ncols=100, leave=True):
                job = futures[future]
                try:
                    if future.result():
                        successful += 1
                    else:
                        failed += 1
                except Exception as e:
                    logger.error(f"处理 {job['code']} ({job['name']}) 时发生未捕获异常: {e}")
                    failed += 1
                    
        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.info("—" * 50)
        logger.info(f"✅ 所有指数数据处理完成。总耗时: {elapsed_time:.2f} 秒")
        logger.info(f"统计：成功更新 {successful} 个文件，失败/跳过 {failed + (len(INDEX_LIST) - len(jobs))} 个 (其中跳过因缺少本地文件：{len(INDEX_LIST) - len(jobs)})。")
        
    finally:
        lock_file_path.unlink(missing_ok=True)
        logger.info("锁文件已清除。")

if __name__ == "__main__":
    main()
