# index_analysis.py - 独立跟踪标的量化分析脚本
import akshare as ak
import pandas as pd
import numpy as np
import talib
import re
import time
import random
# 导入 requests 异常
from requests.exceptions import ConnectionError, Timeout
# 导入底层 http 客户端异常，解决 RemoteDisconnected 错误
import http.client

# --- 配置 ---
# 补充后的指数名称到 AkShare 代码的映射
INDEX_MAP = {
    '沪深300指数': '000300',
    '中证500指数': '000905',
    '中证800指数': '000906',
    '创业板指数': '399006',
    '上证指数': '000001',
    '恒生指数': 'HSI',
    '科创板50成份指数': '000688',
    '中证智能汽车主题指数': '399976',
    '中证电子指数': '000807',
    '中证军工指数': '399967',
    '中证新能源汽车指数': '399808',
    '中证医药卫生指数': '000933',
    '中证光伏产业指数': '399989',  # 已验证并修正：原000807是中证电子，光伏产业指数应为399989
    '中证人工智能主题指数': '000885',  # 已验证并修正：原000688是科创50，人工智能主题指数应为000885
    '中证传媒指数': '399971',
    '中证计算机主题指数': '399673',
    '创业板50指数': '399673',
    '深圳科技创新主题指数': '399668',
    
    # --- 补充新增跳过的指数 ---
    '中证1000指数': '000852',  # 补充
    '中证科创创业50指数': '931448',  # 补充
    '上证科创板50成份指数': '000688',  # 别名，确保匹配
    '中证全指信息技术指数': '000993',  # 补充
    '中证500信息技术指数': '000993',  # 补充
    '中证全指半导体产品与设备指数': 'H30184',  # 补充 (指数代码可能需要验证)
    '中证科技100指数': '931201',  # 补充
    '中证5G通信主题指数': '931079',  # 补充
    '中证芯片产业指数': '931071',  # 补充
    '中证云计算与大数据主题指数': '000992',  # 补充
    '国证半导体芯片指数': '980017',  # 补充
    '中证海外中国互联网50人民币指数': 'H30566',  # 补充
    '中证消费电子主题指数': '931098' # 补充
}

# MACD 参数
SHORT_PERIOD = 12
LONG_PERIOD = 26
SIGNAL_PERIOD = 9

# 最大重试次数和超时设置
MAX_RETRIES = 5  # 增加到5次，提高成功率
REQUEST_TIMEOUT = 30  # 秒，akshare内部请求超时

# --- 配置结束 ---

def fetch_index_data(index_code, start_date):
    """
    使用 AkShare 获取指数的日K线收盘价数据，并加入重试机制。
    """
    for attempt in range(MAX_RETRIES):
        try:
            if index_code == 'HSI':
                df = ak.index_global_hist(symbol="恒生指数", period="daily", start_date=start_date)
            else:
                # A股指数
                df = ak.index_zh_a_hist(symbol=index_code, period="daily", start_date=start_date)
            
            # 成功获取数据，跳出循环
            if not df.empty:
                df.rename(columns={'日期': 'date', '收盘': 'close'}, inplace=True)
                return df[['date', 'close']].set_index('date')
            else:
                raise ValueError("获取的数据为空")
        
        # 捕获网络连接中断和超时，以及数据为空的 ValueError
        except (ConnectionError, Timeout, http.client.RemoteDisconnected, ValueError) as e:
            # ConnectionError 捕获 requests 级别的连接错误
            # RemoteDisconnected 捕获底层 socket/http 级别的连接错误
            print(f" 警告: 尝试 {attempt + 1}/{MAX_RETRIES} - 无法获取 {index_code} 数据: {e.__class__.__name__} - {e}")
            if attempt < MAX_RETRIES - 1:
                # 随机延迟，防止被数据源封禁
                sleep_time = random.uniform(5, 10)  # 增加延迟范围
                print(f" 等待 {sleep_time:.2f} 秒后重试...")
                time.sleep(sleep_time)
            else:
                print(f" 错误: 达到最大重试次数，放弃获取 {index_code} 数据。")
                return pd.DataFrame()
        
        except Exception as e:
            print(f" 错误: 发生未知错误，无法获取 {index_code} 数据: {e.__class__.__name__} - {e}")
            return pd.DataFrame()
    
    return pd.DataFrame()

def analyze_and_suggest(df_data, index_name, fund_name):
    """
    对单一指数应用 MACD 指标，并输出买卖信号。
    """
    if len(df_data) < LONG_PERIOD * 2:
        return f" [ {index_name} ] 数据不足（{len(df_data)}条），跳过技术分析。"
    
    # 计算 MACD 指标
    df_nav = df_data.copy()
    # 确保输入是 float 类型，以避免 talib 警告
    close_prices = df_nav['close'].values.astype(float) 
    
    df_nav['MACD'], df_nav['MACD_Signal'], df_nav['MACD_Hist'] = \
        talib.MACD(close_prices,
                   fastperiod=SHORT_PERIOD,
                   slowperiod=LONG_PERIOD,
                   signalperiod=SIGNAL_PERIOD)
    
    df_nav['Signal'] = np.where(df_nav['MACD'] > df_nav['MACD_Signal'], 1, 0)
    df_nav['Position'] = df_nav['Signal'].diff()
    
    # 提取最近的交易信号
    recent_signals = df_nav[df_nav['Position'].abs() == 1].tail(3)
    
    report_output = [f"\n--- 📈 {index_name} ({fund_name} 的跟踪标的) 最新信号 ---"]
    
    if recent_signals.empty:
        report_output.append(" 未检测到有效信号。")
    else:
        for index, row in recent_signals.iterrows():
            action = "买入/加仓" if row['Position'] == 1 else "卖出/减仓"
            report_output.append(f" 日期: {index}, 信号: {action}, 指数收盘价: {row['close']:.2f}")

    current_position = "多头 (建议持有或加仓)" if df_nav['Signal'].iloc[-1] == 1 else "空头 (建议观望或减仓)"
    report_output.append(f" 当前状态 ({df_nav.index[-1]}): {current_position}")
    
    return "\n".join(report_output)

def main_analysis():
    # 1. 读取 fund_basic_data_c_class.csv
    try:
        # 使用 utf-8-sig 应对可能存在的 BOM
        df_funds = pd.read_csv('fund_basic_data_c_class.csv', encoding='utf_8_sig')
    except FileNotFoundError:
        return "错误：未找到 fund_basic_data_c_class.csv 文件。请确保您的数据抓取工作流已运行。"
    except Exception as e:
        return f"读取 CSV 文件出错: {e}"
    
    # 设置分析数据的起始日期为一年前
    start_date = (pd.Timestamp.today() - pd.DateOffset(years=1)).strftime('%Y%m%d')
    full_report = [f"【基金跟踪标的量化分析报告】\n生成时间：{pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')} (UTC)\n--------------------------------------------------"]
    
    # 2. 遍历每只基金进行分析
    for index, row in df_funds.iterrows():
        fund_code = row['基金代码']
        fund_name = row['基金简称']
        tracking_index_str = row['跟踪标的']
        
        # 3. 明确跳过 '该基金无跟踪标的' 或为空的记录
        if pd.isna(tracking_index_str) or tracking_index_str.strip() == '该基金无跟踪标的' or not tracking_index_str.strip():
            continue
        
        header = f"\n==================================================\n🔬 正在分析指数基金: {fund_name} ({fund_code})\n 跟踪标的: {tracking_index_str}\n=================================================="
        full_report.append(header)
        
        # 4. 尝试从跟踪标的字符串中匹配指数名称 (优化：忽略大小写、括号、特殊字符)
        matched_index_name = None
        # 移除括号、空格、连字符并转小写
        cleaned_tracking_str = re.sub(r'[\(\（\)\）\s-]', '', tracking_index_str).strip().lower()  
        for name in INDEX_MAP.keys():
            cleaned_name = re.sub(r'[\(\（\)\）\s-]', '', name).strip().lower()
            if cleaned_name in cleaned_tracking_str or cleaned_tracking_str in cleaned_name:
                matched_index_name = name
                break
        
        if not matched_index_name:
            full_report.append(f" **跳过:** 跟踪标的 '{tracking_index_str}' 未在映射表中或无法匹配。")
            continue
        
        index_code = INDEX_MAP[matched_index_name]
        full_report.append(f"\n-> 开始分析跟踪标的: {matched_index_name} (代码: {index_code})")
        
        # 5. 抓取数据并分析 (包含重试逻辑)
        df_data = fetch_index_data(index_code, start_date)
        
        if not df_data.empty:
            analysis_result = analyze_and_suggest(df_data, matched_index_name, fund_name)
            full_report.append(analysis_result)
        else:
            full_report.append(f" **错误:** 无法获取 {matched_index_name} 的历史数据，请检查网络或指数代码。")
        
        full_report.append("--------------------------------------------------")
    
    return "\n".join(full_report)

if __name__ == '__main__':
    # 必要的库检查
    try:
        import akshare
        import talib
        import pandas as pd
        import requests
        import http.client
    except ImportError as e:
        print(f"致命错误：请确保已安装 akshare, talib, pandas, requests 库。缺少: {e}")
        exit(1)
    
    report_content = main_analysis()
    
    # 直接将报告内容输出到标准输出，工作流会将其重定向到文件
    print(report_content)
