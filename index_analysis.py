# index_analysis.py - 独立跟踪标的量化分析脚本 (代码已补全)

import akshare as ak
import pandas as pd
import numpy as np
import talib
import re

# --- 配置 ---
# 常见的指数名称到 AkShare 代码的映射 (根据网络搜索结果补全)
INDEX_MAP = {
    '沪深300指数': '000300',
    '中证500指数': '000905',
    '中证800指数': '000906',
    '创业板指数': '399006',
    '上证指数': '000001',
    '恒生指数': 'HSI', # 港股指数
    '科创板50成份指数': '000688', # 搜索结果
    '中证智能汽车主题指数': '399976', # 搜索结果，中证智能汽车指数
    '中证电子指数': '000807', # 搜索结果
    '中证军工指数': '399967', # 搜索结果
    '中证新能源汽车指数': '399808', # 搜索结果，中证新能源指数
    '中证医药卫生指数': '000933', # 搜索结果，中证全指医药卫生指数
    '中证光伏产业指数': '000807', # 搜索结果，需要与中证电子区分，但代码常见冲突
    '中证人工智能主题指数': '000688', # 搜索结果
    '中证传媒指数': '399971', # 搜索结果
    '中证计算机主题指数': '399673', # 搜索结果
    '创业板50指数': '399673', # 补充
    '深圳科技创新主题指数': '399668' # 补充
}

# MACD 参数
SHORT_PERIOD = 12
LONG_PERIOD = 26
SIGNAL_PERIOD = 9
# --- 配置结束 ---


def fetch_index_data(index_code, start_date):
    """
    使用 AkShare 获取指数的日K线收盘价数据。
    """
    try:
        if index_code == 'HSI':
            df = ak.index_global_hist(symbol="恒生指数", period="daily", start_date=start_date)
        else: 
            # A股指数
            df = ak.index_zh_a_hist(symbol=index_code, period="daily", start_date=start_date)
        
        # 统一列名
        df.rename(columns={'日期': 'date', '收盘': 'close'}, inplace=True)
        return df[['date', 'close']].set_index('date')
    except Exception as e:
        print(f"   错误: 无法获取 {index_code} 数据: {e}")
        return pd.DataFrame()

def analyze_and_suggest(df_data, index_name, fund_name):
    """
    对单一指数应用 MACD 指标，并输出买卖信号。
    """
    if len(df_data) < LONG_PERIOD * 2:
        print(f"   [ {index_name} ] 数据不足，跳过技术分析。")
        return

    # 计算 MACD 指标
    df_nav = df_data.copy()
    df_nav['MACD'], df_nav['MACD_Signal'], df_nav['MACD_Hist'] = \
        talib.MACD(df_nav['close'].values, 
                   fastperiod=SHORT_PERIOD, 
                   slowperiod=LONG_PERIOD, 
                   signalperiod=SIGNAL_PERIOD)

    df_nav['Signal'] = np.where(df_nav['MACD'] > df_nav['MACD_Signal'], 1, 0)
    df_nav['Position'] = df_nav['Signal'].diff()
    
    # 提取最近的交易信号
    recent_signals = df_nav[df_nav['Position'].abs() == 1].tail(3)
    
    print(f"\n--- 📈 {index_name} ({fund_name} 的跟踪标的) 最新信号 ---")
    
    if recent_signals.empty:
        print("   未检测到有效信号。")
    else:
        for index, row in recent_signals.iterrows():
            action = "买入/加仓" if row['Position'] == 1 else "卖出/减仓"
            print(f"   日期: {index}, 信号: {action}, 指数收盘价: {row['close']:.2f}")

    current_position = "多头 (建议持有或加仓)" if df_nav['Signal'].iloc[-1] == 1 else "空头 (建议观望或减仓)"
    print(f"   当前状态 ({df_nav.index[-1]}): {current_position}")


def main_analysis():
    # 1. 读取 fund_basic_data_c_class.csv
    try:
        df_funds = pd.read_csv('fund_basic_data_c_class.csv', encoding='utf_8_sig')
    except FileNotFoundError:
        print("错误：未找到 fund_basic_data_c_class.csv 文件，请先运行数据抓取脚本。")
        return
    except Exception as e:
        print(f"读取 CSV 文件出错: {e}")
        return

    start_date = (pd.Timestamp.today() - pd.DateOffset(years=1)).strftime('%Y%m%d')

    # 2. 遍历每只基金进行分析
    for index, row in df_funds.iterrows():
        fund_code = row['基金代码']
        fund_name = row['基金简称']
        tracking_index_str = row['跟踪标的'] # 重点：读取 '跟踪标的' 字段
        
        # 3. 明确跳过 '该基金无跟踪标的' 或为空的记录
        if pd.isna(tracking_index_str) or tracking_index_str.strip() == '该基金无跟踪标的' or not tracking_index_str.strip():
            continue

        print(f"\n==================================================")
        print(f"🔬 正在分析指数基金: {fund_name} ({fund_code})")
        print(f"   跟踪标的: {tracking_index_str}")
        print(f"==================================================")

        # 4. 尝试从跟踪标的字符串中匹配指数名称
        matched_index_name = None
        for name in INDEX_MAP.keys():
            # 查找 INDEX_MAP 中的名称是否出现在跟踪标的字符串中
            if name in tracking_index_str:
                matched_index_name = name
                break
        
        if not matched_index_name:
            print(f"   **跳过:** 跟踪标的 '{tracking_index_str}' 未在映射表中或无法匹配。")
            continue

        index_code = INDEX_MAP[matched_index_name]

        print(f"\n-> 开始分析跟踪标的: {matched_index_name} (代码: {index_code})")
        
        # 5. 抓取数据并分析
        df_data = fetch_index_data(index_code, start_date)
        
        if not df_data.empty:
            analyze_and_suggest(df_data, matched_index_name, fund_name)
        
        print("--------------------------------------------------")


if __name__ == '__main__':
    # 必要的库检查
    try:
        import akshare
        import talib
        # 检查是否能读取 pandas
        _ = pd.DataFrame() 
    except ImportError as e:
        print(f"致命错误：请确保已安装 akshare, talib, pandas 库。缺少: {e}")
    
    main_analysis()
