import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime
import pytz

# --- 趋势跟随策略 (Trend Follow) 配置 ---

# 数据路径配置
FUND_DATA_DIR = 'fund_data'
REPORT_BASE_NAME = 'trend_follow_report'

# 核心筛选参数 (直接编码在脚本中)
MIN_CONSECUTIVE_RISE_DAYS = 2    # 1. 连续上涨天数（筛选条件：近5个交易日内连续上涨天数 >= 2天）
RSI_MIN = 50                     # 2. RSI 下限（确认强势，RSI >= 50）
RSI_MAX = 70                     # 2. RSI 上限（避免超买，RSI < 70）
MACD_SIGNAL = '金叉'             # 3. 趋势确认（必须处于 MACD 金叉）
MAX_MONTH_DRAWDOWN = 0.03        # 4. 短期稳定性（1个月最大回撤 <= 3%）
MIN_DAILY_RISE_PERCENT = 0.005   # 5. 最小当日涨幅（当日涨幅 >= 0.5%）

# --- 辅助函数：计算技术指标 ---
def calculate_technical_indicators(df):
    """
    计算基金净值的RSI(14)、MACD、MA50。
    要求df必须按日期降序排列。
    """
    if 'value' not in df.columns or len(df) < 50:
        return {
            'RSI': np.nan, 'MACD信号': '数据不足', '净值/MA50': np.nan,
            '最新净值': df['value'].iloc[0] if not df.empty else np.nan,
            '当日涨跌幅': np.nan
        }
    
    df_asc = df.iloc[::-1].copy()
    
    # 1. RSI (14)
    delta = df_asc['value'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss.replace(0, np.nan) 
    df_asc['RSI'] = 100 - (100 / (1 + rs))
    rsi_latest = df_asc['RSI'].iloc[-1]
    
    # 2. MACD
    ema_12 = df_asc['value'].ewm(span=12, adjust=False).mean()
    ema_26 = df_asc['value'].ewm(span=26, adjust=False).mean()
    df_asc['MACD'] = ema_12 - ema_26
    df_asc['Signal'] = df_asc['MACD'].ewm(span=9, adjust=False).mean()
    macd_latest = df_asc['MACD'].iloc[-1]
    signal_latest = df_asc['Signal'].iloc[-1]
    macd_prev = df_asc['MACD'].iloc[-2] if len(df_asc) >= 2 else np.nan
    signal_prev = df_asc['Signal'].iloc[-2] if len(df_asc) >= 2 else np.nan

    macd_signal = '观察'
    if not np.isnan(macd_prev) and not np.isnan(signal_prev):
        if macd_latest > signal_latest and macd_prev < signal_prev:
            macd_signal = '金叉'
        elif macd_latest < signal_latest and macd_prev > signal_prev:
            macd_signal = '死叉'

    # 3. MA50
    df_asc['MA50'] = df_asc['value'].rolling(window=50).mean()
    ma50_latest = df_asc['MA50'].iloc[-1]
    value_latest = df_asc['value'].iloc[-1]
    net_to_ma50 = value_latest / ma50_latest if ma50_latest and ma50_latest != 0 else np.nan

    # 4. 计算当日涨跌幅 (T日 vs T-1日)
    daily_drop = 0.0
    if len(df_asc) >= 2:
        value_t_minus_1 = df_asc['value'].iloc[-2]
        if value_t_minus_1 > 0:
            daily_drop = (value_t_minus_1 - value_latest) / value_t_minus_1 # 负值代表上涨

    return {
        'RSI': round(rsi_latest, 2) if not np.isnan(rsi_latest) else np.nan,
        'MACD信号': macd_signal,
        '净值/MA50': round(net_to_ma50, 2) if not np.isnan(net_to_ma50) else np.nan,
        '最新净值': round(value_latest, 4) if not np.isnan(value_latest) else np.nan,
        '当日涨跌幅': round(-daily_drop, 4) # 负值表示当日下跌，正值表示当日上涨
    }

def calculate_max_drawdown(series):
    if series.empty:
        return 0.0
    rolling_max = series.cummax()
    drawdown = (rolling_max - series) / rolling_max
    mdd = drawdown.max()
    return mdd

def calculate_consecutive_rises(series):
    if series.empty or len(series) < 2:
        return 0
    # 净值上涨：当前值 > 前一个值
    rises = (series.iloc[1:].values > series.iloc[:-1].values)
    rises_int = rises.astype(int)
    max_rise_days = 0
    current_rise_days = 0
    for val in rises_int:
        if val == 1:
            current_rise_days += 1
        else:
            max_rise_days = max(max_rise_days, current_rise_days)
            current_rise_days = 0
    max_rise_days = max(max_rise_days, current_rise_days)
    return max_rise_days


# --- 核心分析函数 ---
def analyze_trend_funds():
    
    csv_files = glob.glob(os.path.join(FUND_DATA_DIR, '*.csv'))
    if not csv_files:
        print(f"警告：在目录 '{FUND_DATA_DIR}' 中未找到任何 CSV 文件，请检查路径和数据。")
        return []

    print(f"策略: 趋势跟随 | 找到 {len(csv_files)} 个基金数据文件，开始分析...")
    
    qualifying_funds = []
    
    for filepath in csv_files:
        try:
            fund_code = os.path.splitext(os.path.basename(filepath))[0]
            
            df = pd.read_csv(filepath)
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values(by='date', ascending=False).reset_index(drop=True)
            df = df.rename(columns={'net_value': 'value'})
            
            if len(df) < 50:
                continue
            
            df_recent_month = df.head(30)
            df_recent_week = df.head(5)
            
            mdd_recent_month = calculate_max_drawdown(df_recent_month['value'])
            max_rise_days_week = calculate_consecutive_rises(df_recent_week['value'])
            
            tech_indicators = calculate_technical_indicators(df)
            rsi_val = tech_indicators.get('RSI', np.nan)
            daily_change_val = tech_indicators.get('当日涨跌幅', 0.0) # 正值代表上涨
            macd_signal = tech_indicators['MACD信号']

            # --- TrendFollow 核心筛选条件 (寻找强势中继) ---
            is_qualified = (
                # 1. 连续上涨 >= N天 (动能)
                max_rise_days_week >= MIN_CONSECUTIVE_RISE_DAYS and 
                
                # 2. RSI 处于强势区间但未超买 (50 <= RSI < 70)
                RSI_MIN <= rsi_val < RSI_MAX and
                
                # 3. 趋势确认 (MACD 金叉)
                macd_signal == MACD_SIGNAL and
                
                # 4. 短期稳定性 (排除短期暴跌后急拉，寻找稳定上涨中继)
                mdd_recent_month <= MAX_MONTH_DRAWDOWN and
                
                # 5. 当日涨幅不低于阈值
                daily_change_val >= MIN_DAILY_RISE_PERCENT
            )
            
            action_prompt = '不适用'
            if is_qualified:
                action_prompt = '趋势跟随 (小仓位试探)'
                
                fund_data = {
                    '基金代码': fund_code,
                    '最大回撤': mdd_recent_month,
                    '当日涨跌幅': daily_change_val,
                    '连涨 (1W)': max_rise_days_week,
                    'RSI': rsi_val,
                    'MACD信号': macd_signal,
                    '净值/MA50': tech_indicators['净值/MA50'],
                    '行动提示': action_prompt
                }
                qualifying_funds.append(fund_data)

        except Exception as e:
            print(f"处理文件 {filepath} 时发生错误: {e}")
            continue

    return qualifying_funds


# --- 生成报告函数 (带失效提示) ---
def generate_report(results, timestamp_str):
    
    report = f"# 基金策略报告 - 趋势跟随模式 ({timestamp_str} UTC+8)\n\n"
    report += f"## 策略总结\n"
    
    if not results:
         report += (
             f"**📢 脚本失效提示:** 当前 **趋势跟随模式** 未发现任何基金。\n"
             f"**原因分析:** 市场可能已进入震荡或回调期，或者没有基金符合严格的【强势中继】条件。\n"
             f"**💡 策略提示:** 如果您发现 Deep Value (超跌) 策略也连续多日未选出票，且本策略也无票，则市场可能进入全面回调，建议**空仓等待**。\n\n"
             f"当前筛选条件:\n"
             f" - 连涨天数 $\ge$ {MIN_CONSECUTIVE_RISE_DAYS}天\n"
             f" - RSI 处于 [{RSI_MIN}, {RSI_MAX}) 区间\n"
             f" - MACD信号为 '{MACD_SIGNAL}'\n"
             f" - 1个月回撤 $\le$ {MAX_MONTH_DRAWDOWN*100:.0f}%\n"
         )
         return report
    
    
    df_results = pd.DataFrame(results)
    df_results = df_results.sort_values(by='当日涨跌幅', ascending=False).reset_index(drop=True)
    df_results.index = df_results.index + 1
    
    report += f"**✅ 发现 {len(df_results)} 只符合【强势中继】条件的基金，可考虑小仓位试探。**\n\n"
    report += f"当前筛选条件:\n"
    report += f" - 连涨天数 $\ge$ {MIN_CONSECUTIVE_RISE_DAYS}天\n"
    report += f" - RSI 处于 [{RSI_MIN}, {RSI_MAX}) 区间\n"
    report += f" - MACD信号为 '{MACD_SIGNAL}'\n"
    report += f" - 1个月回撤 $\le$ {MAX_MONTH_DRAWDOWN*100:.0f}%\n\n"
    
    
    report += f"## **Trend Follow 趋势中继列表**\n\n"
    report += f"| 排名 | 基金代码 | 最大回撤 (1M) | **当日涨跌幅** | 连涨 (1W) | RSI(14) | MACD信号 | 净值/MA50 | 行动提示 |\n"
    report += f"| :---: | :---: | ---: | ---: | ---: | ---: | ---: | ---: | :---: |\n"  

    for index, row in df_results.iterrows():
        report += f"| {index} | `{row['基金代码']}` | **{row['最大回撤']:.2%}** | **{row['当日涨跌幅']:.2%}** | {row['连涨 (1W)']} | **{row['RSI']:.2f}** | **{row['MACD信号']}** | {row['净值/MA50']:.2f} | **{row['行动提示']}** |\n"

    report += "\n---\n"
    report += f"**策略纪律:**\n"
    report += f"1. **小仓位试探:** 本模式下的建仓仓位应低于 Deep Value (超跌反弹) 模式。\n"
    report += f"2. **快速止盈/止损:** 趋势跟随风险相对较高，收益率达到 **3-5%** 应考虑止盈；一旦 **MACD 出现死叉**，应**立即清仓**。\n"
    
    return report

# --- 主执行块 (main) ---
if __name__ == '__main__':
    
    try:
        # 统一使用 Asia/Shanghai 时区
        tz = pytz.timezone('Asia/Shanghai')
        now = datetime.now(tz)
        
        timestamp_for_report = now.strftime('%Y-%m-%d %H:%M:%S')
        timestamp_for_filename = now.strftime('%Y%m%d_%H%M%S')
        DIR_NAME = now.strftime('%Y%m')
        
    except Exception:
        # 回退到本地时间
        now_fallback = datetime.now()
        timestamp_for_report = now_fallback.strftime('%Y-%m-%d %H:%M:%S')
        timestamp_for_filename = now_fallback.strftime('%Y%m%d_%H%M%S')
        DIR_NAME = now_fallback.strftime('%Y%m')
        
    os.makedirs(DIR_NAME, exist_ok=True)
    REPORT_FILE = os.path.join(DIR_NAME, f"{REPORT_BASE_NAME}_{timestamp_for_filename}.md")

    # 4. 执行分析
    results = analyze_trend_funds()
    
    # 5. 生成 Markdown 报告
    report_content = generate_report(results, timestamp_for_report)
    
    # 6. 写入报告文件
    with open(REPORT_FILE, 'w', encoding='utf-8') as f:
        f.write(report_content)
    
    print(f"分析完成，报告已保存到 {REPORT_FILE}")
