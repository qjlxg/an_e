# trend_follower.py
# Trend Following System V2.1: Elastic Scoring (3 choose 2) + Simplified Archiving
import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime
import pytz

# --- Configuration ---
FUND_DATA_DIR = 'fund_data'
INDEX_DATA_DIR = 'index_data'
MIN_UP_DAYS = 2                 # Minimum consecutive days of rise for score +1
MAX_VALUATION_RATIO = 1.20      # Max Net Value / MA50 ratio for score +1 (relaxed)
REPORT_BASE_NAME = 'trend_follow_report' # Base name for the report file

# --- Market Trend Detection ---
def detect_market_trend():
    """Detects if the overall market (CSI 300) is in a general upward trend."""
    try:
        filepath = os.path.join(INDEX_DATA_DIR, '000300.csv')
        if not os.path.exists(filepath):
            return "非普涨"
            
        df = pd.read_csv(filepath)
        df = df.sort_values('date', ascending=False).head(6)
        
        if len(df) < 6:
            return "非普涨"
            
        # Check consecutive rise days over the last 5 days
        df = df.iloc[::-1].reset_index(drop=True)
        # Count days where value is higher than the previous day
        up_count = (df['value'].diff() > 0).iloc[-5:].sum()
        
        # 'General Rise' condition: at least 4 out of 5 recent days were up
        # Returns Chinese strings for report readability
        return "普涨" if up_count >= 4 else "非普涨"
    except Exception:
        # Handle file read or data error
        return "非普涨"

# --- Calculate Elastic Indicators ---
def calculate_flexible_indicators(df):
    """Calculates consecutive rise days, valuation ratio (Net/MA50), and flexible MACD signal."""
    if 'value' not in df.columns or len(df) < 50:
        return None
        
    # Ensure data is sorted ascending for time series calculations
    df_asc = df.iloc[::-1].copy().reset_index(drop=True)
    
    # 1. Consecutive Rise Days (within last 5 days)
    up_days = 0
    for i in range(1, min(6, len(df_asc))):
        # Compare current day's net value with the previous day's
        if df_asc['value'].iloc[-i] > df_asc['value'].iloc[-i-1]:
            up_days += 1
        else:
            break
    
    # 2. Valuation Ratio (Latest Net / MA50)
    ma50 = df_asc['value'].rolling(50, min_periods=1).mean().iloc[-1]
    latest = df_asc['value'].iloc[-1]
    valuation_ratio = latest / ma50 if ma50 > 0 else 999
    
    # 3. MACD Flexible Signal
    # Note: Using adjust=False for standard Technical Analysis EMA calculation
    ema12 = df_asc['value'].ewm(span=12, adjust=False).mean()
    ema26 = df_asc['value'].ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    
    macd_now, macd_prev = macd.iloc[-1], macd.iloc[-2]
    signal_now, signal_prev = signal.iloc[-1], signal.iloc[-2]
    
    macd_signal = '观察'
    if macd_now > signal_now and macd_prev <= signal_prev:
        macd_signal = '金叉' # Confirmed Golden Cross
    # Relaxed condition: MACD is currently rising (positive momentum)
    elif macd_now > macd_prev:
        macd_signal = '即将金叉' 
    # Else, it remains '观察' or a '死叉' scenario (which falls into '观察' for this trend-following logic)

    return {
        'up_days': up_days,
        'valuation_ratio': round(valuation_ratio, 3),
        'macd_signal': macd_signal,
        'latest_net': round(latest, 4)
    }

# --- Trend Score Calculation ---
def calculate_trend_score(ind):
    """Calculates a score out of 3 based on the three indicators (Elastic 3 choose 2 logic)."""
    score = 0
    # Score Item 1: Momentum
    if ind['up_days'] >= MIN_UP_DAYS:
        score += 1
    # Score Item 2: Valuation Safety (Not Overvalued yet)
    if ind['valuation_ratio'] <= MAX_VALUATION_RATIO:
        score += 1
    # Score Item 3: Trend Confirmation/Momentum
    if ind['macd_signal'] in ['金叉', '即将金叉']:
        score += 1
    return score

# --- Main Program ---
def run_trend_follow():
    
    market = detect_market_trend()
    
    # Time and Path Setup (Asia/Shanghai Time for Archiving)
    try:
        tz = pytz.timezone('Asia/Shanghai')
        now = datetime.now(tz)
    except:
        now = datetime.now() # Fallback to local time if pytz fails

    timestamp_str = now.strftime('%Y-%m-%d %H:%M:%S')
    timestamp_for_filename = now.strftime('%Y%m%d_%H%M%S')
    ym_dir = now.strftime('%Y%m')  # e.g., 202510
    
    # 1. Create the year-month directory directly
    os.makedirs(ym_dir, exist_ok=True)
    
    # 2. Define the report path
    REPORT_FILE = os.path.join(ym_dir, f"{REPORT_BASE_NAME}_{timestamp_for_filename}.md")

    candidates = []
    
    if market == "普涨":
        for filepath in glob.glob(os.path.join(FUND_DATA_DIR, '*.csv')):
            code = os.path.basename(filepath)[:-4]
            try:
                df = pd.read_csv(filepath)
                df = df.rename(columns={'net_value': 'value'})
                
                ind = calculate_flexible_indicators(df)
                if not ind:
                    continue
                
                score = calculate_trend_score(ind)
                if score >= 2:  # Candidate selection threshold: score >= 2
                    candidates.append({
                        'code': code,
                        'up_days': ind['up_days'],
                        'valuation': ind['valuation_ratio'],
                        'macd': ind['macd_signal'],
                        'score': score,
                        'net': ind['latest_net']
                    })
            except Exception:
                # Silently ignore files that cause processing errors
                continue
    
    # --- Generate Report ---
    report = f"# 趋势跟随系统 v2.1 报告 ({timestamp_str} UTC+8)\n\n"
    report += f"**当前市场状态：{market}**\n\n"
    
    if market != "普涨":
        report += "**当前市场非普涨，系统处于休眠状态，不进行趋势跟随操作。**\n"
    elif not candidates:
        report += "**市场普涨，但暂无符合条件基金（评分 $\\ge 2$），继续观察。**\n\n"
        report += f"**弹性评分核心条件（满足 2/3 即可入选）：**\n"
        report += f"- 评分项1 (动能): 连涨天数 $\\ge$ {MIN_UP_DAYS}天\n"
        report += f"- 评分项2 (估值): 净值/MA50 $\\le$ {MAX_VALUATION_RATIO}\n"
        report += f"- 评分项3 (趋势): MACD为 '金叉' 或 '即将金叉'\n"
    else:
        report += f"**✅ 发现 {len(candidates)} 只趋势基金（评分 $\\ge 2$）**\n\n"
        report += f"**弹性评分核心条件（满足 2/3 即可入选）：**\n"
        report += f"- 评分项1 (动能): 连涨天数 $\\ge$ {MIN_UP_DAYS}天\n"
        report += f"- 评分项2 (估值): 净值/MA50 $\\le$ {MAX_VALUATION_RATIO}\n"
        report += f"- 评分项3 (趋势): MACD为 '金叉' 或 '即将金叉'\n\n"
        
        # Sort: Score (desc), then Consecutive Up Days (desc)
        candidates.sort(key=lambda x: (x['score'], x['up_days']), reverse=True)
        
        report += "| 基金 | 连涨天数 | 估值(净值/MA50) | MACD信号 | **趋势评分** | 最新净值 | 建议 |\n"
        report += "|------|----------|----------------|-----------|---------------|----------|------|\n"
        for c in candidates:
            # Advice based on the score
            suggest = '试探1000元，止损5%' if c['score'] == 3 else '观察或小试500元'
            report += f"| `{c['code']}` | {c['up_days']} | {c['valuation']:.3f} | {c['macd']} | **{c['score']}/3** | {c['net']:.4f} | {suggest} |\n"
        
        report += "\n---\n"
        report += "**策略纪律:**\n"
        report += "1. **小仓位试探:** 本模式下的建仓仓位应低于 Deep Value (超跌反弹) 模式。\n"
        report += "2. **快速止盈/止损:** 趋势跟随风险相对较高，若收益率达到 **3-5%** 应考虑止盈；一旦 **MACD 出现死叉**，应**立即清仓**。\n"
    
    # Write the report file
    with open(REPORT_FILE, 'w', encoding='utf-8') as f:
        f.write(report)
    print(f"趋势报告已生成：{REPORT_FILE}")

if __name__ == '__main__':
    run_trend_follow()
