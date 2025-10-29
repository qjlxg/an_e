# trend_follow.py
# 专为牛市普涨设计：趋势跟随 + 小仓位试探
import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime
import pytz

# --- 配置 ---
FUND_DATA_DIR = 'fund_data'
INDEX_DATA_DIR = 'index_data'
MIN_UP_DAYS = 2           # 连续上涨天数
MAX_VALUATION_RATIO = 1.15 # 净值/MA50 ≤ 115%
REPORT_FILE = 'reports/trend_follow_report.md'

# --- 市场趋势识别 ---
def detect_market_trend():
    try:
        df = pd.read_csv('index_data/000300.csv').tail(5)
        up_count = (df['value'].diff() > 0).sum()
        if up_count >= 4:
            return "普涨"
        return "非普涨"
    except:
        return "非普涨"

# --- 计算指标 ---
def calculate_up_indicators(df):
    if len(df) < 50:
        return None
    df_asc = df.iloc[::-1].copy()
    
    # 连续上涨天数
    up_days = 0
    for i in range(1, min(6, len(df_asc))):
        if df_asc['value'].iloc[-i] > df_asc['value'].iloc[-i-1]:
            up_days += 1
        else:
            break
    
    # MACD
    ema12 = df_asc['value'].ewm(span=12).mean()
    ema26 = df_asc['value'].ewm(span=26).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9).mean()
    macd_now = macd.iloc[-1]
    signal_now = signal.iloc[-1]
    macd_prev = macd.iloc[-2]
    signal_prev = signal.iloc[-2]
    macd_signal = '金叉' if macd_now > signal_now and macd_prev <= signal_prev else '观察'
    
    # 估值
    ma50 = df_asc['value'].rolling(50).mean().iloc[-1]
    latest = df_asc['value'].iloc[-1]
    valuation_ratio = latest / ma50 if ma50 > 0 else 999
    
    return {
        'up_days': up_days,
        'macd_signal': macd_signal,
        'valuation_ratio': valuation_ratio,
        'latest_net': latest
    }

# --- 主分析 ---
def run_trend_follow():
    market = detect_market_trend()
    if market != "普涨":
        report = f"# 趋势跟随系统\n\n**当前市场：{market} → 系统休眠**\n"
        os.makedirs('reports', exist_ok=True)
        with open(REPORT_FILE, 'w', encoding='utf-8') as f:
            f.write(report)
        return
    
    candidates = []
    for filepath in glob.glob(os.path.join(FUND_DATA_DIR, '*.csv')):
        code = os.path.basename(filepath)[:-4]
        df = pd.read_csv(filepath).sort_values('date', ascending=False)
        df = df.rename(columns={'net_value': 'value'})
        
        ind = calculate_up_indicators(df)
        if not ind:
            continue
            
        if (ind['up_days'] >= MIN_UP_DAYS and
            ind['valuation_ratio'] <= MAX_VALUATION_RATIO and
            ind['macd_signal'] == '金叉'):
            candidates.append({
                'code': code,
                'up_days': ind['up_days'],
                'valuation': round(ind['valuation_ratio'], 3),
                'net': round(ind['latest_net'], 4)
            })
    
    # 生成报告
    os.makedirs('reports', exist_ok=True)
    tz = pytz.timezone('Asia/Shanghai')
    now = datetime.now(tz).strftime('%Y-%m-%d %H:%M')
    
    report = f"# 趋势跟随系统报告 ({now})\n\n"
    report += f"**市场状态：普涨 → 系统激活**\n\n"
    
    if not candidates:
        report += "**暂无符合条件基金，继续观察**\n"
    else:
        report += f"**发现 {len(candidates)} 只趋势基金（小仓位试探）**\n\n"
        report += "| 基金 | 连涨天数 | 估值(净值/MA50) | 最新净值 | 建议 |\n"
        report += "|------|----------|----------------|----------|------|\n"
        for c in sorted(candidates, key=lambda x: x['up_days'], reverse=True):
            report += f"| `{c['code']}` | {c['up_days']} | {c['valuation']} | {c['net']} | **试探1000元，止损5%** |\n"
    
    with open(REPORT_FILE, 'w', encoding='utf-8') as f:
        f.write(report)
    print(f"趋势报告已生成：{REPORT_FILE}")

if __name__ == '__main__':
    run_trend_follow()
