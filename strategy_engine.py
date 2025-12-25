import os
import glob
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from multiprocessing import Pool, cpu_count

# --- 核心锁死风控参数 ---
TOTAL_BUDGET_CAP = 10000   # 1万本金锁死
PORTFOLIO_UNIT = 2000      # 单笔抄底2000元
STOP_BUY_LOSS_RATIO = -5.0 # 组合总亏损超过5%，禁买令开启

# --- 策略技术参数 ---
RETR_WINDOW = 250      
RETR_WATCH = -10.0     
RSI_LOW = 30           
BIAS_LOW = -5.0        

def calculate_rsi(series, period=6):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    return 100 - (100 / (1 + (gain / loss)))

def process_file(file_path):
    try:
        try: df = pd.read_csv(file_path, encoding='utf-8')
        except: df = pd.read_csv(file_path, encoding='gbk')
        if 'net_value' in df.columns:
            df = df.rename(columns={'date': '日期', 'net_value': '收盘'})
        df['日期'] = pd.to_datetime(df['日期'])
        df = df.sort_values(by='日期').reset_index(drop=True)
        
        # --- 数据清洗逻辑 ---
        if len(df) < 2: return None
        curr = df.iloc[-1]
        prev = df.iloc[-2]
        # 如果最新价是1.0且跌幅离谱（比如从1.2以上直接掉下来），判定为数据缺失，丢弃
        if curr['收盘'] == 1.0 and prev['收盘'] > 1.1: return None
        
        df['rsi'] = calculate_rsi(df['收盘'], 6)
        df['ma6'] = df['收盘'].rolling(window=6).mean()
        df['bias'] = ((df['收盘'] - df['ma6']) / df['ma6']) * 100
        df['max_high'] = df['收盘'].rolling(window=RETR_WINDOW).max()
        df['retr'] = ((df['收盘'] - df['max_high']) / df['max_high']) * 100
        
        df['in_watch'] = df['retr'] <= RETR_WATCH
        df['persist_days'] = df['in_watch'].groupby((df['in_watch'] != df['in_watch'].shift()).cumsum()).cumcount() + 1
        df.loc[~df['in_watch'], 'persist_days'] = 0

        curr = df.iloc[-1] # 重新获取包含指标的末行
        code = os.path.splitext(os.path.basename(file_path))[0]
        
        if curr['in_watch']:
            score = 1
            if curr['rsi'] < RSI_LOW: score += 2
            if curr['bias'] < BIAS_LOW: score += 2
            
            risk_level = "正常"
            if curr['rsi'] > 55 and score == 1:
                risk_level = "🚩高风险(陷阱)"
            elif score >= 3:
                risk_level = "✅高胜率区"
                
            return {
                'date': str(curr['日期']).split(' ')[0],
                'fund_code': code,
                '评分': score,
                '持续天数': int(curr['persist_days']),
                '风险预警': risk_level,
                '回撤%': round(curr['retr'], 2),
                'RSI': round(curr['rsi'], 2),
                'BIAS': round(curr['bias'], 2),
                'price': round(curr['收盘'], 4)
            }
    except: return None

def get_performance_stats():
    history_files = glob.glob('202*/**/*.csv', recursive=True)
    perf_list = []
    for h_file in history_files:
        if 'perf' in h_file: continue
        try:
            h_df = pd.read_csv(h_file)
            for _, sig in h_df.iterrows():
                code = str(sig['fund_code']).zfill(6)
                raw_path = f'fund_data/{code}.csv'
                if not os.path.exists(raw_path): continue
                raw_df = pd.read_csv(raw_path)
                if 'net_value' in raw_df.columns: raw_df = raw_df.rename(columns={'date': '日期', 'net_value': '收盘'})
                raw_df['日期'] = pd.to_datetime(raw_df['日期']).dt.strftime('%Y-%m-%d')
                
                idx_list = raw_df[raw_df['日期'] == str(sig['date'])].index
                if not idx_list.empty:
                    curr_idx = idx_list[0]
                    signal_price = sig['price']
                    latest_price = raw_df.iloc[-1]['收盘']
                    
                    # --- 统计端数据清洗 ---
                    # 价格正好为1.0且亏损超过20%，大概率是缺失数据，不进入追踪
                    if latest_price == 1.0 and signal_price > 1.2: continue
                    
                    prev_price = raw_df.iloc[-2]['收盘'] if len(raw_df) > 1 else latest_price
                    daily_raw = (latest_price - prev_price) / prev_price * 100
                    color_tag = "🔴 " if daily_raw > 0 else "🟢 " if daily_raw < 0 else ""
                    daily_display = f"{color_tag}{daily_raw:+.2f}%"
                    total_hold_change = (latest_price - signal_price) / signal_price * 100
                    
                    recovery_df = raw_df.iloc[curr_idx+1:]
                    back_days = "未回本"
                    back_idx = recovery_df[recovery_df['收盘'] >= signal_price].index
                    if not back_idx.empty: back_days = int(back_idx[0] - curr_idx)
                    
                    perf_list.append({
                        '日期': sig['date'], '代码': code, '评分': sig.get('评分', 1),
                        '信号价': round(signal_price, 4), '最新价': round(latest_price, 4),
                        '今日涨跌': daily_display, '总盈亏%': round(total_hold_change, 2),
                        '回本天数': back_days,
                        '状态': "✅反弹中" if total_hold_change > 1 else "❌走弱" if total_hold_change < -3 else "⏳磨底中"
                    })
        except: continue
    return pd.DataFrame(perf_list)

def update_readme(current_res, perf_df):
    now_bj = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    content = f"# 🤖 ETF/基金 策略雷达 (实战锁死+清洗版)\n\n> 最后更新: `{now_bj}`\n\n"
    
    total_invested = 0
    total_profit_loss_val = 0
    avg_return_rate = 0
    is_budget_full = False
    is_panic_mode = False

    if not perf_df.empty:
        recent_limit = (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d')
        active_focus = perf_df[(perf_df['评分'] >= 3) & (perf_df['日期'] >= recent_limit)].drop_duplicates(subset=['代码'])
        
        if not active_focus.empty:
            total_invested = len(active_focus) * PORTFOLIO_UNIT
            total_profit_loss_val = (active_focus['总盈亏%'] / 100 * PORTFOLIO_UNIT).sum()
            avg_return_rate = (total_profit_loss_val / total_invested) * 100 if total_invested > 0 else 0
            
            if total_invested >= TOTAL_BUDGET_CAP: is_budget_full = True
            if avg_return_rate <= STOP_BUY_LOSS_RATIO: is_panic_mode = True

            content += "## 💰 实战风控盘口 (含数据异常过滤)\n"
            content += f"> **模拟总投入**: `¥{total_invested} / ¥{TOTAL_BUDGET_CAP}` | **当前总盈亏**: `{'🔴' if total_profit_loss_val > 0 else '🟢'} ¥{total_profit_loss_val:.2f} ({avg_return_rate:+.2f}%)` \n"
            status_desc = "🛡️ 预算内" if not is_budget_full else "⛔ 预算满员"
            if is_panic_mode: status_desc += " | ❌ 禁买令 (组合亏损超标)"
            content += f"> **风控状态**: `{status_desc}`\n\n"

    content += "## 🎯 实时信号监控\n"
    if current_res:
        df = pd.DataFrame(current_res).sort_values(['评分', '回撤%'], ascending=[False, True])
        def decide(row):
            if row['评分'] < 3: return "等待3分"
            if is_budget_full: return "⛔ 预算上限(观望)"
            if is_panic_mode: return "❌ 组合亏损(停买)"
            return "✅ 可分批建仓"
        df['建议'] = df.apply(decide, axis=1)
        content += df.to_markdown(index=False) + "\n\n"
    else:
        content += "> 💤 无触发回撤阈值的品种。\n\n"

    content += "## 🔥 活跃买点追踪 (已过滤异常数据)\n"
    if not perf_df.empty:
        recent_limit = (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d')
        active_focus = perf_df[(perf_df['评分'] >= 3) & (perf_df['日期'] >= recent_limit)].sort_values('日期', ascending=False).drop_duplicates(subset=['代码'])
        if not active_focus.empty:
            cols = ['日期', '代码', '评分', '信号价', '最新价', '今日涨跌', '总盈亏%', '状态', '回本天数']
            content += active_focus[cols].to_markdown(index=False) + "\n\n"

    with open('README.md', 'w', encoding='utf-8') as f: f.write(content)

def main():
    files = glob.glob('fund_data/*.csv')
    with Pool(cpu_count()) as p:
        results = [r for r in p.map(process_file, files) if r is not None]
    if results:
        now = datetime.now()
        folder = now.strftime('%Y/%m')
        os.makedirs(folder, exist_ok=True)
        pd.DataFrame(results).to_csv(f"{folder}/sig_{now.strftime('%d_%H%M%S')}.csv", index=False)
    perf_df = get_performance_stats()
    update_readme(results, perf_df)

if __name__ == "__main__": main()
