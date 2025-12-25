import os
import glob
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from multiprocessing import Pool, cpu_count

# --- 核心参数 ---
RETR_WINDOW = 250      
RETR_WATCH = -10.0     
RSI_LOW = 30           
BIAS_LOW = -5.0        
PORTFOLIO_UNIT = 2000  # 假设每只 3分信号基金 投入 2000元

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
        
        df['rsi'] = calculate_rsi(df['收盘'], 6)
        df['ma6'] = df['收盘'].rolling(window=6).mean()
        df['bias'] = ((df['收盘'] - df['ma6']) / df['ma6']) * 100
        df['max_high'] = df['收盘'].rolling(window=RETR_WINDOW).max()
        df['retr'] = ((df['收盘'] - df['max_high']) / df['max_high']) * 100
        
        df['in_watch'] = df['retr'] <= RETR_WATCH
        df['persist_days'] = df['in_watch'].groupby((df['in_watch'] != df['in_watch'].shift()).cumsum()).cumcount() + 1
        df.loc[~df['in_watch'], 'persist_days'] = 0

        curr = df.iloc[-1]
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
                '建议': "等待3分" if score < 3 else "可分批建仓",
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
                        '日期': sig['date'],
                        '代码': code,
                        '评分': sig.get('评分', 1),
                        '信号价': round(signal_price, 4),
                        '最新价': round(latest_price, 4),
                        '今日涨跌': daily_display,
                        '总盈亏%': round(total_hold_change, 2),
                        '回本天数': back_days,
                        '状态': "✅反弹中" if total_hold_change > 1 else "❌走弱" if total_hold_change < -3 else "⏳磨底中"
                    })
        except: continue
    return pd.DataFrame(perf_list)

def update_readme(current_res, perf_df):
    now_bj = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    content = f"# 🤖 ETF/基金 策略雷达 (250日实战对冲版)\n\n> 最后更新: `{now_bj}`\n\n"
    
    # --- 新增：综合实战盘口 (对冲计算) ---
    content += "## 💰 综合实战盘口 (1万资金模拟对冲)\n"
    if not perf_df.empty:
        recent_limit = (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d')
        active_focus = perf_df[(perf_df['评分'] >= 3) & (perf_df['日期'] >= recent_limit)].drop_duplicates(subset=['代码'])
        
        if not active_focus.empty:
            total_invested = len(active_focus) * PORTFOLIO_UNIT
            total_profit_loss = (active_focus['总盈亏%'] / 100 * PORTFOLIO_UNIT).sum()
            avg_return = (total_profit_loss / total_invested) * 100 if total_invested > 0 else 0
            
            # 对冲强度计算 (正负抵消的程度)
            pos_count = len(active_focus[active_focus['总盈亏%'] > 0])
            neg_count = len(active_focus[active_focus['总盈亏%'] < 0])
            hedge_status = "🛡️ 强对冲" if pos_count > 0 and neg_count > 0 else "⚠️ 风险同向"
            
            content += f"> **当前持仓数**: `{len(active_focus)}` | **累计模拟投入**: `¥{total_invested}`\n"
            content += f"> **对冲总盈亏**: `{'🔴' if total_profit_loss > 0 else '🟢'} ¥{total_profit_loss:.2f} ({avg_return:+.2f}%)` \n"
            content += f"> **对冲状态**: `{hedge_status}` (盈利:{pos_count} / 亏损:{neg_count})\n\n"
        else:
            content += "> 💤 组合当前空仓中。\n\n"

    # 原有功能不变
    content += "## 🎯 实时信号监控 (含持续性)\n"
    if current_res:
        df = pd.DataFrame(current_res).sort_values(['评分', '回撤%'], ascending=[False, True])
        content += df.to_markdown(index=False) + "\n\n"

    content += "## 🔥 活跃买点动态追踪 (3分以上单品)\n"
    if not perf_df.empty:
        recent_limit = (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d')
        active_focus = perf_df[(perf_df['评分'] >= 3) & (perf_df['日期'] >= recent_limit)].sort_values('日期', ascending=False).drop_duplicates(subset=['代码'])
        if not active_focus.empty:
            cols = ['日期', '代码', '评分', '信号价', '最新价', '今日涨跌', '总盈亏%', '状态', '回本天数']
            content += active_focus[cols].to_markdown(index=False) + "\n\n"

    content += "## 📈 历史全景复盘\n"
    if not perf_df.empty:
        hist_cols = ['日期', '代码', '评分', '信号价', '总盈亏%', '状态', '回本天数']
        content += perf_df[hist_cols].tail(15).iloc[::-1].to_markdown(index=False) + "\n"
    
    with open('README.md', 'w', encoding='utf-8') as f:
        f.write(content)

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

if __name__ == "__main__":
    main()
