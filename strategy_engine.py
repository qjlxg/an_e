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
        
        # 计算指标
        df['rsi'] = calculate_rsi(df['收盘'], 6)
        df['ma6'] = df['收盘'].rolling(window=6).mean()
        df['bias'] = ((df['收盘'] - df['ma6']) / df['ma6']) * 100
        df['max_high'] = df['收盘'].rolling(window=RETR_WINDOW).max()
        df['retr'] = ((df['收盘'] - df['max_high']) / df['max_high']) * 100
        
        # --- 信号持续性逻辑 ---
        # 标记所有符合回撤条件的行
        df['in_watch'] = df['retr'] <= RETR_WATCH
        # 计算连续出现的次数
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
    # 此函数逻辑保持复盘统计
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
                    recovery_df = raw_df.iloc[curr_idx+1:]
                    back_days = "未回本"
                    back_idx = recovery_df[recovery_df['收盘'] >= sig['price']].index
                    if not back_idx.empty: back_days = back_idx[0] - curr_idx
                    
                    future_10 = raw_df.iloc[curr_idx+1 : curr_idx+11]
                    if not future_10.empty:
                        max_up = (future_10['收盘'].max() - sig['price']) / sig['price'] * 100
                        max_down = (future_10['收盘'].min() - sig['price']) / sig['price'] * 100
                        status = "✅反弹中" if max_up >= 1.0 else "❌走弱" if max_down <= -3.0 else "⏳磨底中"
                        
                        # 计算当前持有收益 (从信号点到最新收盘)
                        latest_price = raw_df.iloc[-1]['收盘']
                        hold_return = (latest_price - sig['price']) / sig['price'] * 100

                        perf_list.append({
                            '日期': sig['date'], '代码': code,
                            '评分': sig.get('评分', 1), '结果': status,
                            '累积涨跌%': round(hold_return, 2),
                            '回撤%': sig.get('回撤%', 0), 'RSI': sig.get('RSI', 0),
                            '回本天数': back_days, '周期最高%': round(max_up, 2)
                        })
        except: continue
    return pd.DataFrame(perf_list)

def update_readme(current_res, perf_df):
    now_bj = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    content = f"# 🤖 ETF/基金 策略雷达 (实战加强版)\n\n> 最后更新: `{now_bj}`\n\n"
    
    # 1. 效率统计
    if not perf_df.empty:
        win_rate = (len(perf_df[perf_df['结果'] == '✅反弹中']) / len(perf_df)) * 100
        content += f"## 📊 策略效率 (10日追踪)\n> **综合胜率**: `{win_rate:.2f}%` | **总计样本**: `{len(perf_df)}` \n\n"

    # 2. 实时监控 (高评分排前面)
    content += "## 🎯 实时信号监控 (含持续性追踪)\n"
    if current_res:
        df = pd.DataFrame(current_res).sort_values(['评分', '回撤%'], ascending=[False, True])
        content += df.to_markdown(index=False) + "\n\n"
    else:
        content += "> 💤 当前无信号。\n\n"

    # 3. 🔥 重点关注：买入信号后的表现追踪
    content += "## 🔥 活跃买点追踪 (评分>=3 表现监控)\n"
    if not perf_df.empty:
        # 只追踪最近14天内出现的3分以上信号
        recent_date = (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d')
        active_focus = perf_df[(perf_df['评分'] >= 3) & (perf_df['日期'] >= recent_date)]
        if not active_focus.empty:
            content += active_focus[['日期', '代码', '评分', '累积涨跌%', '结果', '回本天数']].sort_values('日期', ascending=False).to_markdown(index=False) + "\n\n"
        else:
            content += "> ⏳ 最近 14 天暂无高分买入信号出现。\n\n"

    # 4. 历史复盘
    content += "## 📈 历史效果全景复盘\n"
    if not perf_df.empty:
        content += perf_df.tail(15).iloc[::-1].to_markdown(index=False) + "\n"
    
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
