import os
import glob
import pandas as pd
from datetime import datetime
from multiprocessing import Pool, cpu_count

# --- 核心参数 (绝对不动，作为实战标尺) ---
GRID_GAP = -5.0        
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
        
        # 核心指标逻辑：严格维持原样
        df['rsi'] = calculate_rsi(df['收盘'], 6)
        df['ma6'] = df['收盘'].rolling(window=6).mean()
        df['bias'] = ((df['收盘'] - df['ma6']) / df['ma6']) * 100
        df['max_30'] = df['收盘'].rolling(window=30).max()
        df['retr'] = ((df['收盘'] - df['max_30']) / df['max_30']) * 100
        
        curr = df.iloc[-1]
        code = os.path.splitext(os.path.basename(file_path))[0]
        
        if curr['retr'] <= RETR_WATCH:
            score = 1
            if curr['rsi'] < RSI_LOW: score += 2
            if curr['bias'] < BIAS_LOW: score += 2
            return {
                'date': str(curr['日期']).split(' ')[0],
                'fund_code': code,
                'price': round(curr['收盘'], 4),
                '回撤%': round(curr['retr'], 2),
                'RSI': round(curr['rsi'], 2),
                'BIAS': round(curr['bias'], 2),
                '评分': score,
                '信号': "重点" if score >= 3 else "观察"
            }
    except: return None

def get_performance_stats():
    """仅升级复盘表，增加原始环境数值，用于后续分析"""
    history_files = glob.glob('202*/**/*.csv', recursive=True)
    perf_list = []
    for h_file in history_files:
        if 'perf' in h_file: continue
        try:
            h_df = pd.read_csv(h_file)
            for _, sig in h_df.iterrows():
                code = str(sig['fund_code']).zfill(6)
                raw_path = f'fund_data/{code}.csv'
                if os.path.exists(raw_path):
                    raw_df = pd.read_csv(raw_path)
                    if 'net_value' in raw_df.columns:
                        raw_df = raw_df.rename(columns={'date': '日期', 'net_value': '收盘'})
                    raw_df['日期'] = pd.to_datetime(raw_df['日期']).dt.strftime('%Y-%m-%d')
                    
                    idx_list = raw_df[raw_df['日期'] == str(sig['date'])].index
                    if not idx_list.empty:
                        curr_idx = idx_list[0]
                        # 追踪后3日表现
                        future_df = raw_df.iloc[curr_idx+1 : curr_idx+4]
                        if not future_df.empty:
                            max_up = (future_df['收盘'].max() - sig['price']) / sig['price'] * 100
                            max_down = (future_df['收盘'].min() - sig['price']) / sig['price'] * 100
                            
                            status = "✅反弹中" if max_up >= 1.0 else "❌走弱" if max_down <= -3.0 else "⏳磨底中"
                            
                            perf_list.append({
                                '日期': sig['date'], '代码': code,
                                '回撤%': sig.get('回撤%', 0), # 记录触发时的原始回撤
                                'RSI': sig.get('RSI', 0),    # 记录触发时的原始RSI
                                'BIAS': sig.get('BIAS', 0),  # 记录触发时的原始BIAS
                                '周期最高%': round(max_up, 2), 
                                '期间最深%': round(max_down, 2),
                                '评分': sig.get('评分', 1), 
                                '结果': status
                            })
        except: continue
    return pd.DataFrame(perf_list)

def update_readme(current_res, perf_df):
    now_bj = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    content = f"# 🤖 ETF/基金 策略雷达 (实战加强版)\n\n> 最后更新: `{now_bj}`\n\n"
    
    if not perf_df.empty:
        win_rate = (len(perf_df[perf_df['结果'] == '✅反弹中']) / len(perf_df)) * 100
        content += f"## 📊 策略效率 (3日内最高反弹 > 1% 概率)\n> **当前综合胜率**: `{win_rate:.2f}%` | **回测样本**: `{len(perf_df)}` \n\n"

    content += "## 🎯 实时监控 (回撤 > 10%)\n"
    if current_res:
        df = pd.DataFrame(current_res).sort_values('评分', ascending=False)
        content += df.to_markdown(index=False) + "\n\n"
    
    content += "## 📈 历史定投点效果追踪 (详细回测版)\n"
    if not perf_df.empty:
        # 按你的要求，输出包含原始数值的详细表格
        cols = ['日期', '代码', '回撤%', 'RSI', 'BIAS', '周期最高%', '期间最深%', '评分', '结果']
        content += perf_df[cols].tail(25).iloc[::-1].to_markdown(index=False) + "\n"
    
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
