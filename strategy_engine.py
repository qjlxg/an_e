import os
import glob
import pandas as pd
from datetime import datetime
from multiprocessing import Pool, cpu_count

# --- 核心风控参数 ---
RSI_LOW = 30
BIAS_LOW = -4.0
RETR_WATCH = -10.0
VOL_BURST = 1.5
STOP_LOSS = -3.0  # 止损阈值

def calculate_rsi(series, period=6):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    return 100 - (100 / (1 + (gain / loss)))

def process_file(file_path):
    try:
        try:
            df = pd.read_csv(file_path, encoding='utf-8')
        except:
            df = pd.read_csv(file_path, encoding='gbk')
        if df.empty: return None

        # 场内/场外自适应
        if 'net_value' in df.columns:
            df = df.rename(columns={'date': '日期', 'net_value': '收盘'})
            df['日期'] = pd.to_datetime(df['日期'])
            df = df.sort_values(by='日期', ascending=True).reset_index(drop=True)
            df['成交量'] = 0
        else:
            df = df.rename(columns={'成交量': 'vol'})
            df['成交量'] = df.get('vol', 0)

        if '收盘' not in df.columns or len(df) < 30: return None

        # 计算指标
        df['rsi'] = calculate_rsi(df['收盘'], 6)
        df['ma6'] = df['收盘'].rolling(window=6).mean()
        df['bias'] = ((df['收盘'] - df['ma6']) / df['ma6']) * 100
        df['max_30'] = df['收盘'].rolling(window=30).max()
        df['retr'] = ((df['收盘'] - df['max_30']) / df['max_30']) * 100
        df['v_ratio'] = df['成交量'] / df['成交量'].rolling(window=5).mean()

        curr = df.iloc[-1]
        code = os.path.splitext(os.path.basename(file_path))[0]
        
        if curr['retr'] <= RETR_WATCH:
            score = 1
            tags = []
            if curr['rsi'] < RSI_LOW: score += 2; tags.append("RSI")
            if curr['bias'] < BIAS_LOW: score += 2; tags.append("BIAS")
            if curr['v_ratio'] > VOL_BURST: score += 2; tags.append("🔥")
            
            return {
                'date': str(curr['日期']).split(' ')[0],
                'fund_code': code,
                'price': round(curr['收盘'], 4),
                '回撤%': round(curr['retr'], 2),
                '评分': score,
                '信号': " ".join(tags) if tags else "观察"
            }
    except: return None
    return None

def get_performance_with_risk():
    """复盘：不仅看涨多少，还看过程中跌了多少 """
    history_files = glob.glob('202*/**/*.csv', recursive=True)
    perf_list = []
    for h_file in history_files:
        if any(x in h_file for x in ['performance', 'track']): continue
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
                        future_df = raw_df.iloc[curr_idx+1 : curr_idx+4]
                        if not future_df.empty:
                            max_p = future_df['收盘'].max()
                            min_p = future_df['收盘'].min()
                            
                            max_up = (max_p - sig['price']) / sig['price'] * 100
                            max_down = (min_p - sig['price']) / sig['price'] * 100
                            
                            # 判定逻辑
                            if max_down <= STOP_LOSS: status = "💀止损"
                            elif max_up >= 1.2 and max_down > -1.5: status = "✨优质"
                            elif max_up >= 1.2: status = "⚠️险胜"
                            else: status = "❌走弱"
                            
                            perf_list.append({
                                '日期': sig['date'], '代码': code,
                                '3日最高%': round(max_up, 2),
                                '期间最大亏%': round(max_down, 2),
                                '状态': status
                            })
        except: continue
    return pd.DataFrame(perf_list)

def update_readme(current_res, perf_df):
    now_bj = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    content = f"# 🤖 ETF/基金 智能风控看板\n\n> 最后更新: `{now_bj}`\n\n"
    
    if not perf_df.empty:
        total = len(perf_df)
        win = len(perf_df[perf_df['状态'].str.contains('优质|险胜')])
        content += "## 📊 策略回测报告\n"
        content += f"> **实战胜率**: `{win/total*100:.2f}%` | **优质信号比**: `{len(perf_df[perf_df['状态']=='✨优质'])/total*100:.2f}%` \n\n"

    content += "## 🎯 实时信号池\n"
    if current_res:
        df = pd.DataFrame(current_res).sort_values('评分', ascending=False)
        content += df.to_markdown(index=False) + "\n\n"
    
    content += "## 📈 历史风控明细 (含最大浮亏追踪)\n"
    if not perf_df.empty:
        content += perf_df.tail(20).iloc[::-1].to_markdown(index=False) + "\n"
    
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
    
    perf_df = get_performance_with_risk()
    update_readme(results, perf_df)

if __name__ == "__main__":
    main()
