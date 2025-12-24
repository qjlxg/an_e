import os
import glob
import pandas as pd
from datetime import datetime
from multiprocessing import Pool, cpu_count

# --- 参数 ---
RSI_LOW = 30
BIAS_LOW = -4.0
RETR_WATCH = -10.0
VOL_BURST = 1.5

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

        if 'net_value' in df.columns:
            df = df.rename(columns={'date': '日期', 'net_value': '收盘'})
            df['日期'] = pd.to_datetime(df['日期'])
            df = df.sort_values(by='日期', ascending=True).reset_index(drop=True)
            df['成交量'] = 0
        else:
            df = df.rename(columns={'成交量': 'vol'})
            df['成交量'] = df.get('vol', 0)

        if '收盘' not in df.columns or len(df) < 30: return None

        df['rsi'] = calculate_rsi(df['收盘'], 6)
        df['ma6'] = df['收盘'].rolling(window=6).mean()
        df['bias'] = ((df['收盘'] - df['ma6']) / df['ma6']) * 100
        df['max_30'] = df['收盘'].rolling(window=30).max()
        df['retr'] = ((df['收盘'] - df['max_30']) / df['max_30']) * 100
        df['v_ma5'] = df['成交量'].rolling(window=5).mean()
        df['v_ratio'] = df['成交量'] / df['v_ma5']

        curr = df.iloc[-1]
        code = os.path.splitext(os.path.basename(file_path))[0]
        
        if curr['retr'] <= RETR_WATCH:
            # --- 评分逻辑 ---
            score = 1 # 基础回撤分
            tags = []
            if curr['rsi'] < RSI_LOW: 
                score += 2
                tags.append("RSI超卖")
            if curr['bias'] < BIAS_LOW: 
                score += 2
                tags.append("BIAS超跌")
            if curr['v_ratio'] > VOL_BURST: 
                score += 2
                tags.append("🔥放量")
            
            return {
                'date': str(curr['日期']).split(' ')[0],
                'fund_code': code,
                'price': round(curr['收盘'], 4),
                '回撤%': round(curr['retr'], 2),
                'RSI': round(curr['rsi'], 2),
                'BIAS': round(curr['bias'], 2),
                '量比': round(curr['v_ratio'], 2) if curr['v_ratio'] > 0 else "--",
                '评分': score,
                '建议': "🚀重点关注" if score >= 3 else "⌛等待"
            }
    except: return None
    return None

def get_performance_3day():
    history_files = glob.glob('202*/**/*.csv', recursive=True)
    perf_list = []
    for h_file in history_files:
        if any(x in h_file for x in ['performance', 'track', 'history']): continue
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
                            max_chg = (max_p - sig['price']) / sig['price'] * 100
                            # 获取最新状态（如果是复盘旧信号，看现在的价格）
                            current_p = raw_df.iloc[-1]['收盘']
                            cum_chg = (current_p - sig['price']) / sig['price'] * 100
                            
                            perf_list.append({
                                '日期': sig['date'], '代码': code,
                                '3日最高%': round(max_chg, 2),
                                '累计总收益%': round(cum_chg, 2),
                                '得分': sig.get('评分', '-'),
                                '结果': '✅获利' if max_chg > 1.2 else '❌走弱'
                            })
        except: continue
    return pd.DataFrame(perf_list)

def update_readme(current_res, perf_df):
    now_bj = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    content = f"# 🤖 ETF/基金 智能决策看板\n\n> 最后更新: `{now_bj}`\n\n"
    
    # 1. 战绩
    if not perf_df.empty:
        win_rate = (perf_df['结果'] == '✅获利').sum() / len(perf_df) * 100
        content += "## 📊 策略实战效率\n"
        content += f"> **3日反弹胜率**: `{win_rate:.2f}%` | **回测样本**: `{len(perf_df)}` \n\n"

    # 2. 实时
    content += "## 🎯 实时决策 (回撤 > 10%)\n"
    if current_res:
        df = pd.DataFrame(current_res).sort_values(by='评分', ascending=False)
        content += df.to_markdown(index=False) + "\n\n"
        content += "> **评分说明**: 3分以上为“极度超跌/异动”，通常反弹概率更高。\n"
    else:
        content += "✅ **当前暂无满足条件的信号。**\n"
    
    # 3. 历史
    content += "\n## 📈 信号表现追踪 (3日表现)\n"
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
    
    perf_df = get_performance_3day()
    update_readme(results, perf_df)

if __name__ == "__main__":
    main()
