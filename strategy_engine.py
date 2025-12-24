import os
import glob
import pandas as pd
from datetime import datetime
from multiprocessing import Pool, cpu_count

# --- 策略参数 ---
RSI_LIMIT = 30
BIAS_LIMIT = -4.0
PREMIUM_LIMIT = 1.0  # 若CSV无此列则自动忽略

def calculate_rsi(series, period=6):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def process_file(file_path):
    try:
        # 兼容处理：尝试 utf-8 和 gbk 编码
        try:
            df = pd.read_csv(file_path, encoding='utf-8')
        except:
            df = pd.read_csv(file_path, encoding='gbk')
            
        if len(df) < 30: return None
        
        # --- 精准匹配你的 CSV 列名 ---
        # 你的列名是：日期, 开盘, 收盘, 最高, 最低...
        df['rsi'] = calculate_rsi(df['收盘'], 6)
        df['ma6'] = df['收盘'].rolling(window=6).mean()
        df['bias'] = ((df['收盘'] - df['ma6']) / df['ma6']) * 100
        
        latest = df.iloc[-1]
        code = os.path.splitext(os.path.basename(file_path))[0]
        
        # 溢价率逻辑兼容处理
        premium = latest.get('premium_rate', 0) 
        
        if (latest['rsi'] < RSI_LIMIT and latest['bias'] < BIAS_LIMIT and premium < PREMIUM_LIMIT):
            return {
                '日期': latest['日期'],
                '代码': code,
                '价格': round(latest['收盘'], 4),
                'RSI': round(latest['rsi'], 2),
                'BIAS': round(latest['bias'], 2),
                '溢价率': f"{premium}%" if 'premium_rate' in latest else "无数据"
            }
    except Exception as e:
        print(f"Error {file_path}: {e}")
    return None

def get_performance():
    history_files = glob.glob('202*/**/*.csv', recursive=True)
    perf_list = []
    for h_file in history_files:
        if 'report' in h_file or 'track' in h_file: continue
        try:
            h_df = pd.read_csv(h_file)
            for _, sig in h_df.iterrows():
                code = str(sig['fund_code'])
                raw_path = f'fund_data/{code}.csv'
                if os.path.exists(raw_path):
                    raw_df = pd.read_csv(raw_path)
                    # 匹配日期
                    idx = raw_df[raw_df['日期'] == sig['date']].index
                    if len(idx) > 0 and (idx[0] + 1) < len(raw_df):
                        next_day = raw_df.iloc[idx[0] + 1]
                        change = (next_day['收盘'] - sig['price']) / sig['price'] * 100
                        perf_list.append({
                            '信号日期': sig['date'], '代码': code, '入场价': sig['price'],
                            '次日收盘': next_day['收盘'], '涨跌%': round(change, 2),
                            '结果': '涨' if change > 0 else '跌'
                        })
        except: continue
    return pd.DataFrame(perf_list)

def main():
    data_dir = 'fund_data'
    if not os.path.exists(data_dir): return
    files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith('.csv')]
    
    with Pool(cpu_count()) as p:
        current_signals = [r for r in p.map(process_file, files) if r is not None]
    
    if current_signals:
        now = datetime.now()
        out_path = now.strftime('%Y/%m')
        os.makedirs(out_path, exist_ok=True)
        # 存档以便后续复盘
        archive_df = pd.DataFrame(current_signals).rename(columns={'日期':'date', '代码':'fund_code', '价格':'price'})
        archive_df.to_csv(os.path.join(out_path, f"signals_{now.strftime('%H%M%S')}.csv"), index=False)
    
    perf_df = get_performance()
    
    # --- 写入 README.md ---
    now_bj = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    md_content = f"# 🤖 ETF 超跌策略自动化监控\n\n"
    md_content += f"> 最后更新时间: `{now_bj}` (北京时间)\n\n"
    md_content += "## 🎯 今日筛选信号\n"
    if current_signals:
        md_content += pd.DataFrame(current_signals).to_markdown(index=False) + "\n"
    else:
        md_content += "✅ **目前没有超跌信号，耐心等待。**\n"
    
    md_content += "\n## 📈 策略胜率复盘 (信号次日涨跌)\n"
    if not perf_df.empty:
        win_rate = (perf_df['结果'] == '涨').sum() / len(perf_df) * 100
        md_content += f"**累计信号数**: `{len(perf_df)}` | **次日上涨概率**: `{win_rate:.2f}%` \n\n"
        md_content += perf_df.tail(10).iloc[::-1].to_markdown(index=False) + "\n"
    else:
        md_content += "⏳ 暂无复盘数据，等待第一个信号次日产生。\n"
        
    with open('README.md', 'w', encoding='utf-8') as f:
        f.write(md_content)

if __name__ == "__main__":
    main()
