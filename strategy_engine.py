import os
import glob
import pandas as pd
from datetime import datetime
from multiprocessing import Pool, cpu_count

# --- 策略参数 ---
RSI_LIMIT = 30
BIAS_LIMIT = -4.0

def calculate_rsi(series, period=6):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def process_file(file_path):
    try:
        try:
            df = pd.read_csv(file_path, encoding='utf-8')
        except:
            df = pd.read_csv(file_path, encoding='gbk')
            
        if df.empty: return None

        # --- 格式自适应逻辑 ---
        # 如果是场外基金格式 (date, net_value)
        if 'net_value' in df.columns:
            df = df.rename(columns={'date': '日期', 'net_value': '收盘'})
            # 场外基金通常是倒序的，必须翻转成正序计算指标
            df['日期'] = pd.to_datetime(df['日期'])
            df = df.sort_values(by='日期', ascending=True).reset_index(drop=True)
        
        # 检查是否包含核心列
        if '收盘' not in df.columns or '日期' not in df.columns:
            return None
            
        if len(df) < 30: return None
        
        # 计算指标
        df['rsi'] = calculate_rsi(df['收盘'], 6)
        df['ma6'] = df['收盘'].rolling(window=6).mean()
        df['bias'] = ((df['收盘'] - df['ma6']) / df['ma6']) * 100
        
        latest = df.iloc[-1]
        code = os.path.splitext(os.path.basename(file_path))[0]
        
        if (latest['rsi'] < RSI_LIMIT and latest['bias'] < BIAS_LIMIT):
            return {
                '日期': str(latest['日期']).split(' ')[0],
                '代码': code,
                '价格/净值': round(latest['收盘'], 4),
                'RSI': round(latest['rsi'], 2),
                'BIAS': round(latest['bias'], 2)
            }
    except Exception as e:
        print(f"Error {file_path}: {e}")
    return None

def get_performance():
    history_files = glob.glob('202*/**/*.csv', recursive=True)
    perf_list = []
    for h_file in history_files:
        if 'track' in h_file: continue
        try:
            h_df = pd.read_csv(h_file)
            for _, sig in h_df.iterrows():
                code = str(sig['fund_code'])
                raw_path = f'fund_data/{code}.csv'
                if os.path.exists(raw_path):
                    # 这里同样需要自适应读取逻辑
                    raw_df = pd.read_csv(raw_path)
                    if 'net_value' in raw_df.columns:
                        raw_df = raw_df.rename(columns={'date': '日期', 'net_value': '收盘'})
                        raw_df['日期'] = pd.to_datetime(raw_df['日期']).dt.strftime('%Y-%m-%d')
                    
                    # 匹配信号日期
                    idx = raw_df[raw_df['日期'].astype(str) == str(sig['date'])].index
                    # 注意：如果原本是倒序翻转后的匹配
                    if len(idx) > 0 and (idx[0] + 1) < len(raw_df):
                        next_day = raw_df.iloc[idx[0] + 1]
                        change = (next_day['收盘'] - sig['price']) / sig['price'] * 100
                        perf_list.append({
                            '信号日期': sig['date'], '代码': code, '入场价': sig['price'],
                            '次日表现': next_day['收盘'], '涨跌%': round(change, 2),
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
        archive_df = pd.DataFrame(current_signals).rename(columns={'日期':'date', '代码':'fund_code', '价格/净值':'price'})
        archive_df.to_csv(os.path.join(out_path, f"signals_{now.strftime('%H%M%S')}.csv"), index=False)
    
    perf_df = get_performance()
    
    # 写入 README.md
    now_bj = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    md_content = f"# 🤖 ETF/基金 策略监控看板\n\n"
    md_content += f"> 更新时间: `{now_bj}` | 策略: RSI(6)<30 & BIAS(6)<-4%\n\n"
    
    md_content += "### 🎯 触发买入信号\n"
    if current_signals:
        md_content += pd.DataFrame(current_signals).to_markdown(index=False) + "\n"
    else:
        md_content += "✅ **空仓等待机会。**\n"
    
    md_content += "\n### 📈 策略历史表现\n"
    if not perf_df.empty:
        win_rate = (perf_df['结果'] == '涨').sum() / len(perf_df) * 100
        md_content += f"**总次数**: `{len(perf_df)}` | **次日上涨胜率**: `{win_rate:.2f}%` \n\n"
        md_content += perf_df.tail(10).iloc[::-1].to_markdown(index=False) + "\n"
    else:
        md_content += "⏳ 等待历史信号复盘...\n"
        
    with open('README.md', 'w', encoding='utf-8') as f:
        f.write(md_content)

if __name__ == "__main__":
    main()
