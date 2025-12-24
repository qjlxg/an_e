import os
import glob
import pandas as pd
from datetime import datetime
from multiprocessing import Pool, cpu_count

# --- 策略参数 ---
RSI_LOW = 30
BIAS_LOW = -4.0
RETR_WATCH = -5.0  # 高位回撤10%进入雷达
VOL_BURST = 1.5    # 1.5倍放量

def calculate_rsi(series, period=6):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    return 100 - (100 / (1 + (gain / loss)))

def process_file(file_path):
    """处理核心逻辑：筛选信号"""
    try:
        try:
            df = pd.read_csv(file_path, encoding='utf-8')
        except:
            df = pd.read_csv(file_path, encoding='gbk')
        if df.empty: return None

        # 格式自适应
        is_otc = 'net_value' in df.columns
        if is_otc:
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
        df['v_ma5'] = df['成交量'].rolling(window=5).mean()
        df['v_ratio'] = df['成交量'] / df['v_ma5']

        curr = df.iloc[-1]
        code = os.path.splitext(os.path.basename(file_path))[0]
        
        # 只要满足“回撤过10%”就进备选池
        if curr['retr'] <= RETR_WATCH:
            tags = []
            if curr['rsi'] < RSI_LOW: tags.append("RSI")
            if curr['bias'] < BIAS_LOW: tags.append("BIAS")
            if not is_otc and curr['v_ratio'] > VOL_BURST: tags.append("🔥")
            
            return {
                'date': str(curr['日期']).split(' ')[0],
                'fund_code': code,
                'price': round(curr['收盘'], 4),
                '回撤%': round(curr['retr'], 2),
                'RSI': round(curr['rsi'], 2),
                'BIAS': round(curr['bias'], 2),
                '量比': round(curr['v_ratio'], 2) if curr['v_ratio'] > 0 else "--",
                '信号': " ".join(tags) if tags else "观察"
            }
    except: return None
    return None

def get_performance_history():
    """复盘功能：对比历史信号与次日实际走势"""
    history_files = glob.glob('202*/**/*.csv', recursive=True)
    perf_list = []
    for h_file in history_files:
        if 'performance' in h_file or 'history' in h_file: continue
        try:
            h_df = pd.read_csv(h_file)
            for _, sig in h_df.iterrows():
                code = str(sig['fund_code'])
                raw_path = f'fund_data/{code}.csv'
                if os.path.exists(raw_path):
                    raw_df = pd.read_csv(raw_path)
                    # 适配场内/场外列名
                    if 'net_value' in raw_df.columns:
                        raw_df = raw_df.rename(columns={'date': '日期', 'net_value': '收盘'})
                    
                    raw_df['日期'] = pd.to_datetime(raw_df['日期']).dt.strftime('%Y-%m-%d')
                    idx = raw_df[raw_df['日期'] == str(sig['date'])].index
                    if len(idx) > 0 and (idx[0] + 1) < len(raw_df):
                        next_day = raw_df.iloc[idx[0] + 1]
                        change = (next_day['收盘'] - sig['price']) / sig['price'] * 100
                        perf_list.append({
                            '日期': sig['date'], '代码': code, '买入价': sig['price'],
                            '次日价': next_day['收盘'], '涨跌%': round(change, 2),
                            '结果': '涨' if change > 0 else '跌'
                        })
        except: continue
    return pd.DataFrame(perf_list)

def update_readme(current_res, perf_df):
    """生成漂亮的 README 面板"""
    now_bj = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    content = f"# 🤖 ETF/基金 策略雷达 & 历史复盘\n\n> 最后更新: `{now_bj}`\n\n"
    
    # 1. 实时信号
    content += "## 🎯 实时监控 (回撤 > 10%)\n"
    if current_res:
        df = pd.DataFrame(current_res)
        # 强力共振
        strong = df[df['信号'].str.contains('RSI') & df['信号'].str.contains('BIAS')]
        if not strong.empty:
            content += "### 🔴 第一梯队：技术见底 (RSI & BIAS 共振)\n"
            content += strong.to_markdown(index=False) + "\n\n"
        
        # 其他信号
        others = df[~df.index.isin(strong.index)]
        content += "### 🔵 第二梯队：备选观察名单\n"
        content += others.sort_values('回撤%').to_markdown(index=False) + "\n"
    else:
        content += "✅ **当前暂无满足回撤条件的品种。**\n"

    # 2. 历史复盘
    content += "\n## 📈 历史战绩复盘 (次日统计)\n"
    if not perf_df.empty:
        win_rate = (perf_df['结果'] == '涨').sum() / len(perf_df) * 100
        content += f"**总计信号**: `{len(perf_df)}` | **次日上涨概率**: `{win_rate:.2f}%` \n\n"
        content += perf_df.tail(10).iloc[::-1].to_markdown(index=False) + "\n"
    else:
        content += "⏳ **暂无复盘数据。一旦产生历史信号且数据更新，此处将自动核算胜率。**\n"
    
    with open('README.md', 'w', encoding='utf-8') as f:
        f.write(content)

def main():
    # 1. 扫描当前信号
    files = glob.glob('fund_data/*.csv')
    with Pool(cpu_count()) as p:
        results = [r for r in p.map(process_file, files) if r is not None]
    
    # 2. 存档今日信号 (这就是你的历史记录)
    if results:
        now = datetime.now()
        folder = now.strftime('%Y/%m')
        os.makedirs(folder, exist_ok=True)
        pd.DataFrame(results).to_csv(f"{folder}/sig_{now.strftime('%d_%H%M%S')}.csv", index=False)
    
    # 3. 计算历史表现并更新面板
    perf_df = get_performance_history()
    update_readme(results, perf_df)

if __name__ == "__main__":
    main()
