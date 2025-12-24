import os
import glob
import pandas as pd
from datetime import datetime
from multiprocessing import Pool, cpu_count

# --- 参数配置 ---
GRID_GAP = -5.0        # 补仓间距
TAKE_PROFIT = 3.0      # 目标反弹高度 (用于判定结果)
RETR_WATCH = -10.0     # 监控线

def calculate_rsi(series, period=12):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    return 100 - (100 / (1 + (gain / loss)))

def process_file(file_path):
    """分析当前信号"""
    try:
        try: df = pd.read_csv(file_path, encoding='utf-8')
        except: df = pd.read_csv(file_path, encoding='gbk')
        if 'net_value' in df.columns: df = df.rename(columns={'date':'日期','net_value':'收盘'})
        df['日期'] = pd.to_datetime(df['日期'])
        df = df.sort_values(by='日期').reset_index(drop=True)
        if len(df) < 60: return None

        df['rsi'] = calculate_rsi(df['收盘'], 12)
        df['ma20'] = df['收盘'].rolling(window=20).mean()
        df['bias'] = ((df['收盘'] - df['ma20']) / df['ma20']) * 100
        df['max_60'] = df['收盘'].rolling(window=60).max()
        df['retr'] = ((df['收盘'] - df['max_60']) / df['max_60']) * 100

        curr = df.iloc[-1]
        if curr['retr'] <= RETR_WATCH:
            score = 1
            if curr['retr'] <= -15.0: score += 2
            if curr['rsi'] < 30: score += 2
            if curr['bias'] < -5: score += 1
            return {
                'date': str(curr['日期']).split(' ')[0],
                'fund_code': os.path.splitext(os.path.basename(file_path))[0],
                'price': round(curr['收盘'], 4),
                '回撤%': round(curr['retr'], 2),
                '评分': score
            }
    except: return None

def get_history_performance():
    """核心复盘逻辑：计算历史信号的最高反弹和最深跌幅"""
    history_files = sorted(glob.glob('202*/**/*.csv', recursive=True))
    perf_list = []
    for h_file in history_files:
        try:
            h_df = pd.read_csv(h_file)
            for _, sig in h_df.iterrows():
                code = str(sig['fund_code']).zfill(6)
                raw_path = f'fund_data/{code}.csv'
                if not os.path.exists(raw_path): continue
                
                raw_df = pd.read_csv(raw_path)
                if 'net_value' in raw_df.columns: raw_df = raw_df.rename(columns={'date':'日期','net_value':'收盘'})
                raw_df['日期'] = pd.to_datetime(raw_df['日期']).dt.strftime('%Y-%m-%d')
                
                idx = raw_df[raw_df['日期'] == str(sig['date'])].index
                if not idx.empty:
                    future = raw_df.iloc[idx[0]+1 : idx[0]+11] # 追踪10个交易日
                    if not future.empty:
                        max_u = (future['收盘'].max() - sig['price']) / sig['price'] * 100
                        max_d = (future['收盘'].min() - sig['price']) / sig['price'] * 100
                        status = "✅反弹中" if max_u >= TAKE_PROFIT else "⏳磨底中"
                        if max_d <= -8.0: status = "💀跌破位"
                        
                        perf_list.append({
                            '日期': sig['date'], '代码': code,
                            '周期最高%': round(max_u, 2), '期间最深%': round(max_d, 2),
                            '评分': sig['评分'], '结果': status,
                            '入场价': sig['price'] # 隐藏字段，用于网格对比
                        })
        except: continue
    return pd.DataFrame(perf_list)

def update_readme(advice_list, perf_df):
    now_bj = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    content = f"# 📊 基金定投/网格实战看板\n\n> 更新：`{now_bj}`\n\n"
    
    # 1. 今日建议
    content += "## 🎯 今日分批执行建议\n"
    if advice_list:
        df_adv = pd.DataFrame(advice_list).sort_values('评分', ascending=False)
        content += df_adv[['date','fund_code','price','回撤%','评分','操作']].to_markdown(index=False) + "\n\n"

    # 2. 核心：历史定投点效果追踪 (你要保留的部分)
    content += "## 📑 历史定投点效果追踪 (近10日表现)\n"
    if not perf_df.empty:
        # 去重，只显示每个代码最新的历史记录或全部显示
        display_df = perf_df.tail(20).iloc[::-1]
        content += display_df[['日期','代码','周期最高%','期间最深%','评分','结果']].to_markdown(index=False) + "\n"

    with open('README.md', 'w', encoding='utf-8') as f: f.write(content)

def main():
    # 获取今日信号
    files = glob.glob('fund_data/*.csv')
    with Pool(cpu_count()) as p:
        today_res = [r for r in p.map(process_file, files) if r is not None]
    
    # 获取历史复盘数据
    perf_df = get_history_performance()
    
    # 生成今日操作建议 (对比历史入场价)
    advice_list = []
    for sig in today_res:
        action = "🌱 首笔建仓"
        if not perf_df.empty:
            match = perf_df[perf_df['代码'] == str(sig['fund_code']).zfill(6)]
            if not match.empty:
                last_price = match.iloc[-1]['入场价']
                gap = (sig['price'] - last_price) / last_price * 100
                if gap <= GRID_GAP: action = "🔥 网格补仓"
                elif gap >= 3.0: action = "💰 止盈/减仓"
                else: action = "⏳ 锁仓观望"
        sig['操作'] = action
        advice_list.append(sig)

    # 存档今日信号
    if advice_list:
        now = datetime.now()
        path = f"{now.strftime('%Y/%m')}"
        os.makedirs(path, exist_ok=True)
        pd.DataFrame(advice_list).to_csv(f"{path}/fund_sig_{now.strftime('%d_%H%M%S')}.csv", index=False)

    update_readme(advice_list, perf_df)

if __name__ == "__main__": main()
