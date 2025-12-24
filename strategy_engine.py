import os
import glob
import pandas as pd
from datetime import datetime
from multiprocessing import Pool, cpu_count

# --- 核心参数设置 ---
GRID_GAP = -5.0        # 补仓网格：较上次买入跌5%再补
RETR_WATCH = -10.0     # 进入雷达的回撤门槛
RSI_LOW = 30           # 超卖阈值
BIAS_LOW = -5.0        # 乖离率阈值

def calculate_rsi(series, period=12):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    return 100 - (100 / (1 + (gain / loss)))

def process_file(file_path):
    """分析单个基金数据"""
    try:
        try: df = pd.read_csv(file_path, encoding='utf-8')
        except: df = pd.read_csv(file_path, encoding='gbk')
        
        if 'net_value' in df.columns:
            df = df.rename(columns={'date': '日期', 'net_value': '收盘'})
        df['日期'] = pd.to_datetime(df['日期'])
        df = df.sort_values(by='日期').reset_index(drop=True)

        if len(df) < 60: return None
        
        # 计算指标
        df['rsi'] = calculate_rsi(df['收盘'], 12)
        df['ma20'] = df['收盘'].rolling(window=20).mean()
        df['bias'] = ((df['收盘'] - df['ma20']) / df['ma20']) * 100
        df['max_60'] = df['收盘'].rolling(window=60).max()
        df['retr'] = ((df['收盘'] - df['max_60']) / df['max_60']) * 100

        curr = df.iloc[-1]
        code = os.path.splitext(os.path.basename(file_path))[0]
        
        if curr['retr'] <= RETR_WATCH:
            score = 1
            if curr['retr'] <= -15.0: score += 2
            if curr['rsi'] < RSI_LOW: score += 2
            if curr['bias'] < BIAS_LOW: score += 1
            
            return {
                'date': str(curr['日期']).split(' ')[0],
                'fund_code': code,
                'price': round(curr['收盘'], 4),
                '回撤%': round(curr['retr'], 2),
                'RSI': round(curr['rsi'], 2),
                '评分': score
            }
    except: return None

def get_last_entry_from_history(fund_code):
    """从所有历史存档中找该基金的最后一笔价格"""
    history_files = sorted(glob.glob('202*/**/*.csv', recursive=True))
    if not history_files: return None
    
    # 从最新的文件往回找
    for f in reversed(history_files):
        try:
            h_df = pd.read_csv(f)
            # 统一转成字符串匹配
            match = h_df[h_df['fund_code'].astype(str).str.zfill(6) == str(fund_code).zfill(6)]
            if not match.empty:
                return match.iloc[-1]['price']
        except: continue
    return None

def main():
    # 1. 获取今日信号
    files = glob.glob('fund_data/*.csv')
    with Pool(cpu_count()) as p:
        today_signals = [r for r in p.map(process_file, files) if r is not None]
    
    # 2. 结合历史数据给出网格建议
    advice_list = []
    for sig in today_signals:
        last_price = get_last_entry_from_history(sig['fund_code'])
        
        if last_price:
            change = (sig['price'] - last_price) / last_price * 100
            if change <= GRID_GAP:
                sig['操作'] = "🔥 网格补仓"
            elif change >= 5.0: # 相比上次买入涨了5%
                sig['操作'] = "💰 止盈减仓"
            else:
                sig['操作'] = "⏳ 锁仓等待"
        else:
            sig['操作'] = "🌱 首笔建仓" if sig['评分'] >= 4 else "🔭 持续观察"
        advice_list.append(sig)

    # 3. 存档今日数据 (保留历史)
    if advice_list:
        now = datetime.now()
        folder = now.strftime('%Y/%m')
        os.makedirs(folder, exist_ok=True)
        filename = f"{folder}/fund_sig_{now.strftime('%d_%H%M%S')}.csv"
        pd.DataFrame(advice_list).to_csv(filename, index=False)
        
        # 4. 更新 README 看板
        update_readme(advice_list)

def update_readme(advice_list):
    now_bj = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df = pd.DataFrame(advice_list).sort_values('评分', ascending=False)
    
    content = f"# 📊 基金网格实战雷达\n\n> 更新：`{now_bj}` | 策略：网格分批加仓\n\n"
    
    # 市场情绪警报
    if len(df[df['评分'] >= 4]) >= 5:
        content += "> 🚨 **底部共振**：当前多个品种进入深度超跌区，适合执行网格补仓。\n\n"

    content += "## 🎯 今日网格执行建议\n"
    content += df.to_markdown(index=False) + "\n\n"
    
    content += "## 📑 网格说明\n"
    content += f"- **网格间距**：{GRID_GAP}%（相比上次买入价跌破此值才补仓）。\n"
    content += "- **历史存档**：所有历史信号均保存在相应月份文件夹下，作为补仓参考依据。\n"

    with open('README.md', 'w', encoding='utf-8') as f:
        f.write(content)

if __name__ == "__main__":
    main()
