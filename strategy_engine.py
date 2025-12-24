import os
import glob
import pandas as pd
from datetime import datetime
from multiprocessing import Pool, cpu_count

# --- 核心网格参数 ---
RETR_LEVEL_1 = -10.0  # 基础预警线
RETR_LEVEL_2 = -15.0  # 深度加仓线
RSI_BOTTOM = 30       # 极度超卖
BIAS_LIMIT = -5.0     # 负乖离阈值 (偏离20日线5%)
GRID_GAP = -5.0       # 网格间距：较上次入场跌5%再补
TAKE_PROFIT = 5.0     # 目标止盈位

def calculate_rsi(series, period=12):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    return 100 - (100 / (1 + (gain / loss)))

def process_file(file_path):
    try:
        try: df = pd.read_csv(file_path, encoding='utf-8')
        except: df = pd.read_csv(file_path, encoding='gbk')
        
        # 兼容净值与收盘价
        if 'net_value' in df.columns:
            df = df.rename(columns={'date': '日期', 'net_value': '收盘'})
        df['日期'] = pd.to_datetime(df['日期'])
        df = df.sort_values(by='日期').reset_index(drop=True)

        if len(df) < 60: return None
        
        # 计算核心指标
        df['rsi'] = calculate_rsi(df['收盘'], 12)
        df['ma20'] = df['收盘'].rolling(window=20).mean()
        df['bias'] = ((df['收盘'] - df['ma20']) / df['ma20']) * 100
        df['max_60'] = df['收盘'].rolling(window=60).max()
        df['retr'] = ((df['收盘'] - df['max_60']) / df['max_60']) * 100

        curr = df.iloc[-1]
        code = os.path.splitext(os.path.basename(file_path))[0]
        
        if curr['retr'] <= RETR_LEVEL_1:
            score = 1
            if curr['retr'] <= RETR_LEVEL_2: score += 2
            if curr['rsi'] < RSI_BOTTOM: score += 2
            if curr['bias'] < BIAS_LIMIT: score += 1
            
            return {
                'date': str(curr['日期']).split(' ')[0],
                'fund_code': code,
                'price': round(curr['收盘'], 4),
                '回撤%': round(curr['retr'], 2),
                'RSI': round(curr['rsi'], 2),
                'BIAS': round(curr['bias'], 2),
                '评分': score
            }
    except: return None

def get_performance_and_advice(current_res):
    """复盘历史并生成今日网格动作建议"""
    history_files = sorted(glob.glob('202*/**/*.csv', recursive=True))
    all_history = []
    for f in history_files:
        if 'perf' not in f:
            try: all_history.append(pd.read_csv(f))
            except: pass
    
    hist_df = pd.concat(all_history) if all_history else pd.DataFrame()
    
    final_results = []
    for item in current_res:
        code = str(item['fund_code']).zfill(6)
        score = item['评分']
        curr_p = item['price']
        
        # 匹配历史最后一次买入价格
        if not hist_df.empty:
            match = hist_df[hist_df['fund_code'].astype(str).str.zfill(6) == code]
            if not match.empty:
                last_p = match.iloc[-1]['price']
                change = (curr_p - last_p) / last_p * 100
                
                if change <= GRID_GAP:
                    item['建议'] = "🔥 网格补仓"
                elif change >= TAKE_PROFIT:
                    item['建议'] = "💰 止盈/分批出"
                else:
                    item['建议'] = "⏳ 锁仓观察"
            else:
                item['建议'] = "🌱 首笔建仓" if score >= 4 else "🔭 持续观察"
        else:
            item['建议'] = "🌱 首笔建仓" if score >= 4 else "🔭 持续观察"
        final_results.append(item)
    return final_results

def update_readme(advice_res):
    now_bj = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    content = f"# 📊 基金网格加仓雷达\n\n> 最后更新: `{now_bj}` | 策略：网格分批布局\n\n"
    
    # 底部共振判断
    if len([x for x in advice_res if x['评分'] >= 4]) >= 5:
        content += "> 🚨 **底部共振预警**：当前大量基金进入评分4+区域，说明市场处于大级别底部，网格补仓胜率极高。\n\n"

    content += "## 🎯 今日实战建议\n"
    if advice_res:
        content += pd.DataFrame(advice_res).sort_values('评分', ascending=False).to_markdown(index=False) + "\n\n"
    
    content += "## 💡 网格执行手册\n"
    content += "* **首笔建仓**: 评分 >= 4 且无历史记录时。  \n"
    content += "* **网格补仓**: 现价比上次入场价跌超 5%。  \n"
    content += "* **锁仓观察**: 价格在波动区间内，不触发买卖。  \n"
    
    with open('README.md', 'w', encoding='utf-8') as f:
        f.write(content)

def main():
    files = glob.glob('fund_data/*.csv')
    with Pool(cpu_count()) as p:
        results = [r for r in p.map(process_file, files) if r is not None]
    
    if results:
        # 存档今日信号
        now = datetime.now()
        folder = now.strftime('%Y/%m')
        os.makedirs(folder, exist_ok=True)
        pd.DataFrame(results).to_csv(f"{folder}/fund_sig_{now.strftime('%d_%H%M%S')}.csv", index=False)
        
        # 获取建议并更新看板
        advice_res = get_performance_and_advice(results)
        update_readme(advice_res)

if __name__ == "__main__":
    main()
