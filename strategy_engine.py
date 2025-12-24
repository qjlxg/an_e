import os
import glob
import pandas as pd
from datetime import datetime
from multiprocessing import Pool, cpu_count

# --- 基金定投/分批加仓参数 ---
RETR_LEVEL_1 = -10.0  # 初次建仓观察线
RETR_LEVEL_2 = -15.0  # 重点加仓线
RSI_BOTTOM = 35       # 情绪低位区
STOP_LOSS_VAL = -8.0  # 基金容忍度较高，设为-8%作为极端风险提示
TAKE_PROFIT_VAL = 3.0 # 基金波动小，目标设为3%的反弹

def calculate_rsi(series, period=12): # 基金波动缓，RSI周期拉长更准
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    return 100 - (100 / (1 + (gain / loss)))

def process_file(file_path):
    try:
        try: df = pd.read_csv(file_path, encoding='utf-8')
        except: df = pd.read_csv(file_path, encoding='gbk')
        
        # 兼容场外基金(net_value)和场内基金(收盘)
        if 'net_value' in df.columns:
            df = df.rename(columns={'date': '日期', 'net_value': '收盘'})
            df['日期'] = pd.to_datetime(df['日期'])
            df = df.sort_values(by='日期', ascending=True).reset_index(drop=True)
        else:
            df['日期'] = pd.to_datetime(df['日期'])

        if len(df) < 60: return None # 基金需要更长的数据周期
        
        # 计算核心加仓指标
        df['rsi'] = calculate_rsi(df['收盘'], 12)
        df['ma20'] = df['收盘'].rolling(window=20).mean() # 基金看20日线
        df['bias'] = ((df['收盘'] - df['ma20']) / df['ma20']) * 100
        df['max_high'] = df['收盘'].rolling(window=60).max() # 季度高点回撤
        df['retr'] = ((df['收盘'] - df['max_high']) / df['max_high']) * 100

        curr = df.iloc[-1]
        code = os.path.splitext(os.path.basename(file_path))[0]
        
        # 逻辑：回撤达标才进入雷达
        if curr['retr'] <= RETR_LEVEL_1:
            score = 1
            if curr['retr'] <= RETR_LEVEL_2: score += 2  # 深跌加分
            if curr['rsi'] < RSI_BOTTOM: score += 2      # 超卖加分
            if curr['bias'] < -5: score += 1             # 乖离率加分
            
            return {
                'date': str(curr['日期']).split(' ')[0],
                'fund_code': code,
                'price': round(curr['收盘'], 4),
                '回撤%': round(curr['retr'], 2),
                'RSI': round(curr['rsi'], 2),
                '评分': score,
                '行动建议': "💎分批重仓" if score >= 4 else "🌱小量试仓" if score >= 2 else "🔭持续观察"
            }
    except: return None

def get_performance():
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
                    if 'net_value' in raw_df.columns: raw_df = raw_df.rename(columns={'date':'日期','net_value':'收盘'})
                    raw_df['日期'] = pd.to_datetime(raw_df['日期']).dt.strftime('%Y-%m-%d')
                    idx = raw_df[raw_df['日期'] == str(sig['date'])].index
                    if not idx.empty:
                        # 基金复盘周期拉长至5-10天看趋势
                        future = raw_df.iloc[idx[0]+1 : idx[0]+11] 
                        if not future.empty:
                            max_u = (future['收盘'].max() - sig['price']) / sig['price'] * 100
                            max_d = (future['收盘'].min() - sig['price']) / sig['price'] * 100
                            
                            if max_d <= STOP_LOSS_VAL: status = "💀跌破位"
                            elif max_u >= TAKE_PROFIT_VAL: status = "✅反弹中"
                            else: status = "⏳横盘/磨底"
                            
                            perf_list.append({
                                '日期': sig['date'], '代码': code,
                                '周期最高%': round(max_u, 2), '期间最深%': round(max_d, 2),
                                '评分': sig.get('评分', 1), '结果': status
                            })
        except: continue
    return pd.DataFrame(perf_list)

def update_readme(current_res, perf_df):
    now_bj = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    content = f"# 📊 基金布局雷达 (分批加仓实战版)\n\n> 核心理念：左侧交易，分批建仓，等待回归。 更新：`{now_bj}`\n\n"
    
    if not perf_df.empty:
        total = len(perf_df)
        success_rate = len(perf_df[perf_df['结果'] == '✅反弹中']) / total * 100
        content += "## 📈 策略回测总结\n"
        content += f"| 累计信号 | 反弹成功率 (目标3%) | 优质底部占比 |\n| :--- | :--- | :--- |\n| {total} | {success_rate:.1f}% | {len(perf_df[perf_df['评分']>=4])/total*100:.1f}% |\n\n"

    content += "## 🎯 今日分批加仓雷达\n"
    if current_res:
        content += pd.DataFrame(current_res).sort_values('评分', ascending=False).to_markdown(index=False) + "\n\n"
    
    content += "## 📑 历史定投点效果追踪\n"
    if not perf_df.empty:
        content += perf_df.tail(20).iloc[::-1].to_markdown(index=False)
    
    with open('README.md', 'w', encoding='utf-8') as f: f.write(content)

def main():
    files = glob.glob('fund_data/*.csv')
    with Pool(cpu_count()) as p:
        results = [r for r in p.map(process_file, files) if r is not None]
    if results:
        now = datetime.now(); folder = now.strftime('%Y/%m')
        os.makedirs(folder, exist_ok=True)
        pd.DataFrame(results).to_csv(f"{folder}/fund_sig_{now.strftime('%d_%H%M%S')}.csv", index=False)
    perf_df = get_performance()
    update_readme(results, perf_df)

if __name__ == "__main__": main()
