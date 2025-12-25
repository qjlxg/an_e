import os
import glob
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from multiprocessing import Pool, cpu_count

# ==========================================
# --- 核心锁死风控与策略参数 ---
# ==========================================
TOTAL_BUDGET_CAP = 10000   # 1万本金总上限
PORTFOLIO_UNIT = 2000      # 单笔抄底金额
STOP_BUY_LOSS_RATIO = -5.0 # 组合总亏损超5%，开启禁买令
RETR_WATCH = -10.0         # 回撤阈值锁定为 -20.0
RETR_WINDOW = 250          # 250日实战周期
RSI_LOW = 30           
BIAS_LOW = -5.0        

# ==========================================
# --- 映射逻辑：加载 ETF 名称 ---
# ==========================================
def load_name_mapping():
    mapping = {}
    try:
        # 读取上传的 ETF列表.txt，匹配证券代码和简称
        if os.path.exists('ETF列表.txt'):
            df_map = pd.read_csv('ETF列表.txt', sep='\t', dtype={'证券代码': str})
            for _, row in df_map.iterrows():
                code = str(row['证券代码']).zfill(6)
                mapping[code] = row['证券简称']
    except Exception as e:
        print(f"名称映射加载失败: {e}")
    return mapping

NAME_MAP = load_name_mapping()

# ==========================================
# --- 技术指标计算模块 ---
# ==========================================
def calculate_rsi(series, period=6):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs.fillna(0)))

def check_rsi_divergence(df, window=20):
    """
    RSI底背离检测：价格创出window日内新低，但RSI未创新低且显著回升
    """
    if len(df) < window + 5: return False
    curr_price = df['收盘'].iloc[-1]
    curr_rsi = df['rsi'].iloc[-1]
    
    lookback_df = df.iloc[-(window+1):-1]
    min_price_idx = lookback_df['收盘'].idxmin()
    min_price_val = lookback_df['收盘'].min()
    min_price_rsi = lookback_df.loc[min_price_idx, 'rsi']
    
    # 底背离：价格创新低，但RSI比低点时抬升超过2个单位
    if curr_price <= min_price_val and curr_rsi > min_price_rsi + 2:
        return True
    return False

# ==========================================
# --- 单文件处理 (含数据清洗) ---
# ==========================================
def process_file(file_path):
    try:
        try: df = pd.read_csv(file_path, encoding='utf-8')
        except: df = pd.read_csv(file_path, encoding='gbk')
        
        if 'net_value' in df.columns:
            df = df.rename(columns={'date': '日期', 'net_value': '收盘'})
        df['日期'] = pd.to_datetime(df['日期'])
        df = df.sort_values(by='日期').reset_index(drop=True)
        
        # --- 增强数据清洗：拦截净值异常跳变为1.0的情况 ---
        if len(df) < 30: return None
        curr_p = df['收盘'].iloc[-1]
        prev_p = df['收盘'].iloc[-2]
        # 如果最新价是1.0，但昨天价格远高于1.1，判定为数据源错误（缺失填充），直接剔除
        if curr_p == 1.0 and prev_p > 1.1: return None
        
        # 计算指标
        df['rsi'] = calculate_rsi(df['收盘'], 6)
        df['ma6'] = df['收盘'].rolling(window=6).mean()
        df['bias'] = ((df['收盘'] - df['ma6']) / df['ma6']) * 100
        df['max_high'] = df['收盘'].rolling(window=RETR_WINDOW).max()
        df['retr'] = ((df['收盘'] - df['max_high']) / df['max_high']) * 100
        
        df['in_watch'] = df['retr'] <= RETR_WATCH
        df['persist_days'] = df['in_watch'].groupby((df['in_watch'] != df['in_watch'].shift()).cumsum()).cumcount() + 1
        df.loc[~df['in_watch'], 'persist_days'] = 0

        curr = df.iloc[-1]
        code = os.path.splitext(os.path.basename(file_path))[0].zfill(6)
        name = NAME_MAP.get(code, "未知品种")
        
        if curr['in_watch']:
            score = 1
            divergence = check_rsi_divergence(df)
            if curr['rsi'] < RSI_LOW: score += 2
            if curr['bias'] < BIAS_LOW: score += 2
            if divergence: score += 2  # 背离加分
            
            risk_level = "正常"
            if divergence: risk_level = "📈底背离形成"
            if curr['rsi'] > 55 and score == 1: risk_level = "🚩高风险(陷阱)"
            elif score >= 5: risk_level = "🔥极高胜率(背离)"
            elif score >= 3: risk_level = "✅高胜率区"
                
            return {
                'date': str(curr['日期']).split(' ')[0],
                'fund_code': code,
                '名称': name,
                '评分': score,
                '持续天数': int(curr['persist_days']),
                '风险预警': risk_level,
                '回撤%': round(curr['retr'], 2),
                'RSI': round(curr['rsi'], 2),
                'BIAS': round(curr['bias'], 2),
                'price': round(curr['收盘'], 4)
            }
    except: return None

# ==========================================
# --- 盈亏统计与追踪 ---
# ==========================================
def get_performance_stats():
    history_files = glob.glob('202*/**/*.csv', recursive=True)
    perf_list = []
    for h_file in history_files:
        if 'perf' in h_file: continue
        try:
            h_df = pd.read_csv(h_file)
            for _, sig in h_df.iterrows():
                code = str(sig['fund_code']).zfill(6)
                raw_path = f'fund_data/{code}.csv'
                if not os.path.exists(raw_path): continue
                raw_df = pd.read_csv(raw_path)
                if 'net_value' in raw_df.columns: raw_df = raw_df.rename(columns={'date': '日期', 'net_value': '收盘'})
                raw_df['日期'] = pd.to_datetime(raw_df['日期']).dt.strftime('%Y-%m-%d')
                
                idx_list = raw_df[raw_df['日期'] == str(sig['date'])].index
                if not idx_list.empty:
                    curr_idx = idx_list[0]
                    signal_price = sig['price']
                    latest_price = raw_df.iloc[-1]['收盘']
                    
                    # 统计端清洗：如果最新价还是异常的1.0，跳过不计入总盈亏计算
                    if latest_price == 1.0 and signal_price > 1.1: continue
                    
                    prev_price = raw_df.iloc[-2]['收盘'] if len(raw_df) > 1 else latest_price
                    daily_raw = (latest_price - prev_price) / prev_price * 100
                    color_tag = "🔴 " if daily_raw > 0 else "🟢 " if daily_raw < 0 else ""
                    daily_display = f"{color_tag}{daily_raw:+.2f}%"
                    total_hold_change = (latest_price - signal_price) / signal_price * 100
                    
                    recovery_df = raw_df.iloc[curr_idx+1:]
                    back_days = "未回本"
                    back_idx = recovery_df[recovery_df['收盘'] >= signal_price].index
                    if not back_idx.empty: back_days = int(back_idx[0] - curr_idx)
                    
                    perf_list.append({
                        '日期': sig['date'], '代码': code, '名称': NAME_MAP.get(code, "未知"),
                        '评分': sig.get('评分', 1), '信号价': round(signal_price, 4),
                        '最新价': round(latest_price, 4), '今日涨跌': daily_display,
                        '总盈亏%': round(total_hold_change, 2), '回本天数': back_days,
                        '状态': "✅反弹中" if total_hold_change > 1 else "❌走弱" if total_hold_change < -3 else "⏳磨底中"
                    })
        except: continue
    return pd.DataFrame(perf_list)

# ==========================================
# --- 报告生成 (北京时间版) ---
# ==========================================
def update_readme(current_res, perf_df):
    now_utc = datetime.now(timezone.utc)
    now_bj = (now_utc + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
    
    content = f"# 🤖 ETF/基金 策略雷达 (250日实战锁死版)\n\n> 最后更新: `{now_bj}` (北京时间)\n\n"
    
    total_invested = 0
    total_profit_loss_val = 0
    avg_return_rate = 0
    is_budget_full = False
    is_panic_mode = False

    if not perf_df.empty:
        # 只追踪最近14天触发信号的活跃买点进行实战统计
        recent_limit = (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d')
        active_focus = perf_df[(perf_df['评分'] >= 3) & (perf_df['日期'] >= recent_limit)].drop_duplicates(subset=['代码'])
        
        if not active_focus.empty:
            total_invested = len(active_focus) * PORTFOLIO_UNIT
            total_profit_loss_val = (active_focus['总盈亏%'] / 100 * PORTFOLIO_UNIT).sum()
            avg_return_rate = (total_profit_loss_val / total_invested) * 100 if total_invested > 0 else 0
            is_budget_full = total_invested >= TOTAL_BUDGET_CAP
            is_panic_mode = avg_return_rate <= STOP_BUY_LOSS_RATIO

            content += "## 💰 实战风控盘口 (1万资金上限)\n"
            content += f"> **模拟投入**: `¥{total_invested} / ¥{TOTAL_BUDGET_CAP}` | **总盈亏**: `{'🔴' if total_profit_loss_val > 0 else '🟢'} ¥{total_profit_loss_val:.2f} ({avg_return_rate:+.2f}%)` \n"
            status_desc = "🛡️ 预算内" if not is_budget_full else "⛔ 预算满员"
            if is_panic_mode: status_desc += " | ❌ 禁买令 (总亏损过大)"
            content += f"> **风控状态**: `{status_desc}`\n\n"

    content += "## 🎯 实时信号监控 (-20%阈值 + 底背离检测)\n"
    if current_res:
        df = pd.DataFrame(current_res).sort_values(['评分', '回撤%'], ascending=[False, True])
        def decide(row):
            if row['评分'] < 3: return "等待3分"
            if is_budget_full: return "⛔ 预算上限"
            if is_panic_mode: return "❌ 组合亏损(停买)"
            return "✅ 可分批建仓"
        df['建议'] = df.apply(decide, axis=1)
        # 调整列顺序
        cols = ['date', 'fund_code', '名称', '评分', '持续天数', '风险预警', '回撤%', 'RSI', 'BIAS', 'price', '建议']
        content += df[cols].to_markdown(index=False) + "\n\n"
    else:
        content += "> 💤 当前无触发 -20% 阈值的信号。\n\n"

    content += "## 🔥 活跃买点追踪 (已清洗异常1.0数据)\n"
    if not perf_df.empty:
        recent_limit = (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d')
        active_focus = perf_df[(perf_df['评分'] >= 3) & (perf_df['日期'] >= recent_limit)].sort_values('日期', ascending=False).drop_duplicates(subset=['代码'])
        if not active_focus.empty:
            cols = ['日期', '代码', '名称', '评分', '信号价', '最新价', '今日涨跌', '总盈亏%', '状态', '回本天数']
            content += active_focus[cols].to_markdown(index=False) + "\n\n"

    with open('README.md', 'w', encoding='utf-8') as f: f.write(content)

# ==========================================
# --- 主程序 ---
# ==========================================
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
