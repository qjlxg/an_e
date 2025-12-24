import os
import glob
import pandas as pd
from datetime import datetime
from multiprocessing import Pool, cpu_count

# --- 策略参数 ---
RSI_THRESHOLD = 30
BIAS_THRESHOLD = -4.0
RETRENCHMENT_LIMIT = -10.0  # 高位回撤起码10%
VOL_RATIO_LIMIT = 1.5       # 成交量放大 1.5 倍

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

        # 格式自适应 (场内 vs 场外)
        if 'net_value' in df.columns:
            df = df.rename(columns={'date': '日期', 'net_value': '收盘'})
            df['日期'] = pd.to_datetime(df['日期'])
            df = df.sort_values(by='日期', ascending=True).reset_index(drop=True)
            # 场外基金没有成交量，设为1以跳过放量逻辑
            df['成交量'] = 1 
        else:
            # 适配场内 ETF 列名
            df = df.rename(columns={'成交量': 'vol'})
            if 'vol' not in df.columns: df['vol'] = 1
            df['成交量'] = df['vol']

        if '收盘' not in df.columns or len(df) < 30: return None

        # 1. 计算指标
        df['rsi'] = calculate_rsi(df['收盘'], 6)
        df['ma6'] = df['收盘'].rolling(window=6).mean()
        df['bias'] = ((df['收盘'] - df['ma6']) / df['ma6']) * 100
        
        # 2. 计算回撤 (30日高点)
        window_max = df['收盘'].rolling(window=30).max()
        df['retrenchment'] = ((df['收盘'] - window_max) / window_max) * 100
        
        # 3. 计算成交量放大比率 (当前量 / 5日均量)
        df['vol_ma5'] = df['成交量'].rolling(window=5).mean()
        df['vol_ratio'] = df['成交量'] / df['vol_ma5']
        
        latest = df.iloc[-1]
        code = os.path.splitext(os.path.basename(file_path))[0]
        
        # 条件打标
        is_rsi_low = latest['rsi'] < RSI_THRESHOLD
        is_bias_low = latest['bias'] < BIAS_THRESHOLD
        is_drop_enough = latest['retrenchment'] <= RETRENCHMENT_LIMIT
        is_vol_burst = latest['vol_ratio'] >= VOL_RATIO_LIMIT if latest['vol_ratio'] > 0 else False

        # 汇总：只要满足任何一个超跌指标且回撤够大就记录
        if (is_rsi_low or is_bias_low) and is_drop_enough:
            tags = []
            if is_rsi_low: tags.append("RSI超卖")
            if is_bias_low: tags.append("BIAS负乖离")
            if is_vol_burst: tags.append("🔥放量")
            
            return {
                '日期': str(latest['日期']).split(' ')[0],
                '代码': code,
                '价格': round(latest['收盘'], 4),
                '回撤%': round(latest['retrenchment'], 2),
                'RSI': round(latest['rsi'], 2),
                'BIAS': round(latest['bias'], 2),
                '量比': round(latest['vol_ratio'], 2) if latest['vol_ratio'] > 1 else "--",
                '满足信号': " | ".join(tags)
            }
    except: return None
    return None

def update_readme(results):
    now_bj = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    md_content = f"# 🤖 ETF/基金 多维度自动监控看板\n\n"
    md_content += f"> 更新时间: `{now_bj}` | **核心准则：回撤 > 10% 才有抄底价值**\n\n"
    
    if results:
        df_res = pd.DataFrame(results)
        # 排序：优先展示放量的，然后是回撤最大的
        df_res['burst'] = df_res['满足信号'].str.contains("🔥").astype(int)
        df_res = df_res.sort_values(by=['burst', '回撤%'], ascending=[False, True])
        
        md_content += "### 🎯 实时筛选清单\n"
        md_content += df_res.drop(columns=['burst']).to_markdown(index=False) + "\n\n"
        md_content += "> **提示**：标注 🔥 的品种代表成交量异常放大，反转概率更高。\n"
    else:
        md_content += "### 🎯 实时筛选清单\n✅ **当前暂无满足“高位回撤>10%”且“技术指标见底”的品种。**\n"

    md_content += "\n---\n### 📊 筛选标准说明\n"
    md_content += "1. **回撤%**: 当前价格较近30个交易日最高点的跌幅。\n"
    md_content += "2. **RSI(6)**: 低于 30 进入超卖区。\n"
    md_content += "3. **BIAS(6)**: 乖离率低于 -4% 意味着短线超跌。\n"
    md_content += "4. **量比**: 大于 1.5 意味着今日成交量超过过去5日均值的50%。\n"
    
    with open('README.md', 'w', encoding='utf-8') as f:
        f.write(md_content)

def main():
    data_dir = 'fund_data'
    if not os.path.exists(data_dir): return
    files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith('.csv')]
    with Pool(cpu_count()) as p:
        results = [r for r in p.map(process_file, files) if r is not None]
    update_readme(results)

if __name__ == "__main__":
    main()
