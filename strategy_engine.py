import os
import glob
import pandas as pd
from datetime import datetime
from multiprocessing import Pool, cpu_count

# --- 阈值设置 ---
RSI_LOW = 30
BIAS_LOW = -4.0
RETR_WATCH = -10.0  # 回撤10%进入雷达
VOL_BURST = 1.5    # 1.5倍放量

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

        # 计算核心指标
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
            return {
                '代码': code,
                '价格': round(curr['收盘'], 4),
                '回撤%': round(curr['retr'], 2),
                'RSI': round(curr['rsi'], 2),
                'BIAS': round(curr['bias'], 2),
                '量比': round(curr['v_ratio'], 2) if curr['v_ratio'] > 0 else "--",
                '信号': f"{'RSI' if curr['rsi']<RSI_LOW else ''} {'BIAS' if curr['bias']<BIAS_LOW else ''} {'🔥' if (not is_otc and curr['v_ratio']>VOL_BURST) else ''}".strip()
            }
    except: return None
    return None

def update_readme(all_data):
    now_bj = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    content = f"# 🤖 ETF/基金 阶梯式抄底雷达\n\n> 更新时间: `{now_bj}`\n\n"
    
    if not all_data:
        content += "### 🎯 实时信号\n✅ 市场整体估值尚可，暂无大幅回撤品种。\n"
    else:
        df = pd.DataFrame(all_data)
        
        # 1. 强力共振区 (两个指标都见底 + 放量)
        strong = df[df['信号'].str.contains('RSI') & df['信号'].str.contains('BIAS')]
        content += "### 🔴 第一梯队：技术面见底 (RSI & BIAS 共振)\n"
        content += strong.to_markdown(index=False) if not strong.empty else "*暂无品种进入强力共振区*\n"
        
        # 2. 放量关注区 (有超跌信号且放量)
        burst = df[df['信号'].str.contains('🔥')]
        content += "\n### 🟠 第二梯队：异动放量区 (恐慌盘/接盘盘)\n"
        content += burst.to_markdown(index=False) if not burst.empty else "*暂无异常放量品种*\n"
        
        # 3. 基础雷达区 (所有回撤>10%的品种)
        content += "\n### 🔵 第三梯队：高位回撤池 (跌幅 > 10%)\n"
        content += df.sort_values('回撤%').head(15).to_markdown(index=False)

    content += "\n\n---\n**逻辑说明**：\n- **第一梯队**：短线情绪与价格乖离同时到达极值，反弹概率高。\n- **第二梯队**：放量代表多空分歧加大，往往是变盘信号。\n- **第三梯队**：仅展示回撤深度，作为中长期观察名单。"
    
    with open('README.md', 'w', encoding='utf-8') as f:
        f.write(content)

def main():
    files = glob.glob('fund_data/*.csv')
    with Pool(cpu_count()) as p:
        results = [r for r in p.map(process_file, files) if r is not None]
    update_readme(results)

if __name__ == "__main__":
    main()
