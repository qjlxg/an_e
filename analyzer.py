import pandas as pd
import glob
import os
import numpy as np
import requests

# --- 配置参数 ---
FUND_DATA_DIR = 'fund_data'
MIN_CONSECUTIVE_DROP_DAYS = 3
MIN_MONTH_DRAWDOWN = 0.06
HIGH_ELASTICITY_MIN_DRAWDOWN = 0.10
MIN_DAILY_DROP_PERCENT = 0.03
REPORT_BASE_NAME = 'fund_warning_report'


# --- 修复：API 请求 + 降级处理 ---
def get_fund_info(fund_code):
    """从天天基金API获取经理在任年数 + 基金规模（亿元），失败时返回默认值"""
    url = f"http://api.fund.eastmoney.com/pinzhong/LSPZ?fundcode={fund_code}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'http://fund.eastmoney.com/'
    }
    try:
        response = requests.get(url, timeout=6, headers=headers)
        if response.status_code != 200:
            return 0.0, 0.0
        data = response.json()
        if data.get('Datas') and data['Datas']:
            info = data['Datas'][0]
            tenure = float(info.get('MANAGER_TENURE', 0))
            scale = float(info.get('FUND_SCALE', 0))
            return tenure, scale
    except:
        pass
    return 0.0, 0.0  # 任何异常都返回0，避免阻塞


# --- 计算技术指标 ---
def calculate_technical_indicators(df):
    if 'value' not in df.columns or len(df) < 50:
        return {
            'RSI': np.nan, 'MACD信号': '数据不足', '净值/MA50': np.nan,
            '布林带位置': '数据不足', '最新净值': df['value'].iloc[0] if not df.empty else np.nan,
            '当日跌幅': np.nan
        }

    df_asc = df.iloc[::-1].copy()
    delta = df_asc['value'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss.replace(0, np.nan)
    df_asc['RSI'] = 100 - (100 / (1 + rs))
    rsi_latest = df_asc['RSI'].iloc[-1]

    ema_12 = df_asc['value'].ewm(span=12, adjust=False).mean()
    ema_26 = df_asc['value'].ewm(span=26, adjust=False).mean()
    df_asc['MACD'] = ema_12 - ema_26
    df_asc['Signal'] = df_asc['MACD'].ewm(span=9, adjust=False).mean()
    macd_latest, signal_latest = df_asc['MACD'].iloc[-1], df_asc['Signal'].iloc[-1]
    macd_prev, signal_prev = df_asc['MACD'].iloc[-2], df_asc['Signal'].iloc[-2]

    macd_signal = ('金叉' if macd_latest > signal_latest and macd_prev < signal_prev else
                   '死叉' if macd_latest < signal_latest and macd_prev > signal_prev else '观察')

    df_asc['MA50'] = df_asc['value'].rolling(window=50).mean()
    ma50_latest = df_asc['MA50'].iloc[-1]
    value_latest = df_asc['value'].iloc[-1]
    net_to_ma50 = value_latest / ma50_latest if ma50_latest else np.nan

    df_asc['MA20'] = df_asc['value'].rolling(window=20).mean()
    df_asc['StdDev'] = df_asc['value'].rolling(window=20).std()
    upper = df_asc['MA20'].iloc[-1] + 2 * df_asc['StdDev'].iloc[-1]
    lower = df_asc['MA20'].iloc[-1] - 2 * df_asc['StdDev'].iloc[-1]
    bollinger_pos = ('上轨上方' if value_latest > upper else
                     '下轨下方' if value_latest < lower else
                     '中轨上方' if value_latest > df_asc['MA20'].iloc[-1] else '中轨下方/中轨')

    daily_drop = ((df_asc['value'].iloc[-2] - value_latest) / df_asc['value'].iloc[-2]
                  if len(df_asc) >= 2 and df_asc['value'].iloc[-2] > 0 else 0.0)

    return {
        'RSI': round(rsi_latest, 2),
        'MACD信号': macd_signal,
        '净值/MA50': round(net_to_ma50, 2),
        '布林带位置': bollinger_pos,
        '最新净值': round(value_latest, 4),
        '当日跌幅': round(daily_drop, 4)
    }


# --- 辅助函数 ---
def extract_fund_codes(report_content):
    codes = set()
    for line in report_content.split('\n'):
        if line.count('|') >= 8 and '买入信号' in line:
            parts = [p.strip() for p in line.split('|')]
            if len(parts) >= 11 and parts[2].isdigit():
                codes.add(parts[2])
    return list(codes)

def calculate_consecutive_drops(series):
    if len(series) < 2: return 0
    drops = (series < series.shift(1)).iloc[1:].astype(int)
    max_drop = current = 0
    for d in drops:
        current = current + 1 if d else 0
        max_drop = max(max_drop, current)
    return max_drop

def calculate_max_drawdown(series):
    if series.empty: return 0.0
    return ((series.cummax() - series) / series.cummax()).max()


# --- 生成报告（已全部使用 rf"" 消除 SyntaxWarning）---
def generate_report(results, timestamp_str):
    if not results:
        return (f"# 基金预警报告 ({timestamp_str} UTC+8)\n\n"
                f"## 分析总结\n\n"
                f"**恭喜，无符合条件基金**\n\n---\n")

    df_results = pd.DataFrame(results).sort_values(by='最大回撤', ascending=False)
    df_results.index = range(1, len(df_results) + 1)
    total_count = len(df_results)

    report = f"# 基金预警报告 ({timestamp_str} UTC+8)\n\n"
    report += f"## 分析总结\n\n"
    report += f"共 **{total_count}** 只基金满足条件\n"
    report += f"**已通过基本面红线过滤**\n---\n"

    # 高弹性基础
    df_base_elastic = df_results[
        (df_results['最大回撤'] >= HIGH_ELASTICITY_MIN_DRAWDOWN) &
        (df_results['近一周连跌'] == 1) &
        (df_results['RSI'] < 35)
    ].copy()

    # 第一优先级：恐慌买入
    df_buy1 = df_base_elastic[df_base_elastic['当日跌幅'] >= MIN_DAILY_DROP_PERCENT].copy()
    if not df_buy1.empty:
        df_buy1 = df_buy1.sort_values(by=['当日跌幅', 'RSI'], ascending=[False, True])
        df_buy1.index = range(1, len(df_buy1) + 1)
        report += f"\n## **第一优先级：【即时恐慌买入】** ({len(df_buy1)}只)\n\n"
        report += rf"**条件：** 长期超跌 ($\ge$ {HIGH_ELASTICITY_MIN_DRAWDOWN*100:.0f}%) + RSI < 35 + **当日跌幅 $\ge$ {MIN_DAILY_DROP_PERCENT*100:.0f}%**\n\n"
        report += f"| 排名 | 代码 | 回撤 | **跌幅** | 连跌 | RSI | MACD | MA50 | 试水价 | 提示 |\n"
        report += f"| :---: | :---: | ---: | ---: | ---: | ---: | :---: | ---: | :---: | :---: |\n"
        for i, r in df_buy1.iterrows():
            price = r['最新净值'] * 0.97
            prompt = '极度超卖+大跌' if r['RSI'] < 30 else '超卖+大跌'
            report += f"| {i} | `{r['基金代码']}` | **{r['最大回撤']:.2%}** | **{r['当日跌幅']:.2%}** | {r['最大连续下跌']} | {r['RSI']:.1f} | {r['MACD信号']} | {r['净值/MA50']:.2f} | {price:.4f} | **{prompt}** |\n"
        report += "\n---\n"
    else:
        report += f"\n## **第一优先级**\n\n**今日无恐慌买入信号**\n\n---\n"

    # 第二优先级
    df_buy2 = df_base_elastic[~df_base_elastic['基金代码'].isin(df_buy1['基金代码'])].copy()
    if not df_buy2.empty:
        df_buy2 = df_buy2.sort_values(by=['RSI', '最大回撤'], ascending=[True, False])
        df_buy2.index = range(1, len(df_buy2) + 1)
        report += f"\n## **第二优先级：【技术共振建仓】** ({len(df_buy2)}只)\n\n"
        report += rf"**条件：** 长期超跌 + RSI < 35 + **当日跌幅 $< {MIN_DAILY_DROP_PERCENT*100:.0f}\%$**\n\n"
        report += f"| 排名 | 代码 | 回撤 | 跌幅 | 连跌 | RSI | MACD | MA50 | 试水价 | 提示 |\n"
        report += f"| :---: | :---: | ---: | ---: | ---: | ---: | :---: | ---: | :---: |\n"
        for i, r in df_buy2.iterrows():
            price = r['最新净值'] * 0.97
            report += f"| {i} | `{r['基金代码']}` | **{r['最大回撤']:.2%}** | {r['当日跌幅']:.2%} | {r['最大连续下跌']} | **{r['RSI']:.1f}** | {r['MACD信号']} | {r['净值/MA50']:.2f} | {price:.4f} | **{r['行动提示']}** |\n"
        report += "\n---\n"

    # 第三优先级
    df_ext = df_results[
        (df_results['最大回撤'] >= HIGH_ELASTICITY_MIN_DRAWDOWN) &
        (df_results['近一周连跌'] == 1) &
        (df_results['RSI'] >= 35)
    ].copy()
    if not df_ext.empty:
        df_ext = df_ext.sort_values(by='最大回撤', ascending=False)
        df_ext.index = range(1, len(df_ext) + 1)
        report += f"\n## **第三优先级：【扩展观察池】** ({len(df_ext)}只)\n\n"
        report += rf"**条件：** 长期超跌 + RSI $\ge 35$\n\n"
        report += f"| 排名 | 代码 | 回撤 | 跌幅 | 连跌 | RSI | MACD | MA50 | 试水价 | 提示 |\n"
        report += f"| :---: | :---: | ---: | ---: | ---: | ---: | :---: | ---: | :---: |\n"
        for i, r in df_ext.iterrows():
            price = r['最新净值'] * 0.97
            report += f"| {i} | `{r['基金代码']}` | **{r['最大回撤']:.2%}** | {r['当日跌幅']:.2%} | {r['最大连续下跌']} | {r['RSI']:.1f} | {r['MACD信号']} | {r['净值/MA50']:.2f} | {price:.4f} | {r['行动提示']} |\n"
        report += "\n---\n"

    # 所有预警
    report += f"\n## 所有预警基金 ({total_count}只)\n\n"
    report += f"| 排名 | 代码 | 回撤 | 跌幅 | 连跌(1M) | 连跌(1W) | RSI | MACD | MA50 | 布林带 |\n"
    report += f"| :---: | :---: | ---: | ---: | ---: | ---: | ---: | :---: | ---: | :---: |\n"
    for i, r in df_results.iterrows():
        report += f"| {i} | `{r['基金代码']}` | **{r['最大回撤']:.2%}** | {r['当日跌幅']:.2%} | {r['最大连续下跌']} | {r['近一周连跌']} | {r['RSI']:.1f} | {r['MACD信号']} | {r['净值/MA50']:.2f} | {r['布林带位置']} |\n"

    report += "\n---\n"
    report += f"## 执行纪律\n\n"
    report += f"**建仓**：第一优先级 → 2000元；第二优先级 → 1000元试水\n"
    report += f"**止盈**：MACD金叉 + 浮盈5% → 卖50%\n"
    report += f"**止损**：-10% → 全清仓\n"

    return report


# --- 主分析函数 ---
def analyze_all_funds(target_codes=None):
    if target_codes:
        csv_files = [os.path.join(FUND_DATA_DIR, f'{c}.csv') for c in target_codes]
        csv_files = [f for f in csv_files if os.path.exists(f)]
        # 兼容持仓_015456_2024.csv
        extra = [f for f in glob.glob(os.path.join(FUND_DATA_DIR, '持仓_*.csv'))
                 if any(c in f for c in target_codes)]
        csv_files = list(set(csv_files + extra))
    else:
        csv_files = glob.glob(os.path.join(FUND_DATA_DIR, '*.csv'))

    if not csv_files:
        return []

    qualifying_funds = []
    for filepath in csv_files:
        try:
            base_name = os.path.splitext(os.path.basename(filepath))[0]
            fund_code = ''.join(filter(str.isdigit, base_name))
            if not fund_code:
                continue

            # 基本面过滤
            tenure, scale = get_fund_info(fund_code)
            if tenure < 2.0 or scale < 2.0:
                print(f"基本面过滤：{fund_code} (经理{tenure:.1f}年, 规模{scale:.1f}亿) → 跳过")
                continue

            df = pd.read_csv(filepath)
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date', ascending=False).reset_index(drop=True)
            df = df.rename(columns={'net_value': 'value'})
            if len(df) < 30:
                continue

            df_m = df.head(30)
            df_w = df.head(5)
            drop_m = calculate_consecutive_drops(df_m['value'])
            mdd_m = calculate_max_drawdown(df_m['value'])
            drop_w = calculate_consecutive_drops(df_w['value'])

            tech = calculate_technical_indicators(df)
            rsi = tech['RSI']
            drop_today = tech['当日跌幅']

            action = '不适用'
            if mdd_m >= HIGH_ELASTICITY_MIN_DRAWDOWN and drop_w == 1:
                if rsi < 30 and drop_today >= MIN_DAILY_DROP_PERCENT:
                    action = '买入信号 (极度超卖+大跌)'
                elif rsi < 35 and drop_today >= MIN_DAILY_DROP_PERCENT:
                    action = '买入信号 (超卖+大跌)'
                elif rsi < 35:
                    action = '考虑试水建仓 (RSI超卖)'
                else:
                    action = '高回撤观察 (RSI未超卖)'

            if drop_m >= MIN_CONSECUTIVE_DROP_DAYS and mdd_m >= MIN_MONTH_DRAWDOWN:
                qualifying_funds.append({
                    '基金代码': fund_code,
                    '最大回撤': mdd_m,
                    '最大连续下跌': drop_m,
                    '近一周连跌': drop_w,
                    'RSI': tech['RSI'],
                    'MACD信号': tech['MACD信号'],
                    '净值/MA50': tech['净值/MA50'],
                    '布林带位置': tech['布林带位置'],
                    '最新净值': tech['最新净值'],
                    '当日跌幅': drop_today,
                    '行动提示': action
                })
        except Exception as e:
            print(f"错误 {filepath}: {e}")
            continue
    return qualifying_funds


# --- 主程序 ---
if __name__ == '__main__':
    now = pd.Timestamp.now(tz='Asia/Shanghai')
    ts_report = now.strftime('%Y-%m-%d %H:%M:%S')
    ts_file = now.strftime('%Y%m%d_%H%M%S')
    DIR_NAME = now.strftime('%Y%m')
    os.makedirs(DIR_NAME, exist_ok=True)
    REPORT_FILE = os.path.join(DIR_NAME, f"{REPORT_BASE_NAME}_{ts_file}.md")

    try:
        with open('market_monitor_report.md', 'r', encoding='utf-8') as f:
            target_funds = extract_fund_codes(f.read())
        print(f"提取 {len(target_funds)} 个目标基金")
    except:
        target_funds = None
        print("无目标基金，分析全部")

    results = analyze_all_funds(target_codes=target_funds)
    report = generate_report(results, ts_report)

    with open(REPORT_FILE, 'w', encoding='utf-8') as f:
        f.write(report)

    print(f"报告已生成：{REPORT_FILE}")
