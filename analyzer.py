import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime
import pytz

# ================================
#         配置参数（集中管理）
# ================================
FUND_DATA_DIR = 'fund_data'

# 预警双重筛选
MIN_CONSECUTIVE_DROP_DAYS = 3       # 30日内连续下跌天数阈值
MIN_MONTH_DRAWDOWN = 0.06           # 1个月最大回撤阈值 (6%)

# 高弹性策略核心参数
HIGH_ELASTICITY_MIN_DRAWDOWN = 0.10 # 长期超跌阈值 (10%)
MIN_DAILY_DROP_PERCENT = 0.03       # 当日跌幅触发阈值 (3%)
TRIAL_DROP_RATE = 0.03              # 试水买价下跌比例

# RSI 超卖阈值
RSI_OVERSOLD = 30.0                 # 极度超卖
RSI_STRONG_OVERSOLD = 35.0          # 超卖区

REPORT_BASE_NAME = 'fund_warning_report'

# ================================
#       核心函数：技术指标计算
# ================================
def calculate_technical_indicators(df):
    """
    计算 RSI(14)、MACD、MA50、布林带位置、当日跌幅
    df 必须包含 'value' 列，且按日期降序排列
    """
    if 'value' not in df.columns or len(df) < 50:
        return {
            'RSI': np.nan, 'MACD信号': '数据不足', '净值/MA50': np.nan,
            '布林带位置': '数据不足', '最新净值': np.nan, '当日跌幅': np.nan
        }

    df_asc = df.iloc[::-1].copy()  # 升序用于技术分析

    # 1. RSI(14)
    delta = df_asc['value'].diff()
    gain = delta.where(delta > 0, 0).rolling(window=14).mean()
    loss = -delta.where(delta < 0, 0).rolling(window=14).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    rsi_latest = rsi.iloc[-1]

    # 2. MACD
    ema12 = df_asc['value'].ewm(span=12, adjust=False).mean()
    ema26 = df_asc['value'].ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()

    macd_latest = macd.iloc[-1]
    signal_latest = signal.iloc[-1]
    macd_prev = macd.iloc[-2] if len(df_asc) >= 2 else np.nan
    signal_prev = signal.iloc[-1] if len(df_asc) >= 2 else np.nan

    macd_signal = '观察'
    if not np.isnan(macd_prev) and not np.isnan(signal_prev):
        if macd_latest > signal_latest and macd_prev < signal_prev:
            macd_signal = '金叉'
        elif macd_latest < signal_latest and macd_prev > signal_prev:
            macd_signal = '死叉'

    # 3. MA50
    ma50 = df_asc['value'].rolling(window=50).mean()
    ma50_latest = ma50.iloc[-1]
    value_latest = df_asc['value'].iloc[-1]
    net_to_ma50 = value_latest / ma50_latest if ma50_latest and ma50_latest != 0 else np.nan

    # 4. 布林带
    ma20 = df_asc['value'].rolling(window=20).mean()
    std20 = df_asc['value'].rolling(window=20).std()
    upper = ma20 + 2 * std20
    lower = ma20 - 2 * std20

    bollinger_pos = '数据不足'
    if not pd.isna(ma20.iloc[-1]) and not pd.isna(std20.iloc[-1]):
        if value_latest > upper.iloc[-1]:
            bollinger_pos = '上轨上方'
        elif value_latest < lower.iloc[-1]:
            bollinger_pos = '下轨下方'
        elif value_latest > ma20.iloc[-1]:
            bollinger_pos = '中轨上方'
        else:
            bollinger_pos = '中轨下方/中轨'

    # 5. 当日跌幅
    daily_drop = 0.0
    if len(df_asc) >= 2:
        prev = df_asc['value'].iloc[-2]
        if prev > 0:
            daily_drop = (prev - value_latest) / prev

    return {
        'RSI': round(rsi_latest, 2) if not np.isnan(rsi_latest) else np.nan,
        'MACD信号': macd_signal,
        '净值/MA50': round(net_to_ma50, 2) if not np.isnan(net_to_ma50) else np.nan,
        '布林带位置': bollinger_pos,
        '最新净值': round(value_latest, 4),
        '当日跌幅': round(daily_drop, 4)
    }


# ================================
#       高效连跌天数计算
# ================================
def calculate_consecutive_drops(series):
    """计算 series 中最长连续下跌天数（降序排列）"""
    if len(series) < 2:
        return 0
    drops = (series.iloc[1:].values < series.iloc[:-1].values)
    if not drops.any():
        return 0
    # 计算连续 True 的长度
    diff = np.diff(np.where(np.concatenate(([False], drops, [False])))[0])
    return (diff[::2]).max()


# ================================
#       最大回撤计算
# ================================
def calculate_max_drawdown(series):
    if series.empty:
        return 0.0
    peak = series.cummax()
    drawdown = (peak - series) / peak
    return drawdown.max()


# ================================
#       报告生成（三优先级 + 行动提示）
# ================================
def generate_report(results, timestamp_str):
    now_str = timestamp_str
    if not results:
        return (
            f"# 基金预警报告 ({now_str})\n\n"
            f"## 分析总结\n\n"
            f"**恭喜！过去30个交易日内，无基金同时满足 '连跌{MIN_CONSECUTIVE_DROP_DAYS}天+' 和 '回撤{MIN_MONTH_DRAWDOWN*100:.0f}%+' 的双重预警条件。**\n\n"
            f"---\n"
            f"数据时间范围：最近30个交易日"
        )

    df = pd.DataFrame(results)
    df = df.sort_values(by='最大回撤', ascending=False).reset_index(drop=True)
    df.index = df.index + 1
    total = len(df)

    report = f"# 基金预警报告 ({now_str})\n\n"
    report += f"## 分析总结\n\n"
    report += f"共发现 **{total}** 只基金满足双重预警条件（近30日）：\n"
    report += f"- 连续下跌 **≥ {MIN_CONSECUTIVE_DROP_DAYS}** 天\n"
    report += f"- 最大回撤 **≥ {MIN_MONTH_DRAWDOWN*100:.0f}%**\n\n"
    report += f"**新增：高弹性三层建仓体系 + 技术指标 + 当日跌幅触发**\n"
    report += f"---\n"

    # 高弹性基础池：回撤≥10% 且 近一周连跌==1（低位企稳）
    df_elastic = df[
        (df['最大回撤'] >= HIGH_ELASTICITY_MIN_DRAWDOWN) &
        (df['近一周连跌'] == 1)
    ].copy()

    # 超卖池：RSI < 35
    df_oversold = df_elastic[df_elastic['RSI'] < RSI_STRONG_OVERSOLD].copy()

    # 🥇 第一优先级：RSI超卖 + 当日大跌
    df_priority1 = df_oversold[df_oversold['当日跌幅'] >= MIN_DAILY_DROP_PERCENT].copy()
    if not df_priority1.empty:
        df_priority1 = df_priority1.sort_values(by=['当日跌幅', 'RSI'], ascending=[False, True]).reset_index(drop=True)
        df_priority1.index = df_priority1.index + 1
        report += f"\n## **🥇 第一优先级：即时恐慌买入** ({len(df_priority1)}只)\n\n"
        report += f"**条件：** 超跌≥{HIGH_ELASTICITY_MIN_DRAWDOWN*100:.0f}% + 低位企稳 + RSI<{RSI_STRONG_OVERSOLD} + **当日跌幅≥{MIN_DAILY_DROP_PERCENT*100:.0f}%**\n"
        report += f"**纪律：** 市场恐慌时果断出手，按跌幅排序\n\n"
        report += f"| 排名 | 基金代码 | 最大回撤 | 当日跌幅 | 连跌(30日) | RSI | MACD | 净值/MA50 | 试水买价 | 行动 |\n"
        report += f"| :---: | :---: | ---: | ---: | ---: | ---: | :---: | ---: | :---: | :---: |\n"
        for idx, row in df_priority1.iterrows():
            price = row['最新净值'] * (1 - TRIAL_DROP_RATE)
            action = '极度超卖+大跌' if row['RSI'] < RSI_OVERSOLD else '超卖+大跌'
            report += f"| {idx} | `{row['基金代码']}` | **{row['最大回撤']:.2%}** | **{row['当日跌幅']:.2%}** | {row['最大连续下跌']} | **{row['RSI']:.1f}** | {row['MACD信号']} | {row['净值/MA50']:.2f} | {price:.4f} | **买入 {action}** |\n"
        report += "\n---\n"
    else:
        report += f"\n## **🥇 第一优先级：即时恐慌买入**\n\n**今日无大跌触发，暂无恐慌买入机会**\n\n---\n"

    # 🥈 第二优先级：超卖但未大跌
    codes1 = df_priority1['基金代码'].tolist() if not df_priority1.empty else []
    df_priority2 = df_oversold[~df_oversold['基金代码'].isin(codes1)].copy()
    if not df_priority2.empty:
        df_priority2 = df_priority2.sort_values(by=['RSI', '最大回撤'], ascending=[True, False]).reset_index(drop=True)
        df_priority2.index = df_priority2.index + 1
        report += f"\n## **🥈 第二优先级：技术共振建仓** ({len(df_priority2)}只)\n\n"
        report += f"**条件：** 超跌≥{HIGH_ELASTICITY_MIN_DRAWDOWN*100:.0f}% + RSI<{RSI_STRONG_OVERSOLD} + 当日跌幅<{MIN_DAILY_DROP_PERCENT*100:.0f}%\n"
        report += f"**纪律：** 按RSI排序，分批建仓\n\n"
        report += f"| 排名 | 基金代码 | 最大回撤 | 当日跌幅 | 连跌(30日) | RSI | MACD | 净值/MA50 | 试水买价 | 行动 |\n"
        report += f"| :---: | :---: | ---: | ---: | ---: | ---: | :---: | ---: | :---: | :---: |\n"
        for idx, row in df_priority2.iterrows():
            price = row['最新净值'] * (1 - TRIAL_DROP_RATE)
            report += f"| {idx} | `{row['基金代码']}` | **{row['最大回撤']:.2%}** | {row['当日跌幅']:.2%} | {row['最大连续下跌']} | **{row['RSI']:.1f}** | {row['MACD信号']} | {row['净值/MA50']:.2f} | {price:.4f} | 试水建仓 |\n"
        report += "\n---\n"
    else:
        report += f"\n## **🥈 第二优先级：技术共振建仓**\n\n**全部进入第一优先级**\n\n---\n"

    # 🥉 第三优先级：超跌但未超卖
    codes2 = df_oversold['基金代码'].tolist()
    df_priority3 = df_elastic[~df_elastic['基金代码'].isin(codes2)].copy()
    if not df_priority3.empty:
        df_priority3 = df_priority3.sort_values(by='最大回撤', ascending=False).reset_index(drop=True)
        df_priority3.index = df_priority3.index + 1
        report += f"\n## **🥉 第三优先级：扩展观察池** ({len(df_priority3)}只)\n\n"
        report += f"**条件：** 超跌≥{HIGH_ELASTICITY_MIN_DRAWDOWN*100:.0f}% + 低位企稳 + RSI≥{RSI_STRONG_OVERSOLD}\n"
        report += f"**纪律：** 等待RSI进入超卖区\n\n"
        report += f"| 排名 | 基金代码 | 最大回撤 | 当日跌幅 | 连跌(30日) | RSI | MACD | 净值/MA50 | 试水买价 | 行动 |\n"
        report += f"| :---: | :---: | ---: | ---: | ---: | ---: | :---: | ---: | :---: | :---: |\n"
        for idx, row in df_priority3.iterrows():
            price = row['最新净值'] * (1 - TRIAL_DROP_RATE)
            report += f"| {idx} | `{row['基金代码']}` | **{row['最大回撤']:.2%}** | {row['当日跌幅']:.2%} | {row['最大连续下跌']} | {row['RSI']:.1f} | {row['MACD信号']} | {row['净值/MA50']:.2f} | {price:.4f} | 观察 |\n"
        report += "\n---\n"

    # 所有预警基金
    report += f"\n## 所有预警基金 ({total}只，按回撤排序)\n\n"
    report += f"| 排名 | 基金代码 | 最大回撤 | 当日跌幅 | 连跌(30日) | 连跌(5日) | RSI | MACD | 净值/MA50 | 布林带 |\n"
    report += f"| :---: | :---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :---: |\n"
    for idx, row in df.iterrows():
        rsi_str = f"{row['RSI']:.1f}" if pd.notna(row['RSI']) else "NaN"
        ma_str = f"{row['净值/MA50']:.2f}" if pd.notna(row['净值/MA50']) else "NaN"
        report += f"| {idx} | `{row['基金代码']}` | **{row['最大回撤']:.2%}** | {row['当日跌幅']:.2%} | {row['最大连续下跌']} | {row['近一周连跌']} | {rsi_str} | {row['MACD信号']} | {ma_str} | {row['布林带位置']} |\n"
    report += "\n---\n"
    report += f"数据时间范围：最近30个交易日\n"

    # 交易纪律
    report += f"\n## **高弹性交易纪律**\n\n"
    report += f"1. **建仓**：仅在 🥇 列表出手，🥈 分批，🥉 观察\n"
    report += f"2. **加仓**：试水后跌5% + RSI<20 → 最大加仓\n"
    report += f"3. **止盈**：MACD金叉 + 盈利5% → 减半\n"
    report += f"4. **止损**：成本跌8% → 立即清仓\n"

    return report


# ================================
#       主分析函数
# ================================
def analyze_all_funds():
    csv_files = glob.glob(os.path.join(FUND_DATA_DIR, '*.csv'))
    if not csv_files:
        print(f"警告：目录 '{FUND_DATA_DIR}' 中无CSV文件")
        return []

    print(f"发现 {len(csv_files)} 个基金，开始分析...")
    results = []

    for i, filepath in enumerate(csv_files, 1):
        fund_code = os.path.splitext(os.path.basename(filepath))[0]
        try:
            df = pd.read_csv(filepath)
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date', ascending=False).reset_index(drop=True)
            df = df.rename(columns={'net_value': 'value'})

            if len(df) < 50:
                continue

            month = df.head(30)
            week = df.head(5)

            drop_month = calculate_consecutive_drops(month['value'])
            mdd = calculate_max_drawdown(month['value'])
            drop_week = calculate_consecutive_drops(week['value'])

            tech = calculate_technical_indicators(df)

            if drop_month >= MIN_CONSECUTIVE_DROP_DAYS and mdd >= MIN_MONTH_DRAWDOWN:
                results.append({
                    '基金代码': fund_code,
                    '最大回撤': mdd,
                    '最大连续下跌': drop_month,
                    '近一周连跌': drop_week,
                    'RSI': tech['RSI'],
                    'MACD信号': tech['MACD信号'],
                    '净值/MA50': tech['净值/MA50'],
                    '布林带 Statue': tech['布林带位置'],
                    '最新净值': tech['最新净值'],
                    '当日跌幅': tech['当日跌幅']
                })

            if i % 20 == 0:
                print(f"  已处理 {i}/{len(csv_files)} ...")

        except Exception as e:
            print(f"错误处理 {fund_code}: {e}")
            continue

    print(f"分析完成，符合预警基金：{len(results)} 只")
    if results:
        max_dd = max(r['最大回撤'] for r in results)
        code = next(r['基金代码'] for r in results if r['最大回撤'] == max_dd)
        print(f"  最大回撤冠军：{code} ({max_dd:.2%})")
    return results


# ================================
#            主程序入口
# ================================
if __name__ == '__main__':
    # 时间处理
    try:
        tz = pytz.timezone('Asia/Shanghai')
        now = datetime.now(tz)
    except:
        now = datetime.now()
    timestamp_report = now.strftime('%Y-%m-%d %H:%M:%S')
    timestamp_file = now.strftime('%Y%m%d_%H%M%S')
    dir_name = now.strftime('%Y%m')

    os.makedirs(dir_name, exist_ok=True)
    report_file = os.path.join(dir_name, f"{REPORT_BASE_NAME}_{timestamp_file}.md")

    print("开始生成基金预警报告...")
    results = analyze_all_funds()
    report = generate_report(results, timestamp_report)

    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(report)

    print(f"报告已生成：{report_file}")