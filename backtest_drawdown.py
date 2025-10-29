#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
回测模块：滚动30天窗口回测所有基金
核心修改：
1. 根据持有天数（d）应用阶梯式交易成本。
2. 成本在止损和最终收益计算后扣除，确保收益为“净收益”。
【重要修正】：修复了交易成本在止损和正常退出时的准确扣除逻辑。
【本次修正】：将信号生成逻辑与实盘预警脚本 (analyzer.py) 保持一致，移除 '当日跌幅' 限制，采用 RSI 阶梯式判断。
"""
import os
import glob
import pandas as pd
import numpy as np
from datetime import datetime
import pytz
import yaml
import logging
from concurrent.futures import ProcessPoolExecutor
from typing import List, Dict, Any
import warnings

warnings.filterwarnings("ignore")

# ================================
# 日志配置
# ================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ================================
# 加载配置
# ================================
CONFIG_PATH = "config_backtest.yaml"
try:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
except FileNotFoundError:
    logger.warning("未找到 config_backtest.yaml，使用默认配置。请确保在实际运行时提供此文件。")
    cfg = {
        "data_dir": 'fund_data',
        "output_dir": 'backtest_results',
        "forward_days": [1, 3, 5, 10, 20],
        "benchmark_hold_days": 20,
        "min_history_days": 60,
        "max_workers": 4,
        "thresholds": {
            "min_consecutive_drop_days": 3,
            "min_month_drawdown": 0.06,
            "high_elasticity_min_drawdown": 0.10,
            "min_daily_drop_percent": 0.03,
            "rsi_extreme_oversold": 30,
            "rsi_oversold": 35,
            "stop_loss_threshold": 0.10
        }
    }

DATA_DIR = cfg["data_dir"]
OUTPUT_ROOT = cfg["output_dir"]
START_DATE = cfg.get("start_date")
END_DATE = cfg.get("end_date")
TH = cfg["thresholds"]
FORWARD_DAYS = cfg["forward_days"]
BENCH_DAYS = cfg["benchmark_hold_days"]
MIN_HISTORY = cfg["min_history_days"]
MAX_WORKERS = cfg.get("max_workers", 4)

# ================================
# 核心指标函数 (省略，与原脚本一致)
# ================================
def calculate_consecutive_drops(series: pd.Series) -> int:
    """从最新一天开始连续下跌天数（包含今天）。series 为降序（最新在前）。"""
    if len(series) < 2:
        return 0
    values = series.values
    drops = values[1:] < values[:-1]
    count = 0
    for d in drops:
        if d:
            count += 1
        else:
            break
    return count


def calculate_max_drawdown(series: pd.Series) -> float:
    """计算最大回撤。series 按日期升序排列。"""
    if series.empty:
        return 0.0
    peak = series.cummax()
    drawdown = (peak - series) / peak
    return drawdown.max()


def calculate_technical_indicators(df: pd.DataFrame) -> Dict[str, Any]:
    """计算 RSI、MACD、MA50、布林带、当日跌幅。df 按日期降序排列（最新在前）。"""
    if 'value' not in df.columns or len(df) < 50:
        return {
            'RSI': np.nan, 'MACD信号': '数据不足', '净值/MA50': np.nan,
            '布林带位置': '数据不足', '最新净值': np.nan, '当日跌幅': np.nan
        }

    df_asc = df.iloc[::-1].copy()  # 转为升序
    value_series = df_asc['value']

    # RSI (14)
    delta = value_series.diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean()
    loss = -delta.where(delta < 0, 0).rolling(14).mean()
    loss_safe = loss.replace(0, np.nan)
    rs = gain / loss_safe
    rsi_series = 100 - (100 / (1 + rs)).fillna(100)
    rsi_latest = rsi_series.iloc[-1]

    # MACD
    ema12 = value_series.ewm(span=12, adjust=False).mean()
    ema26 = value_series.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    macd_latest = macd.iloc[-1]
    signal_latest = signal.iloc[-1]
    macd_prev = macd.iloc[-2] if len(macd) >= 2 else np.nan
    signal_prev = signal.iloc[-2] if len(signal) >= 2 else np.nan

    macd_signal = '观察'
    if not np.isnan(macd_prev) and not np.isnan(signal_prev):
        if macd_latest > signal_latest and macd_prev <= signal_prev:
            macd_signal = '金叉'
        elif macd_latest < signal_latest and macd_prev >= signal_prev:
            macd_signal = '死叉'

    # MA50
    ma50_latest = value_series.rolling(50).mean().iloc[-1]
    value_latest = value_series.iloc[-1]
    net_to_ma50 = value_latest / ma50_latest if ma50_latest != 0 else np.nan

    # 布林带
    ma20 = value_series.rolling(20).mean()
    std20 = value_series.rolling(20).std()
    ma20_latest = ma20.iloc[-1]
    std_latest = std20.iloc[-1]
    bollinger_pos = '数据不足'
    if not np.isnan(ma20_latest) and not np.isnan(std_latest) and std_latest > 0:
        upper = ma20_latest + 2 * std_latest
        lower = ma20_latest - 2 * std_latest
        if value_latest > upper:
            bollinger_pos = '上轨上方'
        elif value_latest < lower:
            bollinger_pos = '下轨下方'
        elif value_latest > ma20_latest:
            bollinger_pos = '中轨上方'
        else:
            bollinger_pos = '中轨下方/中轨'

    # 当日跌幅
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
# 修正位置：与 analyzer.py 保持一致
# ================================
def generate_signal(row: pd.Series) -> str:
    """生成信号等级，使用纯RSI阶梯判断，与实盘预警脚本一致。"""
    # 必须满足基础回撤和连跌条件
    if (row['最大回撤'] >= TH["high_elasticity_min_drawdown"] and
        row['近一周连跌'] == 1 and
        not pd.isna(row['RSI'])):
        
        # 第一优先级：极度超卖 (RSI < 30)
        if row['RSI'] < TH.get("rsi_extreme_oversold", 30):
            return "第一优先级 即时买入"
        
        # 第二优先级：超卖 (RSI < 35, 且前面未满足)
        if row['RSI'] < TH.get("rsi_oversold", 35):
            return "第二优先级 技术建仓"

        # 第三优先级：观察（满足高回撤、连跌，但RSI >= 35）
        return "第三优先级 观察池"
        
    return "无信号"
# ================================
# 单基金回测（修正：止损后扣成本，不放大亏损） (省略，与原脚本一致)
# ================================
def backtest_single_fund(filepath: str) -> List[Dict]:
    fund_code = os.path.splitext(os.path.basename(filepath))[0]
    try:
        df = pd.read_csv(filepath, parse_dates=['date'])
        if 'net_value' in df.columns:
            df = df.rename(columns={'net_value': 'value'})
        elif 'value' not in df.columns:
            logger.warning(f"{fund_code} 无净值列")
            return []

        df = df[['date', 'value']].dropna().sort_values('date').reset_index(drop=True)
        if len(df) < MIN_HISTORY:
            return []

        if START_DATE:
            df = df[df['date'] >= pd.to_datetime(START_DATE)]
        if END_DATE:
            df = df[df['date'] <= pd.to_datetime(END_DATE)]
        if len(df) < 60:
            return []

        records = []
        SL_THRESHOLD = TH.get("stop_loss_threshold", 0.10)
        COST_7_DAYS_OR_LESS = 0.015  # 1.5%
        COST_OVER_7_DAYS = 0.005     # 0.5%

        for i in range(30, len(df)):
            window_df = df.iloc[i-30:i].copy()
            week_df = window_df.iloc[-5:].copy()

            window_desc = window_df.iloc[::-1]['value']
            week_desc = week_df.iloc[::-1]['value']

            max_drop_month = calculate_consecutive_drops(window_desc)
            mdd_month = calculate_max_drawdown(window_df['value'])
            tech = calculate_technical_indicators(df.iloc[:i].iloc[::-1])
            max_drop_week = calculate_consecutive_drops(week_desc)

            if (max_drop_month >= TH["min_consecutive_drop_days"] and
                mdd_month >= TH["min_month_drawdown"]):

                row = {
                    '基金代码': fund_code,
                    '日期': df.iloc[i-1]['date'].strftime('%Y-%m-%d'),
                    '最新净值': tech['最新净值'],
                    '最大回撤': mdd_month,
                    '最大连续下跌': max_drop_month,
                    '近一周连跌': max_drop_week,
                    'RSI': tech['RSI'],
                    '当日跌幅': tech['当日跌幅'],
                    '止损退出天数': np.nan
                }
                row['信号'] = generate_signal(pd.Series(row)) # 调用新的信号生成函数

                base_price = df.iloc[i-1]['value']
                max_hold_days = max(FORWARD_DAYS)
                future_series = df.iloc[i: i + max_hold_days + 1]['value']
                returns_from_entry = (future_series - base_price) / base_price

                for d in FORWARD_DAYS:
                    col = f'未来{d}日收益'
                    if len(returns_from_entry) < d + 1:
                        row[col] = np.nan
                        continue

                    hold_period_returns = returns_from_entry.head(d)
                    cost = COST_7_DAYS_OR_LESS if d <= 7 else COST_OVER_7_DAYS

                    min_return_in_period = hold_period_returns.min()
                    
                    # --- 修正后的核心逻辑：计算净收益 ---
                    if min_return_in_period <= -SL_THRESHOLD:
                        # 触发止损：毛收益被限制在 -SL_THRESHOLD，净收益需扣除成本
                        final_return = -SL_THRESHOLD - cost
                        
                        if pd.isna(row['止损退出天数']):
                            # 记录首次触及止损的天数
                            sl_hit_idx = (returns_from_entry <= -SL_THRESHOLD).idxmax()
                            sl_days = sl_hit_idx - i + 1
                            row['止损退出天数'] = sl_days
                    else:
                        # 正常持有到期：毛收益为到期时的收益，净收益需扣除成本
                        final_return = hold_period_returns.iloc[-1] - cost
                    # ----------------------------------------
                        
                    row[col] = final_return

                records.append(row)
        return records

    except Exception as e:
        logger.error(f"回测 {fund_code} 失败: {e}")
        return []


# ================================
# 主流程 (省略，与原脚本一致)
# ================================
def run_backtest():
    os.makedirs(OUTPUT_ROOT, exist_ok=True)
    tz = pytz.timezone('Asia/Shanghai')
    now = datetime.now(tz)
    yyyymm = now.strftime('%Y%m')
    out_dir = os.path.join(OUTPUT_ROOT, yyyymm)
    os.makedirs(out_dir, exist_ok=True)
    timestamp = now.strftime('%Y%m%d_%H%M%S')
    summary_path = os.path.join(out_dir, f"backtest_summary_{timestamp}.csv")
    report_path = os.path.join(out_dir, f"backtest_report_{timestamp}.md")

    all_csv_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    csv_files = [f for f in all_csv_files if '持仓_' not in os.path.basename(f)]

    initial_count = len(all_csv_files)
    filtered_count = len(csv_files)
    if initial_count > filtered_count:
        logger.info(f"已过滤 {initial_count - filtered_count} 个包含 '持仓_' 的文件。")

    # === 调试模式：限制为前 5 个基金 ===
    DEBUG_LIMIT = 600
    if csv_files:
        total_files = len(csv_files)
        csv_files = csv_files[:DEBUG_LIMIT]
        logger.info(f"【调试模式】仅回测前 {len(csv_files)} 只基金（共 {total_files} 只）")

    logger.info(f"开始回测 {len(csv_files)} 只基金...")

    all_records = []
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(backtest_single_fund, f) for f in csv_files]
        for future in futures:
            try:
                all_records.extend(future.result())
            except Exception as e:
                logger.error(f"进程执行失败: {e}")

    if not all_records:
        logger.info("无符合条件的回测信号")
        return

    df_all = pd.DataFrame(all_records)
    df_all.to_csv(summary_path, index=False, encoding='utf-8-sig')
    logger.info(f"详细回测数据已保存：{summary_path}")

    # 生成 Markdown 报告
    signal_groups = df_all.groupby('信号')
    SL_THRESHOLD_PCT = TH.get("stop_loss_threshold", 0.10) * 100
    report_lines = [
        f"# 基金预警策略回测报告\n",
        f"**生成时间**：{now.strftime('%Y-%m-%d %H:%M:%S')} (UTC+8)\n",
        f"**回测范围**：{START_DATE or '全部'} ~ {END_DATE or '全部'}\n",
        f"**基金数量**：{len(set(df_all['基金代码']))}，**信号总数**：{len(df_all)}\n",
        f"**核心风控纪律**：所有收益已模拟 {SL_THRESHOLD_PCT:.0f}% 严格止损退出。\n",
        f"**交易成本**：持有 $\\le 7$ 天扣除 $1.5\\%$；持有 $> 7$ 天扣除 $0.5\\%$。\n\n",
        "---\n"
    ]

    COST_7_DAYS_OR_LESS = 0.015
    COST_OVER_7_DAYS = 0.005

    for signal_name, group in signal_groups:
        if signal_name == "无信号":
            continue
        report_lines.append(f"## {signal_name}\n")
        report_lines.append(f"**出现次数**：{len(group)}\n")
        stats = []

        for d in FORWARD_DAYS:
            col = f'未来{d}日收益'
            valid = group[col].dropna()
            if len(valid) == 0:
                continue

            # 统计止损次数：检查净收益是否低于止损点（即：跌幅达到止损阈值 + 成本）
            cost = COST_7_DAYS_OR_LESS if d <= 7 else COST_OVER_7_DAYS
            stop_loss_net = -TH.get("stop_loss_threshold", 0.10) - cost
            # 1e-6 用于浮点数比较的安全裕度
            stop_loss_count = (valid <= stop_loss_net + 1e-6).sum() 

            win_rate = (valid > 0).mean()
            avg_ret = valid.mean()
            median_ret = valid.median()
            max_ret = valid.max()
            min_ret = valid.min()

            stats.append({
                '持有天数': d,
                '胜率': win_rate,
                '平均收益': avg_ret,
                '中位数收益': median_ret,
                '最大收益': max_ret,
                '最小收益': min_ret,
                '止损次数': stop_loss_count,
                '样本数': len(valid)
            })

        if stats:
            df_stats = pd.DataFrame(stats)
            report_lines.append("\n### 净收益分布 (已应用止损和交易成本)\n")
            report_lines.append("| 持有天数 | 胜率 | 平均收益 | 中位数 | 最大 | **止损次数** | 样本 |\n")
            report_lines.append("| :---: | :---: | :---: | :---: | :---: | :---: | :---: |\n")
            for _, r in df_stats.iterrows():
                report_lines.append(
                    f"| {r['持有天数']} | {r['胜率']:.1%} | **{r['平均收益']:.2%}** | "
                    f"{r['中位数收益']:.2%} | {r['最大收益']:.2%} | **{r['止损次数']}** | {r['样本数']} |\n"
                )
            report_lines.append("\n")

        bench_col = f'未来{BENCH_DAYS}日收益'
        if bench_col in group.columns:
            bench_valid = group[bench_col].dropna()
            if len(bench_valid) > 0:
                bench_ret = bench_valid.mean()
                report_lines.append(f"**基准持有 {BENCH_DAYS} 天平均收益**：{bench_ret:.2%}\n\n")

        report_lines.append("---\n")

    with open(report_path, "w", encoding="utf-8") as f:
        f.writelines(report_lines)
    logger.info(f"回测报告已生成：{report_path}")


if __name__ == "__main__":
    run_backtest()
