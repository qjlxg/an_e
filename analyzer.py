import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime
import pytz
import logging
import math

# --- 配置参数 ---
FUND_DATA_DIR = 'fund_data'
MIN_CONSECUTIVE_DROP_DAYS = 3
MIN_MONTH_DRAWDOWN = 0.06
HIGH_ELASTICITY_MIN_DRAWDOWN = 0.10  # 高弹性策略的基础回撤要求 (10%)
MIN_DAILY_DROP_PERCENT = 0.03  # 当日大跌的定义 (3%)
REPORT_BASE_NAME = 'fund_warning_report'

# --- 核心阈值调整 ---
# RSI 超卖极值收紧至 29
EXTREME_RSI_THRESHOLD = 29.0

# --- 设置日志 ---
def setup_logging():
    """设置日志配置"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('fund_analysis.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

def validate_fund_data(df, fund_code):
    """
    验证基金数据的完整性和质量
    
    Args:
        df: 基金数据DataFrame
        fund_code: 基金代码
        
    Returns:
        tuple: (是否有效, 错误信息)
    """
    if df.empty:
        return False, "数据为空"
    
    if 'value' not in df.columns:
        return False, "缺少净值列"
    
    if len(df) < 250:
        return False, f"数据不足250条，当前只有{len(df)}条"
    
    # 检查净值合理性
    if (df['value'] <= 0).any():
        return False, "存在无效净值(<=0)"
    
    return True, "数据有效"

def calculate_technical_indicators(df):
    """
    计算基金净值的完整技术指标 (RSI, MACD, MA, 趋势等)
    
    Args:
        df: 基金数据DataFrame，按日期降序排列
        
    Returns:
        dict: 包含所有技术指标的字典
    """
    # 确保最新值在最后
    df_asc = df.iloc[::-1].copy().reset_index(drop=True)

    try:
        # 数据验证
        if 'value' not in df_asc.columns or len(df_asc) < 250:
             # 返回默认值
            return {
                'RSI': np.nan, 'MACD信号': '数据不足', '净值/MA50': np.nan,
                '净值/MA250': np.nan, 'MA50/MA250': np.nan, 
                'MA50/MA250趋势': '数据不足',
                '布林带位置': '数据不足', '最新净值': df_asc['value'].iloc[-1] if not df_asc.empty else np.nan,
                '当日跌幅': np.nan
            }

        # 1. RSI (14)
        delta = df_asc['value'].diff()
        # 涨幅和跌幅
        gain = (delta.where(delta > 0, 0)).rolling(window=14, min_periods=1).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14, min_periods=1).mean()
        rs = gain / loss.replace(0, np.nan) 
        df_asc['RSI'] = 100 - (100 / (1 + rs))
        rsi_latest = df_asc['RSI'].iloc[-1]

        # 2. MACD (简化为信号判断)
        ema_12 = df_asc['value'].ewm(span=12, adjust=False).mean()
        ema_26 = df_asc['value'].ewm(span=26, adjust=False).mean()
        df_asc['MACD'] = ema_12 - ema_26
        df_asc['Signal'] = df_asc['MACD'].ewm(span=9, adjust=False).mean()
        
        macd_latest = df_asc['MACD'].iloc[-1]
        signal_latest = df_asc['Signal'].iloc[-1]
        macd_prev = df_asc['MACD'].iloc[-2] if len(df_asc) >= 2 else np.nan
        signal_prev = df_asc['Signal'].iloc[-2] if len(df_asc) >= 2 else np.nan

        macd_signal = '观察'
        if not np.isnan(macd_prev) and not np.isnan(signal_prev):
            if macd_latest > signal_latest and macd_prev < signal_prev:
                macd_signal = '金叉'
            elif macd_latest < signal_latest and macd_prev > signal_prev:
                macd_signal = '死叉'

        # 3. 移动平均线和趋势分析
        df_asc['MA50'] = df_asc['value'].rolling(window=50, min_periods=1).mean()
        df_asc['MA250'] = df_asc['value'].rolling(window=250, min_periods=1).mean()
        
        ma50_latest = df_asc['MA50'].iloc[-1]
        ma250_latest = df_asc['MA250'].iloc[-1]
        value_latest = df_asc['value'].iloc[-1]

        # 计算比值
        net_to_ma50 = value_latest / ma50_latest if ma50_latest and ma50_latest != 0 else np.nan
        net_to_ma250 = value_latest / ma250_latest if ma250_latest and ma250_latest != 0 else np.nan
        ma50_to_ma250 = ma50_latest / ma250_latest if ma250_latest and ma250_latest != 0 else np.nan


        # 4. MA50/MA250 趋势方向判断
        trend_direction = '数据不足'
        if len(df_asc) >= 250:
            recent_ratio = (df_asc['MA50'] / df_asc['MA250']).tail(20).dropna()
            if len(recent_ratio) >= 5:
                # 拟合斜率
                slope = np.polyfit(np.arange(len(recent_ratio)), recent_ratio.values, 1)[0]
                
                if slope > 0.001:
                    trend_direction = '向上'
                elif slope < -0.001:
                    trend_direction = '向下'
                else:
                    trend_direction = '平稳'
        
        # 5. 布林带位置 (简化为 N/A)
        bollinger_pos = 'N/A'

        # 6. 当日跌幅 (最新一天跌幅)
        daily_drop = 0.0
        if len(df_asc) >= 2:
            value_t_minus_1 = df_asc['value'].iloc[-2]
            if value_t_minus_1 > 0:
                daily_drop = (value_t_minus_1 - value_latest) / value_t_minus_1

        return {
            'RSI': round(rsi_latest, 2) if not math.isnan(rsi_latest) else np.nan,
            'MACD信号': macd_signal,
            '净值/MA50': round(net_to_ma50, 2) if not math.isnan(net_to_ma50) else np.nan,
            '净值/MA250': round(net_to_ma250, 2) if not math.isnan(net_to_ma250) else np.nan, 
            'MA50/MA250': round(ma50_to_ma250, 2) if not math.isnan(ma50_to_ma250) else np.nan, 
            'MA50/MA250趋势': trend_direction,
            '布林带位置': bollinger_pos,
            '最新净值': round(value_latest, 4) if not math.isnan(value_latest) else np.nan,
            '当日跌幅': round(daily_drop, 4)
        }

    except Exception as e:
        logging.error(f"计算技术指标时发生错误: {e}")
        return {
            'RSI': np.nan, 'MACD信号': '计算错误', '净值/MA50': np.nan,
            '净值/MA250': np.nan, 'MA50/MA250': np.nan, 
            'MA50/MA250趋势': '计算错误',
            '布林带位置': '计算错误', '最新净值': np.nan,
            '当日跌幅': np.nan
        }

def calculate_consecutive_drops(series):
    """计算净值序列中最大的连续下跌天数 (降序序列，最新在前)"""
    try:
        if series.empty or len(series) < 2:
            return 0
        
        # drops: [T < T-1, T-1 < T-2, T-2 < T-3, ...]
        drops = (series.iloc[:-1].values < series.iloc[1:].values) 
        
        max_drop_days = 0
        current_drop_days = 0
        
        # 从最新一天开始迭代
        for is_dropped in drops:
            if is_dropped:
                current_drop_days += 1
                max_drop_days = max(max_drop_days, current_drop_days)
            else:
                current_drop_days = 0
                
        return max_drop_days
        
    except Exception as e:
        logging.error(f"计算连续下跌天数时发生错误: {e}")
        return 0

def calculate_max_drawdown(series):
    """计算最大回撤 (降序序列，最新在前)"""
    try:
        if series.empty:
            return 0.0
        
        series_asc = series.iloc[::-1]
        rolling_max = series_asc.cummax().iloc[::-1] # 保持降序索引，但值是倒序累积最大值
        
        drawdown = (rolling_max - series) / rolling_max
        return drawdown.max()
        
    except Exception as e:
        logging.error(f"计算最大回撤时发生错误: {e}")
        return 0.0

def get_action_prompt(rsi_val, daily_drop_val, mdd_recent_month, max_drop_days_week):
    """
    根据技术指标生成行动提示
    
    Args:
        rsi_val: RSI值
        daily_drop_val: 当日跌幅
        mdd_recent_month: 月最大回撤
        max_drop_days_week: 周连跌天数
        
    Returns:
        str: 行动提示
    """
    if mdd_recent_month >= HIGH_ELASTICITY_MIN_DRAWDOWN and max_drop_days_week == 1:
        if pd.isna(rsi_val):
            return '高回撤观察 (RSI数据缺失)'
        
        # 使用收紧后的 RSI 阈值
        if rsi_val <= EXTREME_RSI_THRESHOLD:
            if daily_drop_val >= MIN_DAILY_DROP_PERCENT:
                # P1: 极度超卖 + 当日大跌
                return f'🌟 买入信号 (RSI极度超卖 <= {EXTREME_RSI_THRESHOLD:.0f} + 当日大跌)'
            else:
                # P2: 极度超卖 + 非大跌日
                return f'考虑试水建仓 (RSI极度超卖 <= {EXTREME_RSI_THRESHOLD:.0f})'
        elif rsi_val < 35: # 保持 30-35 之间的提示
            return '观察中 (RSI超卖, 但未达极值)'
        else:
            return '高回撤观察 (RSI未超卖)'
    else:
        return '不适用 (非高弹性精选)'

def analyze_single_fund(filepath):
    """分析单只基金"""
    try:
        fund_code = os.path.splitext(os.path.basename(filepath))[0]
        
        df = pd.read_csv(filepath)
        df['date'] = pd.to_datetime(df['date'])
        # 核心：确保数据按日期降序排列，最新数据在最前面
        df = df.sort_values(by='date', ascending=False).reset_index(drop=True)
        df = df.rename(columns={'net_value': 'value'})
        
        # 数据验证
        is_valid, msg = validate_fund_data(df, fund_code)
        if not is_valid:
            logging.warning(f"基金 {fund_code} 数据无效: {msg}")
            return None
        
        # 计算基础指标
        df_recent_month = df.head(30)
        df_recent_week = df.head(5)
        
        mdd_recent_month = calculate_max_drawdown(df_recent_month['value'])
        max_drop_days_week = calculate_consecutive_drops(df_recent_week['value'])
        
        # 计算技术指标
        tech_indicators = calculate_technical_indicators(df)
        
        # 生成行动提示
        action_prompt = get_action_prompt(
            tech_indicators.get('RSI', np.nan), 
            tech_indicators.get('当日跌幅', 0.0), 
            mdd_recent_month, 
            max_drop_days_week
        )
        
        # 核心筛选条件: 满足月回撤底线
        if mdd_recent_month >= MIN_MONTH_DRAWDOWN:
            
            return {
                '基金代码': fund_code,
                '最大回撤': mdd_recent_month,
                '最大连续下跌': calculate_consecutive_drops(df_recent_month['value']),
                '近一周连跌': max_drop_days_week,
                **tech_indicators,
                '行动提示': action_prompt
            }
        
        return None
        
    except Exception as e:
        logging.error(f"分析基金 {filepath} 时发生错误: {e}")
        return None

def analyze_all_funds(target_codes=None):
    """分析所有基金数据"""
    try:
        if target_codes:
            csv_files = [os.path.join(FUND_DATA_DIR, f'{code}.csv') for code in target_codes if os.path.exists(os.path.join(FUND_DATA_DIR, f'{code}.csv'))]
        else:
            csv_files = glob.glob(os.path.join(FUND_DATA_DIR, '*.csv'))
        
        if not csv_files:
            logging.warning(f"在目录 '{FUND_DATA_DIR}' 中未找到CSV文件")
            return []
        
        logging.info(f"找到 {len(csv_files)} 个基金数据文件，开始分析...")
        
        qualifying_funds = []
        for filepath in csv_files:
            result = analyze_single_fund(filepath)
            if result is not None:
                qualifying_funds.append(result)
        
        logging.info(f"分析完成，共找到 {len(qualifying_funds)} 只符合条件的基金")
        return qualifying_funds
        
    except Exception as e:
        logging.error(f"分析所有基金时发生错误: {e}")
        return []

def format_technical_value(value, format_type='percent'):
    """格式化技术指标值用于显示"""
    if pd.isna(value): return 'NaN'
    if format_type == 'percent': return f"{value:.2%}"
    elif format_type == 'decimal2': return f"{value:.2f}"
    elif format_type == 'decimal4': return f"{value:.4f}"
    else: return str(value)

def format_table_row(index, row):
    """格式化 Markdown 表格行，包含颜色/符号标记"""
    latest_value = row.get('最新净值', 1.0)
    trial_price = latest_value * 0.97
    
    # 趋势健康度标记
    trend_display = row['MA50/MA250趋势']
    ma_ratio_display = format_technical_value(row['MA50/MA250'], 'decimal2')
    
    # 突出显示不健康的趋势
    if trend_display == '向下' and row['MA50/MA250'] < 0.95:
         trend_display = f"⚠️ {trend_display}"
         ma_ratio_display = f"⚠️ {ma_ratio_display}"

    return (
        f"| {index} | `{row['基金代码']}` | **{format_technical_value(row['最大回撤'], 'percent')}** | "
        f"{format_technical_value(row['当日跌幅'], 'percent')} | {row['RSI']:.2f} | "
        f"{row['MACD信号']} | {format_technical_value(row['净值/MA50'], 'decimal2')} | "
        f"**{ma_ratio_display}** | **{trend_display}** | "
        f"{format_technical_value(row['净值/MA250'], 'decimal2')} | {trial_price:.4f} | **{row['行动提示']}** |\n"
    )


def generate_report(results, timestamp_str):
    """
    生成完整的Markdown格式报告，包含严格的优先级划分逻辑
    """
    try:
        if not results:
            return (
                f"# 基金预警报告 ({timestamp_str} UTC+8)\n\n"
                f"## 分析总结\n\n"
                f"**恭喜，在过去一个月内，没有发现同时满足 '1个月回撤{MIN_MONTH_DRAWDOWN*100:.0f}%以上' 的基金。**\n\n"
                f"---\n分析数据时间范围: 最近30个交易日 (通常约为1个月)。"
            )

        # 创建DataFrame并排序
        df_results = pd.DataFrame(results).sort_values(by='最大回撤', ascending=False).reset_index(drop=True)
        df_results.index = df_results.index + 1
        total_count = len(df_results)

        report_parts = []
        
        # 报告头部
        report_parts.extend([
            f"# 基金预警报告 ({timestamp_str} UTC+8)\n\n",
            f"## 分析总结\n\n",
            f"本次分析共发现 **{total_count}** 只基金满足基础预警条件（近 1 个月回撤 $\ge {MIN_MONTH_DRAWDOWN*100:.0f}\%$）。\n",
            f"**策略更新：RSI超卖阈值收紧至 $\le {EXTREME_RSI_THRESHOLD:.0f}$，聚焦极值机会。**\n",
            f"---\n"
        ])

        # 核心筛选：高弹性基金 (MDD>=10% 且 近一周连跌=1)
        df_base_elastic = df_results[
            (df_results['最大回撤'] >= HIGH_ELASTICITY_MIN_DRAWDOWN) &
            (df_results['近一周连跌'] == 1)
        ].copy()

        # 进一步筛选：RSI极度超卖 (RSI <= 29.0)
        df_base_elastic_low_rsi = df_base_elastic[
            df_base_elastic['RSI'] <= EXTREME_RSI_THRESHOLD
        ].copy()
        
        # --- 优先级划分：关键修复逻辑 ---

        # 1. 🥇 第一优先级：【即时恐慌买入】 (RSI极度超卖 且 当日大跌 >= 3%)
        df_buy_signal_1 = df_base_elastic_low_rsi[
            df_base_elastic_low_rsi['当日跌幅'] >= MIN_DAILY_DROP_PERCENT
        ].copy()

        if not df_buy_signal_1.empty:
            df_buy_signal_1 = df_buy_signal_1.sort_values(
                by=['当日跌幅', 'RSI'], ascending=[False, True]
            ).reset_index(drop=True)
            df_buy_signal_1.index = df_buy_signal_1.index + 1

            report_parts.extend([
                f"\n## **🥇 第一优先级：【即时恐慌买入】** ({len(df_buy_signal_1)}只)\n\n",
                r"**条件：** 长期超跌 ($\ge$ " + f"{HIGH_ELASTICITY_MIN_DRAWDOWN*100:.0f}%) + "
                r"低位企稳 + **RSI极度超卖 ($\le {EXTREME_RSI_THRESHOLD:.0f}$)** + **当日跌幅 $\ge$ " + f"{MIN_DAILY_DROP_PERCENT*100:.0f}%**\n",
                r"**纪律：** 市场恐慌时出手，本金充足时应优先配置此列表。**严格关注 MA50/MA250 趋势。**" + "\n\n",
                f"| 排名 | 基金代码 | 最大回撤 (1M) | **当日跌幅** | RSI(14) | MACD信号 | 净值/MA50 | **MA50/MA250** | **趋势** | 净值/MA250 | 试水买价 (跌3%) | 行动提示 |\n",
                f"| :---: | :---: | ---: | ---: | ---: | :---: | ---: | **---:** | :---: | ---: | :---: | :---: |\n"
            ])

            for index, row in df_buy_signal_1.iterrows():
                report_parts.append(format_table_row(index, row))

            report_parts.append("\n---\n")
        else:
            report_parts.extend([
                f"\n## **🥇 第一优先级：【即时恐慌买入】**\n\n",
                f"**今日没有基金同时满足所有严格条件 (RSI $\le {EXTREME_RSI_THRESHOLD:.0f}$ 且 当日跌幅 $\ge {MIN_DAILY_DROP_PERCENT*100:.0f}\%$**，市场恐慌度不足。\n\n",
                f"---\n"
            ])

        # 2. 🥈 第二优先级：【技术共振建仓】 (RSI极度超卖 且 当日跌幅 < 3%)
        df_buy_signal_2 = df_base_elastic_low_rsi[
            df_base_elastic_low_rsi['当日跌幅'] < MIN_DAILY_DROP_PERCENT
        ].copy()
        
        # 确保 P1 和 P2 互斥：排除 P1 中的基金（防止浮点数误差导致重叠）
        funds_to_exclude_1 = df_buy_signal_1['基金代码'].tolist()
        df_buy_signal_2 = df_buy_signal_2[
            ~df_buy_signal_2['基金代码'].isin(funds_to_exclude_1)
        ].copy()


        if not df_buy_signal_2.empty:
            df_buy_signal_2 = df_buy_signal_2.sort_values(
                by=['RSI', '最大回撤'], ascending=[True, False]
            ).reset_index(drop=True)
            df_buy_signal_2.index = df_buy_signal_2.index + 1

            report_parts.extend([
                f"\n## **🥈 第二优先级：【技术共振建仓】** ({len(df_buy_signal_2)}只)\n\n",
                r"**条件：** 长期超跌 ($\ge$ " + f"{HIGH_ELASTICITY_MIN_DRAWDOWN*100:.0f}%) + "
                r"低位企稳 + **RSI极度超卖 ($\le {EXTREME_RSI_THRESHOLD:.0f}$)** + **当日跌幅 $< $" + f"{MIN_DAILY_DROP_PERCENT*100:.0f}%**\n",
                r"**纪律：** 适合在本金有限时优先配置，或在非大跌日进行建仓。**严格关注 MA50/MA250 趋势。**" + "\n\n",
                f"| 排名 | 基金代码 | 最大回撤 (1M) | **当日跌幅** | RSI(14) | MACD信号 | 净值/MA50 | **MA50/MA250** | **趋势** | 净值/MA250 | 试水买价 (跌3%) | 行动提示 |\n",
                f"| :---: | :---: | ---: | ---: | ---: | :---: | ---: | **---:** | :---: | ---: | :---: | :---: |\n"
            ])

            for index, row in df_buy_signal_2.iterrows():
                report_parts.append(format_table_row(index, row))

            report_parts.append("\n---\n")
        else:
            report_parts.extend([
                f"\n## **🥈 第二优先级：【技术共振建仓】**\n\n",
                f"没有基金同时满足 **长期超跌**、**低位企稳** 和 **RSI极度超卖 ($\le {EXTREME_RSI_THRESHOLD:.0f}$)** 且 **当日跌幅 $< {MIN_DAILY_DROP_PERCENT*100:.0f}\%$** 的条件。\n\n",
                f"---\n"
            ])

        # 3. 🥉 第三优先级：扩展观察池 (RSI > 29.0)
        df_extended_elastic = df_base_elastic[
            df_base_elastic['RSI'] > EXTREME_RSI_THRESHOLD
        ].copy()

        if not df_extended_elastic.empty:
            df_extended_elastic = df_extended_elastic.sort_values(
                by='最大回撤', ascending=False
            ).reset_index(drop=True)
            df_extended_elastic.index = df_extended_elastic.index + 1

            report_parts.extend([
                f"\n## **🥉 第三优先级：【扩展观察池】** ({len(df_extended_elastic)}只)\n\n",
                r"**条件：** 长期超跌 ($\ge$ " + f"{HIGH_ELASTICITY_MIN_DRAWDOWN*100:.0f}%) + "
                r"低位企稳，但 **RSI $>{EXTREME_RSI_THRESHOLD:.0f}$ (未达极度超卖)**。\n",
                r"**纪律：** 风险较高，仅作为观察和备选，等待 RSI 进一步进入极度超卖区。**严格关注 MA50/MA250 趋势。**" + "\n\n",
                f"| 排名 | 基金代码 | 最大回撤 (1M) | **当日跌幅** | RSI(14) | MACD信号 | 净值/MA50 | **MA50/MA250** | **趋势** | 净值/MA250 | 试水买价 (跌3%) | 行动提示 |\n",
                f"| :---: | :---: | ---: | ---: | ---: | ---: | ---: | **---:** | :---: | ---: | :---: | :---: |\n"
            ])

            for index, row in df_extended_elastic.iterrows():
                report_parts.append(format_table_row(index, row))

            report_parts.append("\n---\n")
        else:
            report_parts.extend([
                f"\n## **🥉 第三优先级：【扩展观察池】**\n\n",
                r"没有基金满足 **长期超跌** 且 **RSI $>{EXTREME_RSI_THRESHOLD:.0f}$** 的观察条件。" + "\n\n",
                f"---\n"
            ])

        # 所有预警基金列表
        # (省略所有预警基金列表的表格生成，因为它只是一个总览，保持报告简洁)

        # 策略执行纪律（包含行业风险提示）
        report_parts.extend([
            "\n---\n",
            f"## **⚠️ 强化执行纪律：风控与行业审查 (RSI $\le {EXTREME_RSI_THRESHOLD:.0f}$ 极值策略)**\n\n",
            f"**1. 🛑 趋势健康度（MA50/MA250 决定能否买）：**\n",
            r"    * **MA50/MA250 $\ge 0.95$ 且 趋势方向为 '向上' 或 '平稳'** 的基金，视为 **趋势健康**，允许试水。", "\n",
            r"    * **若基金趋势显示 ⚠️ 向下，或 MA50/MA250 $< 0.95$，** 则表明长期处于熊市通道，**必须放弃**，无论短期超跌有多严重。", "\n",
            f"**2. 🔍 人工行业与K线审查（排除接飞刀风险）：**\n",
            r"    * **在买入前，必须查阅基金重仓行业。** 如果基金属于近期（如近 3-6 个月）**涨幅巨大、估值过高**的板块（例如：部分AI、半导体），则即使技术超卖，也应视为**高风险回调**，建议**放弃**或**大幅缩减**试水仓位。", "\n",
            r"    * **同时复核 K 线图：** 确认当前价格是否距离**近半年历史高点**太近。若是，则风险高。", "\n",
            f"**3. I 级试水建仓（RSI极值策略）：**\n",
            r"    * 仅当基金满足：**趋势健康** + **净值/MA50 $\le 1.0$** + **RSI $\le {EXTREME_RSI_THRESHOLD:.0f}$** 时，才进行 $\mathbf{I}$ 级试水。", "\n",
            f"**4. 风险控制：**\n",
            f"    * 严格止损线：平均成本价**跌幅达到 8%-10%**，立即清仓止损。\n"
        ])

        return "".join(report_parts)
        
    except Exception as e:
        logging.error(f"生成报告时发生错误: {e}")
        return f"# 报告生成错误\n\n错误信息: {str(e)}"

def main():
    """主函数"""
    try:
        # 1. 设置日志
        setup_logging()
        
        # 2. 获取当前时间
        try:
            tz = pytz.timezone('Asia/Shanghai')
            now = datetime.now(tz)
        except:
            now = datetime.now()
            logging.warning("使用时区失败，使用本地时间")
        
        timestamp_for_report = now.strftime('%Y-%m-%d %H:%M:%S')
        timestamp_for_filename = now.strftime('%Y%m%d_%H%M%S')
        dir_name = now.strftime('%Y%m')

        # 3. 创建输出目录
        os.makedirs(dir_name, exist_ok=True)
        report_file = os.path.join(dir_name, f"{REPORT_BASE_NAME}_{timestamp_for_filename}.md")

        logging.info("开始分析基金数据...")
        
        # 4. 执行分析
        results = analyze_all_funds()
        
        # 5. 生成报告
        report_content = generate_report(results, timestamp_for_report)
        
        # 6. 保存报告
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        logging.info(f"分析完成，报告已保存到 {report_file}")
        return True
        
    except Exception as e:
        logging.error(f"主程序执行失败: {e}")
        return False

if __name__ == '__main__':
    # 请确保 'fund_data' 目录存在，且其中包含以基金代码命名的 CSV 文件 (date, net_value)
    success = main()
    print("脚本执行完毕。")

