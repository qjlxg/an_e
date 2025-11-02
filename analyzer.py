import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime
import pytz
import logging

# --- 配置参数 ---
FUND_DATA_DIR = 'fund_data'
MIN_CONSECUTIVE_DROP_DAYS = 3
MIN_MONTH_DRAWDOWN = 0.06
HIGH_ELASTICITY_MIN_DRAWDOWN = 0.10
MIN_DAILY_DROP_PERCENT = 0.03
REPORT_BASE_NAME = 'fund_warning_report'

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
    
    # 检查日期连续性
    if 'date' in df.columns:
        df_sorted = df.sort_values('date', ascending=False)
        date_diff = df_sorted['date'].diff().dt.days.abs()
        if (date_diff > 10).any():  # 允许最大间隔10天
            logging.warning(f"基金 {fund_code} 数据日期间隔异常")
    
    return True, "数据有效"

def calculate_technical_indicators(df):
    """
    计算基金净值的完整技术指标
    
    Args:
        df: 基金数据DataFrame，按日期降序排列
        
    Returns:
        dict: 包含所有技术指标的字典
    """
    try:
        # 数据验证
        if 'value' not in df.columns or len(df) < 250:
            return {
                'RSI': np.nan, 'MACD信号': '数据不足', '净值/MA50': np.nan,
                '净值/MA250': np.nan, 'MA50/MA250': np.nan, 
                'MA50/MA250趋势': '数据不足',
                '布林带位置': '数据不足', '最新净值': df['value'].iloc[0] if not df.empty else np.nan,
                '当日跌幅': np.nan
            }

        # 创建升序副本用于计算
        df_asc = df.iloc[::-1].copy().reset_index(drop=True)

        # 1. RSI (14)
        delta = df_asc['value'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14, min_periods=1).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14, min_periods=1).mean()
        rs = gain / loss.replace(0, np.nan) 
        df_asc['RSI'] = 100 - (100 / (1 + rs))
        rsi_latest = df_asc['RSI'].iloc[-1]

        # 2. MACD
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
        df_asc['MA50/MA250'] = df_asc['MA50'] / df_asc['MA250']
        
        ma50_latest = df_asc['MA50'].iloc[-1]
        ma250_latest = df_asc['MA250'].iloc[-1]
        value_latest = df_asc['value'].iloc[-1]

        # 计算比值
        net_to_ma50 = value_latest / ma50_latest if ma50_latest and ma50_latest != 0 else np.nan
        net_to_ma250 = value_latest / ma250_latest if ma250_latest and ma250_latest != 0 else np.nan
        ma50_to_ma250 = df_asc['MA50/MA250'].iloc[-1]

        # 4. MA50/MA250 趋势方向判断
        trend_direction = '数据不足'
        if len(df_asc) >= 250:
            recent_trend = df_asc['MA50/MA250'].tail(20).dropna()
            if len(recent_trend) >= 5:  # 至少5个数据点
                try:
                    x = np.arange(len(recent_trend))
                    slope = np.polyfit(x, recent_trend.values, 1)[0]
                    
                    if slope > 0.001:
                        trend_direction = '向上'
                    elif slope < -0.001:
                        trend_direction = '向下'
                    else:
                        trend_direction = '平稳'
                except:
                    trend_direction = '计算错误'

        # 5. 布林带
        df_asc['MA20'] = df_asc['value'].rolling(window=20, min_periods=1).mean()
        df_asc['StdDev'] = df_asc['value'].rolling(window=20, min_periods=1).std()
        ma20_latest = df_asc['MA20'].iloc[-1]
        std_latest = df_asc['StdDev'].iloc[-1]

        bollinger_pos = '数据不足'
        if not np.isnan(ma20_latest) and not np.isnan(std_latest) and std_latest > 0:
            upper_latest = ma20_latest + (std_latest * 2)
            lower_latest = ma20_latest - (std_latest * 2)

            if value_latest > upper_latest:
                bollinger_pos = '上轨上方'
            elif value_latest < lower_latest:
                bollinger_pos = '下轨下方'
            elif value_latest > ma20_latest:
                bollinger_pos = '中轨上方'
            else:
                bollinger_pos = '中轨下方'

        # 6. 当日跌幅
        daily_drop = 0.0
        if len(df_asc) >= 2:
            value_t_minus_1 = df_asc['value'].iloc[-2]
            if value_t_minus_1 > 0:
                daily_drop = (value_t_minus_1 - value_latest) / value_t_minus_1

        return {
            'RSI': round(rsi_latest, 2) if not np.isnan(rsi_latest) else np.nan,
            'MACD信号': macd_signal,
            '净值/MA50': round(net_to_ma50, 2) if not np.isnan(net_to_ma50) else np.nan,
            '净值/MA250': round(net_to_ma250, 2) if not np.isnan(net_to_ma250) else np.nan, 
            'MA50/MA250': round(ma50_to_ma250, 2) if not np.isnan(ma50_to_ma250) else np.nan, 
            'MA50/MA250趋势': trend_direction,
            '布林带位置': bollinger_pos,
            '最新净值': round(value_latest, 4) if not np.isnan(value_latest) else np.nan,
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
    """
    计算净值序列中最大的连续下跌天数
    
    Args:
        series: 净值序列，按日期降序排列
        
    Returns:
        int: 最大连续下跌天数
    """
    try:
        if series.empty or len(series) < 2:
            return 0
        
        # 计算每日是否下跌 (今日净值 < 昨日净值)
        drops = (series.iloc[:-1].values < series.iloc[1:].values)
        drops_int = drops.astype(int)
        
        max_drop_days = 0
        current_drop_days = 0
        
        for val in drops_int:
            if val == 1:
                current_drop_days += 1
                max_drop_days = max(max_drop_days, current_drop_days)
            else:
                current_drop_days = 0
                
        return max_drop_days
        
    except Exception as e:
        logging.error(f"计算连续下跌天数时发生错误: {e}")
        return 0

def calculate_max_drawdown(series):
    """
    计算最大回撤
    
    Args:
        series: 净值序列
        
    Returns:
        float: 最大回撤比例
    """
    try:
        if series.empty:
            return 0.0
        
        rolling_max = series.cummax()
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
    # 只有满足高弹性基础条件时才生成具体提示
    if mdd_recent_month >= HIGH_ELASTICITY_MIN_DRAWDOWN and max_drop_days_week == 1:
        if pd.isna(rsi_val):
            return '高回撤观察 (RSI数据缺失)'
        
        if rsi_val < 30 and daily_drop_val >= MIN_DAILY_DROP_PERCENT:
            return '买入信号 (RSI极度超卖 + 当日大跌)'
        elif rsi_val < 35 and daily_drop_val >= MIN_DAILY_DROP_PERCENT:
            return '买入信号 (RSI超卖 + 当日大跌)'
        elif rsi_val < 35:
            return '考虑试水建仓 (RSI超卖)'
        else:
            return '高回撤观察 (RSI未超卖)'
    else:
        return '不适用 (非高弹性精选)'

def analyze_single_fund(filepath):
    """
    分析单只基金
    
    Args:
        filepath: 基金数据文件路径
        
    Returns:
        dict or None: 基金分析结果，分析失败时返回None
    """
    try:
        fund_code = os.path.splitext(os.path.basename(filepath))[0]
        
        # 读取数据
        df = pd.read_csv(filepath)
        df['date'] = pd.to_datetime(df['date'])
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
        
        max_drop_days_month = calculate_consecutive_drops(df_recent_month['value'])
        mdd_recent_month = calculate_max_drawdown(df_recent_month['value'])
        max_drop_days_week = calculate_consecutive_drops(df_recent_week['value'])
        
        # 计算技术指标
        tech_indicators = calculate_technical_indicators(df)
        rsi_val = tech_indicators.get('RSI', np.nan)
        daily_drop_val = tech_indicators.get('当日跌幅', 0.0)
        
        # 生成行动提示
        action_prompt = get_action_prompt(rsi_val, daily_drop_val, mdd_recent_month, max_drop_days_week)
        
        # 核心筛选条件
        if (max_drop_days_month >= MIN_CONSECUTIVE_DROP_DAYS and 
            mdd_recent_month >= MIN_MONTH_DRAWDOWN):
            
            return {
                '基金代码': fund_code,
                '最大回撤': mdd_recent_month,
                '最大连续下跌': max_drop_days_month,
                '近一周连跌': max_drop_days_week,
                'RSI': tech_indicators['RSI'],
                'MACD信号': tech_indicators['MACD信号'],
                '净值/MA50': tech_indicators['净值/MA50'],
                '净值/MA250': tech_indicators['净值/MA250'], 
                'MA50/MA250': tech_indicators['MA50/MA250'],
                'MA50/MA250趋势': tech_indicators['MA50/MA250趋势'],
                '布林带位置': tech_indicators['布林带位置'],
                '最新净值': tech_indicators['最新净值'],
                '当日跌幅': daily_drop_val,
                '行动提示': action_prompt
            }
        
        return None
        
    except Exception as e:
        logging.error(f"分析基金 {filepath} 时发生错误: {e}")
        return None

def analyze_all_funds(target_codes=None):
    """
    分析所有基金数据
    
    Args:
        target_codes: 指定分析的基金代码列表，None表示分析所有基金
        
    Returns:
        list: 符合条件的基金分析结果列表
    """
    try:
        # 获取基金数据文件
        if target_codes:
            csv_files = []
            for code in target_codes:
                filepath = os.path.join(FUND_DATA_DIR, f'{code}.csv')
                if os.path.exists(filepath):
                    csv_files.append(filepath)
        else:
            csv_files = glob.glob(os.path.join(FUND_DATA_DIR, '*.csv'))
        
        if not csv_files:
            logging.warning(f"在目录 '{FUND_DATA_DIR}' 中未找到CSV文件")
            return []
        
        logging.info(f"找到 {len(csv_files)} 个基金数据文件，开始分析...")
        
        # 分析所有基金
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
    """
    格式化技术指标值用于显示
    
    Args:
        value: 原始值
        format_type: 格式化类型
        
    Returns:
        str: 格式化后的字符串
    """
    if pd.isna(value):
        return 'NaN'
    
    if format_type == 'percent':
        return f"{value:.2%}"
    elif format_type == 'decimal2':
        return f"{value:.2f}"
    elif format_type == 'decimal4':
        return f"{value:.4f}"
    else:
        return str(value)

def generate_report(results, timestamp_str):
    """
    生成完整的Markdown格式报告
    
    Args:
        results: 基金分析结果列表
        timestamp_str: 时间戳字符串
        
    Returns:
        str: Markdown格式的报告内容
    """
    try:
        if not results:
            return (
                f"# 基金预警报告 ({timestamp_str} UTC+8)\n\n"
                f"## 分析总结\n\n"
                f"**恭喜，在过去一个月内，没有发现同时满足 '连续下跌{MIN_CONSECUTIVE_DROP_DAYS}天以上' "
                f"和 '1个月回撤{MIN_MONTH_DRAWDOWN*100:.0f}%以上' 的基金。**\n\n"
                f"---\n分析数据时间范围: 最近30个交易日 (通常约为1个月)。"
            )

        # 创建DataFrame并排序
        df_results = pd.DataFrame(results)
        df_results = df_results.sort_values(by='最大回撤', ascending=False).reset_index(drop=True)
        df_results.index = df_results.index + 1
        total_count = len(df_results)

        report_parts = []
        
        # 报告头部
        report_parts.extend([
            f"# 基金预警报告 ({timestamp_str} UTC+8)\n\n",
            f"## 分析总结\n\n",
            f"本次分析共发现 **{total_count}** 只基金同时满足以下两个预警条件（基于最近30个交易日）：\n",
            f"1. **连续下跌**：净值连续下跌 **{MIN_CONSECUTIVE_DROP_DAYS}** 天以上。\n",
            f"2. **高回撤**：近 1 个月内最大回撤达到 **{MIN_MONTH_DRAWDOWN*100:.0f}%** 以上。\n\n",
            f"**指标增强：新增 MA50/MA250 趋势健康指标（含趋势方向），用于过滤长期熊市风险。**\n",
            f"---\n"
        ])

        # 核心筛选：高弹性基金
        df_base_elastic = df_results[
            (df_results['最大回撤'] >= HIGH_ELASTICITY_MIN_DRAWDOWN) &
            (df_results['近一周连跌'] == 1)
        ].copy()

        df_base_elastic_low_rsi = df_base_elastic[df_base_elastic['RSI'] < 35.0].copy()

        # 第一优先级：即时恐慌买入
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
                f"**条件：** 长期超跌 ($\ge$ {HIGH_ELASTICITY_MIN_DRAWDOWN*100:.0f}%) + "
                f"低位企稳 + RSI超卖 ($ < 35$) + **当日跌幅 $\ge$ {MIN_DAILY_DROP_PERCENT*100:.0f}%**\n",
                f"**纪律：** 市场恐慌时出手，本金充足时应优先配置此列表。**严格关注 MA50/MA250 趋势。**\n\n",
                f"| 排名 | 基金代码 | 最大回撤 (1M) | **当日跌幅** | RSI(14) | MACD信号 | 净值/MA50 | **MA50/MA250** | **趋势** | 净值/MA250 | 试水买价 (跌3%) | 行动提示 |\n",
                f"| :---: | :---: | ---: | ---: | ---: | :---: | ---: | **---:** | :---: | ---: | :---: | :---: |\n"
            ])

            for index, row in df_buy_signal_1.iterrows():
                latest_value = row.get('最新净值', 1.0)
                trial_price = latest_value * 0.97
                
                report_parts.append(
                    f"| {index} | `{row['基金代码']}` | **{format_technical_value(row['最大回撤'], 'percent')}** | "
                    f"**{format_technical_value(row['当日跌幅'], 'percent')}** | {row['RSI']:.2f} | "
                    f"{row['MACD信号']} | {format_technical_value(row['净值/MA50'], 'decimal2')} | "
                    f"**{format_technical_value(row['MA50/MA250'], 'decimal2')}** | **{row['MA50/MA250趋势']}** | "
                    f"{format_technical_value(row['净值/MA250'], 'decimal2')} | {trial_price:.4f} | **{row['行动提示']}** |\n"
                )

            report_parts.append("\n---\n")
        else:
            report_parts.extend([
                f"\n## **🥇 第一优先级：【即时恐慌买入】**\n\n",
                f"**今日没有基金同时满足所有严格条件，市场恐慌度不足。**\n\n",
                f"---\n"
            ])

        # 第二优先级：技术共振建仓
        funds_to_exclude_1 = df_buy_signal_1['基金代码'].tolist() if not df_buy_signal_1.empty else []
        df_buy_signal_2 = df_base_elastic_low_rsi[
            ~df_base_elastic_low_rsi['基金代码'].isin(funds_to_exclude_1)
        ].copy()

        if not df_buy_signal_2.empty:
            df_buy_signal_2 = df_buy_signal_2.sort_values(
                by=['RSI', '最大回撤'], ascending=[True, False]
            ).reset_index(drop=True)
            df_buy_signal_2.index = df_buy_signal_2.index + 1

            report_parts.extend([
                f"\n## **🥈 第二优先级：【技术共振建仓】** ({len(df_buy_signal_2)}只)\n\n",
                f"**条件：** 长期超跌 ($\ge$ {HIGH_ELASTICITY_MIN_DRAWDOWN*100:.0f}%) + "
                f"低位企稳 + RSI超卖 ($ < 35$) + **当日跌幅 $< {MIN_DAILY_DROP_PERCENT*100:.0f}\%$**\n",
                f"**纪律：** 适合在本金有限时优先配置，或在非大跌日进行建仓。**严格关注 MA50/MA250 趋势。**\n\n",
                f"| 排名 | 基金代码 | 最大回撤 (1M) | **当日跌幅** | RSI(14) | MACD信号 | 净值/MA50 | **MA50/MA250** | **趋势** | 净值/MA250 | 试水买价 (跌3%) | 行动提示 |\n",
                f"| :---: | :---: | ---: | ---: | ---: | :---: | ---: | **---:** | :---: | ---: | :---: | :---: |\n"
            ])

            for index, row in df_buy_signal_2.iterrows():
                latest_value = row.get('最新净值', 1.0)
                trial_price = latest_value * 0.97
                
                report_parts.append(
                    f"| {index} | `{row['基金代码']}` | **{format_technical_value(row['最大回撤'], 'percent')}** | "
                    f"{format_technical_value(row['当日跌幅'], 'percent')} | **{row['RSI']:.2f}** | "
                    f"{row['MACD信号']} | {format_technical_value(row['净值/MA50'], 'decimal2')} | "
                    f"**{format_technical_value(row['MA50/MA250'], 'decimal2')}** | **{row['MA50/MA250趋势']}** | "
                    f"{format_technical_value(row['净值/MA250'], 'decimal2')} | {trial_price:.4f} | **{row['行动提示']}** |\n"
                )

            report_parts.append("\n---\n")
        else:
            report_parts.extend([
                f"\n## **🥈 第二优先级：【技术共振建仓】**\n\n",
                f"所有满足 **长期超跌+RSI超卖** 基础条件的基金，均已进入 **第一优先级列表**。\n\n",
                f"---\n"
            ])

        # 第三优先级：扩展观察池
        funds_to_exclude_2 = df_base_elastic_low_rsi['基金代码'].tolist()
        df_extended_elastic = df_base_elastic[
            ~df_base_elastic['基金代码'].isin(funds_to_exclude_2)
        ].copy()

        if not df_extended_elastic.empty:
            df_extended_elastic = df_extended_elastic.sort_values(
                by='最大回撤', ascending=False
            ).reset_index(drop=True)
            df_extended_elastic.index = df_extended_elastic.index + 1

            report_parts.extend([
                f"\n## **🥉 第三优先级：【扩展观察池】** ({len(df_extended_elastic)}只)\n\n",
                f"**条件：** 长期超跌 ($\ge$ {HIGH_ELASTICITY_MIN_DRAWDOWN*100:.0f}%) + "
                f"低位企稳，但 **RSI $\ge 35$ (未超卖)**。\n",
                f"**纪律：** 风险较高，仅作为观察和备选，等待 RSI 进一步进入超卖区。**严格关注 MA50/MA250 趋势。**\n\n",
                f"| 排名 | 基金代码 | 最大回撤 (1M) | **当日跌幅** | RSI(14) | MACD信号 | 净值/MA50 | **MA50/MA250** | **趋势** | 净值/MA250 | 试水买价 (跌3%) | 行动提示 |\n",
                f"| :---: | :---: | ---: | ---: | ---: | :---: | ---: | **---:** | :---: | ---: | :---: | :---: |\n"
            ])

            for index, row in df_extended_elastic.iterrows():
                latest_value = row.get('最新净值', 1.0)
                trial_price = latest_value * 0.97
                
                report_parts.append(
                    f"| {index} | `{row['基金代码']}` | **{format_technical_value(row['最大回撤'], 'percent')}** | "
                    f"{format_technical_value(row['当日跌幅'], 'percent')} | {row['RSI']:.2f} | "
                    f"{row['MACD信号']} | {format_technical_value(row['净值/MA50'], 'decimal2')} | "
                    f"**{format_technical_value(row['MA50/MA250'], 'decimal2')}** | **{row['MA50/MA250趋势']}** | "
                    f"{format_technical_value(row['净值/MA250'], 'decimal2')} | {trial_price:.4f} | {row['行动提示']} |\n"
                )

            report_parts.append("\n---\n")
        else:
            report_parts.extend([
                f"\n## **🥉 第三优先级：【扩展观察池】**\n\n",
                f"没有基金满足 **长期超跌** 且 **RSI $\ge 35$** 的观察条件。\n\n",
                f"---\n"
            ])

        # 所有预警基金列表
        report_parts.extend([
            f"\n## 所有预警基金列表 (共 {total_count} 只，按最大回撤降序排列)\n\n",
            f"| 排名 | 基金代码 | 最大回撤 (1M) | **当日跌幅** | 连跌 (1M) | 连跌 (1W) | RSI(14) | MACD信号 | 净值/MA50 | **MA50/MA250** | **趋势** | 净值/MA250 | 布林带位置 |\n",
            f"| :---: | :---: | ---: | ---: | ---: | ---: | :---: | ---: | ---: | **---:** | :---: | ---: | :---: |\n"
        ])

        for index, row in df_results.iterrows():
            report_parts.append(
                f"| {index} | `{row['基金代码']}` | **{format_technical_value(row['最大回撤'], 'percent')}** | "
                f"{format_technical_value(row['当日跌幅'], 'percent')} | {row['最大连续下跌']} | {row['近一周连跌']} | "
                f"{format_technical_value(row['RSI'], 'decimal2')} | {row['MACD信号']} | "
                f"{format_technical_value(row['净值/MA50'], 'decimal2')} | **{format_technical_value(row['MA50/MA250'], 'decimal2')}** | "
                f"**{row['MA50/MA250趋势']}** | {format_technical_value(row['净值/MA250'], 'decimal2')} | {row['布林带位置']} |\n"
            )

        report_parts.extend([
            "\n---\n",
            f"分析数据时间范围: 最近30个交易日 (通常约为1个月)。\n",
            f"\n## **高弹性策略执行纪律（已结合 MA50/MA250 趋势过滤）**\n\n",
            f"**1. 趋势过滤与建仓（MA指标优先）：**\n",
            f"    * **趋势健康度（MA50/MA250）：** 优先关注 **MA50/MA250 $\ge 0.95$** 且 **趋势方向为 '向上' 或 '平稳'** 的基金。若比值低于 $0.95$ 且趋势方向为 **'向下'**，则表明中期趋势严重走熊，应**果断放弃**。\n",
            f"    * **I 级试水建仓：** 仅当基金同时满足：**MA50/MA250 趋势健康** + **净值/MA50 $\le 1.0$** + **RSI $\le 35$** 时，才进行 $\mathbf{I}$ 级试水。\n",
            f"    * **II/III 级加仓：** 应严格结合**价格跌幅**和**技术共振**。例如，$\mathbf{P}_{\text{current}} \le \mathbf{P}_0 \times 0.95$ **且 $\text{MACD}$ 出现金叉** 或 **RSI $\le 30$** 时，才执行 $\mathbf{II}$ 级/$\mathbf{III}$ 级加仓。\n",
            f"**2. 波段止盈与清仓信号（顺势原则）：**\n",
            f"    * **确认反弹/止盈警惕:** 当目标基金的 **MACD 信号从 '观察/死叉' 变为 '金叉'** 时，表明反弹趋势确立，此时应视为 **分批止盈** 的警惕信号。应在达到您的**平均成本 $\times 1.05$** 止盈线时，果断赎回 $\mathbf{50\%}$ 份额。\n",
            f"    * **趋势反转/清仓:** 当 **MACD 信号从 '金叉' 变为 '死叉'** 或 **净值/MA50 $>$ 1.10** (短期超涨) 且您的**平均成本已实现 5% 利润**时，应考虑**清仓止盈**。\n", 
            f"**3. 风险控制（严格止损）：**\n",
            f"    * 为所有买入的基金设置严格的止损线。建议从买入平均成本价开始计算，一旦跌幅达到 **8%-10%**，应**立即**卖出清仓，避免深度套牢。\n"
        ])

        return "".join(report_parts)
        
    except Exception as e:
        logging.error(f"生成报告时发生错误: {e}")
        return f"# 报告生成错误\n\n错误信息: {str(e)}"

def main():
    """主函数"""
    try:
        # 设置日志
        setup_logging()
        
        # 获取当前时间
        try:
            tz = pytz.timezone('Asia/Shanghai')
            now = datetime.now(tz)
        except:
            now = datetime.now()
            logging.warning("使用时区失败，使用本地时间")
        
        timestamp_for_report = now.strftime('%Y-%m-%d %H:%M:%S')
        timestamp_for_filename = now.strftime('%Y%m%d_%H%M%S')
        dir_name = now.strftime('%Y%m')

        # 创建输出目录
        os.makedirs(dir_name, exist_ok=True)
        report_file = os.path.join(dir_name, f"{REPORT_BASE_NAME}_{timestamp_for_filename}.md")

        logging.info("开始分析基金数据...")
        
        # 执行分析
        results = analyze_all_funds()
        
        # 生成报告
        report_content = generate_report(results, timestamp_for_report)
        
        # 保存报告
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        logging.info(f"分析完成，报告已保存到 {report_file}")
        return True
        
    except Exception as e:
        logging.error(f"主程序执行失败: {e}")
        return False

if __name__ == '__main__':
    success = main()
    exit(0 if success else 1)