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
MIN_MONTH_DRAWDOWN = 0.06 # 基础回撤要求 (6%)
HIGH_ELASTICITY_MIN_DRAWDOWN = 0.10 # 高弹性策略的基础回撤要求 (10%)
MIN_DAILY_DROP_PERCENT = 0.03 # 当日大跌的定义 (3%)

# --- 核心阈值调整 ---
EXTREME_RSI_THRESHOLD_P1 = 29.0 
STRONG_RSI_THRESHOLD_P2 = 35.0

# --- 设置日志 ---
def setup_logging():
    """配置日志，避免在脚本运行时产生不必要的控制台输出"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.NullHandler() 
        ]
    )
    # 清空所有处理器，确保静默
    logging.getLogger().handlers = [] 
    
# --- 验证数据 ---
def validate_fund_data(df, fund_code):
    """验证基金数据的完整性和质量"""
    if df.empty: return False, "数据为空"
    if 'value' not in df.columns: return False, "缺少净值列"
    # 最小数据要求为 60
    if len(df) < 60: return False, f"数据不足60条，当前只有{len(df)}条"
    if (df['value'] <= 0).any(): return False, "存在无效净值(<=0)"
    return True, "数据有效"

# --- 布林带计算 ---
def calculate_bollinger_bands(series, window=20):
    """计算布林带位置"""
    if len(series) < window:
        return "数据不足", np.nan
    
    df_temp = pd.DataFrame({'value': series.values})
    # 确保有足够的非NaN数据进行计算
    if len(df_temp.dropna()) < window:
         return "数据不足", np.nan
         
    df_temp['MA20'] = df_temp['value'].rolling(window=window).mean()
    df_temp['STD20'] = df_temp['value'].rolling(window=window).std()
    
    if pd.isna(df_temp['STD20'].iloc[-1]) or df_temp['STD20'].iloc[-1] == 0:
        # 如果标准差为0或NaN，说明数据平稳或不足
        return "波动极小", 0.5
        
    df_temp['Upper Band'] = df_temp['MA20'] + (df_temp['STD20'] * 2)
    df_temp['Lower Band'] = df_temp['MA20'] - (df_temp['STD20'] * 2)
    
    latest_value = df_temp['value'].iloc[-1]
    latest_lower = df_temp['Lower Band'].iloc[-1]
    latest_upper = df_temp['Upper Band'].iloc[-1]
    
    if pd.isna(latest_lower) or pd.isna(latest_upper):
        return "数据不足", np.nan
        
    range_band = latest_upper - latest_lower
    
    # 布林带位置归一化：0 代表在下轨，1 代表在上轨
    position = (latest_value - latest_lower) / range_band if range_band > 0 else 0.5

    if latest_value <= latest_lower:
        return "**下轨下方**", position # position <= 0
    elif latest_value >= latest_upper:
        return "**上轨上方**", position # position >= 1
    else:
        if position < 0.2:
            return "下轨附近", position
        elif position > 0.8:
            return "上轨附近", position
        else:
            return "轨道中间", position

# --- KDJ 计算 (新增) ---
def calculate_kdj(df):
    """
    计算 KDJ 指标 (9, 3, 3)
    """
    if len(df) < 9:
        return {'K': np.nan, 'D': np.nan, 'J': np.nan, 'KDJ信号': '数据不足'}
        
    # 计算 RSV
    # rolling().min() 和 rolling().max() 会自动处理 NaN
    low_min = df['value'].rolling(window=9).min()
    high_max = df['value'].rolling(window=9).max()
    
    # 避免除以零或 NaN
    range_max_min = high_max - low_min
    # 在 9 个周期内价格未变动时，range_max_min可能为0，此时RSV通常视为100或0，但为安全起见，使用replace(0, np.nan)
    rsv = (df['value'] - low_min) / range_max_min.replace(0, np.nan) * 100
    df['RSV'] = rsv

    # 计算 K 和 D (3日 EMA 平滑)
    # pandas ewm(com=2) 对应 alpha=1/3，符合标准 KDJ 平滑
    df['K'] = df['RSV'].ewm(com=2, adjust=False).mean()
    df['D'] = df['K'].ewm(com=2, adjust=False).mean()
    
    # J = 3K - 2D
    df['J'] = 3 * df['K'] - 2 * df['D']

    k_latest = df['K'].iloc[-1]
    d_latest = df['D'].iloc[-1]
    j_latest = df['J'].iloc[-1]

    # KDJ 信号判断
    k_prev = df['K'].iloc[-2] if len(df) >= 2 else np.nan
    d_prev = df['D'].iloc[-2] if len(df) >= 2 else np.nan
    
    kdj_signal = '观察'
    # 必须在超卖区 (K < 30) 且发生金叉
    if not np.isnan(k_prev) and not np.isnan(d_prev) and k_latest < 30:
        if k_latest > d_latest and k_prev < d_prev: 
            kdj_signal = '超卖金叉'
        
    return {
        'K': round(k_latest, 2) if not math.isnan(k_latest) else np.nan,
        'D': round(d_latest, 2) if not math.isnan(d_latest) else np.nan,
        'J': round(j_latest, 2) if not math.isnan(j_latest) else np.nan,
        'KDJ信号': kdj_signal
    }

# --- 技术指标计算 ---
def calculate_technical_indicators(df):
    """计算基金净值的完整技术指标"""
    df_asc = df.copy()

    try:
        if 'value' not in df_asc.columns or len(df_asc) < 60:
            # 简化错误/数据不足返回
            return {
                'RSI': np.nan, 'MACD信号': '数据不足', '净值/MA50': np.nan,
                '净值/MA250': np.nan, 'MA50/MA250': np.nan, 
                'MA50/MA250趋势': '数据不足',
                '布林带位置': '数据不足', '布林带位置值': np.nan,
                '最新净值': df_asc['value'].iloc[-1] if not df_asc.empty else np.nan,
                '当日跌幅': np.nan, 'K': np.nan, 'D': np.nan, 'J': np.nan, 'KDJ信号': '数据不足'
            }

        # 1. RSI (14)
        delta = df_asc['value'].diff()
        gain = (delta.where(delta > 0, 0)).ewm(span=14, adjust=False).mean()
        loss = (-delta.where(delta < 0, 0)).ewm(span=14, adjust=False).mean()
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
            if macd_latest > signal_latest and macd_prev < signal_prev: macd_signal = '金叉'
            elif macd_latest < signal_latest and macd_prev > signal_prev: macd_signal = '死叉'

        # 3. 移动平均线和趋势分析 (MA50, MA250)
        df_asc['MA50'] = df_asc['value'].rolling(window=50, min_periods=1).mean()
        df_asc['MA250'] = df_asc['value'].rolling(window=250, min_periods=1).mean() 
        
        ma50_latest = df_asc['MA50'].iloc[-1]
        ma250_latest = df_asc['MA250'].iloc[-1]
        value_latest = df_asc['value'].iloc[-1]
        
        net_to_ma50 = value_latest / ma50_latest if ma50_latest and ma50_latest != 0 else np.nan
        
        # 250天数据不足时，大趋势指标显示 '---'
        if len(df_asc) < 250 or pd.isna(ma250_latest):
            net_to_ma250 = np.nan
            ma50_to_ma250 = np.nan
            trend_direction = '数据不足'
        else:
            net_to_ma250 = value_latest / ma250_latest if ma250_latest and ma250_latest != 0 else np.nan
            ma50_to_ma250 = ma50_latest / ma250_latest if ma250_latest and ma250_latest != 0 else np.nan
        
            # MA50/MA250 趋势方向判断 (近20日比率斜率)
            trend_direction = '数据不足'
            recent_ratio = (df_asc['MA50'] / df_asc['MA250']).tail(20).dropna()
            if len(recent_ratio) >= 5:
                # 简单线性拟合斜率
                slope = np.polyfit(np.arange(len(recent_ratio)), recent_ratio.values, 1)[0]
                if slope > 0.0005: trend_direction = '向上'
                elif slope < -0.0005: trend_direction = '向下'
                else: trend_direction = '平稳'
        
        # 4. 当日涨跌幅
        daily_drop = 0.0
        if len(df_asc) >= 2:
            value_t_minus_1 = df_asc['value'].iloc[-2]
            if value_t_minus_1 > 0:
                daily_drop = (value_latest - value_t_minus_1) / value_t_minus_1
                
        # 5. KDJ 计算 (调用新增函数)
        kdj_indicators = calculate_kdj(df_asc)

        # 6. 布林带位置 (调用函数)
        bollinger_position, bollinger_value = calculate_bollinger_bands(df_asc['value'])

        return {
            'RSI': round(rsi_latest, 2) if not math.isnan(rsi_latest) else np.nan,
            'MACD信号': macd_signal,
            '净值/MA50': round(net_to_ma50, 2) if not math.isnan(net_to_ma50) else np.nan,
            '净值/MA250': round(net_to_ma250, 2) if not math.isnan(net_to_ma250) else np.nan, 
            'MA50/MA250': round(ma50_to_ma250, 2) if not math.isnan(ma50_to_ma250) else np.nan, 
            'MA50/MA250趋势': trend_direction,
            '布林带位置': bollinger_position, 
            '布林带位置值': bollinger_value, # 新增，用于数值判断
            '最新净值': round(value_latest, 4) if not math.isnan(value_latest) else np.nan,
            '当日跌幅': round(daily_drop, 4),
            **kdj_indicators # 导入 KDJ 结果
        }

    except Exception as e:
        # 实际运行中可在此处启用 logging.error(f"计算技术指标时发生错误: {e}")
        return {
            'RSI': np.nan, 'MACD信号': '计算错误', '净值/MA50': np.nan,
            '净值/MA250': np.nan, 'MA50/MA250': np.nan, 
            'MA50/MA250趋势': '计算错误',
            '布林带位置': '计算错误', '布林带位置值': np.nan,
            '最新净值': np.nan,
            '当日跌幅': np.nan, 'K': np.nan, 'D': np.nan, 'J': np.nan, 'KDJ信号': '计算错误'
        }

# --- 连续下跌计算 ---
def calculate_consecutive_drops(series):
    """计算净值序列中最大的连续下跌天数 (t < t-1)"""
    try:
        if series.empty or len(series) < 2: return 0
        series_asc = series
        # diff() < 0 表示净值下降
        drops = (series_asc.diff() < 0).values
        max_drop_days = 0
        current_drop_days = 0
        # 从第二个元素开始计算 (因为 diff() 结果的第一个是 NaN)
        for is_dropped in drops[1:]:
            if is_dropped:
                current_drop_days += 1
                max_drop_days = max(max_drop_days, current_drop_days)
            else:
                current_drop_days = 0
        
        return max_drop_days
    except Exception as e:
        return 0

# --- 最大回撤计算 ---
def calculate_max_drawdown(series):
    """计算最大回撤"""
    try:
        if series.empty: return 0.0
        rolling_max = series.cummax()
        drawdown = (rolling_max - series) / rolling_max
        return drawdown.max()
    except Exception as e:
        return 0.0

# --- 行动提示生成 (核心逻辑优化) ---
def get_action_prompt(rsi_val, mdd_recent_month, bollinger_val, k_val, daily_drop_val):
    """
    根据技术指标生成行动提示，整合 KDJ 和布林带作为二次风控。
    """
    
    # 辅助函数：判断布林带是否在下轨区域
    def is_near_lower_band(bollinger_v):
        # 布林带位置值 <= 0.2 (下轨下方、下轨附近)
        return not pd.isna(bollinger_v) and bollinger_v <= 0.2
        
    # 辅助函数：判断 KDJ 是否处于超卖区
    def is_kdj_oversold(k_v):
        # K 值 <= 20
        return not pd.isna(k_v) and k_v <= 20

    # 优先筛选：一个月回撤 >= 10% (HIGH_ELASTICITY_MIN_DRAWDOWN)
    if mdd_recent_month >= HIGH_ELASTICITY_MIN_DRAWDOWN:
        
        # P1 极值超卖
        if rsi_val <= EXTREME_RSI_THRESHOLD_P1:
            if is_near_lower_band(bollinger_val):
                # 必须 BB 共振，否则降级为 P1-观察
                if is_kdj_oversold(k_val):
                     return f'🌟 P1-**三指标共振** (RSI $\le {EXTREME_RSI_THRESHOLD_P1:.0f}$, KDJ $\le 20$)'
                else:
                     return f'🔥 P1-**RSI&BB共振** (RSI $\le {EXTREME_RSI_THRESHOLD_P1:.0f}$)'
            else:
                 return f'P1-高回撤观察 (RSI $\le {EXTREME_RSI_THRESHOLD_P1:.0f}$)'
        
        # P2 强力超卖
        elif rsi_val <= STRONG_RSI_THRESHOLD_P2:
            if is_near_lower_band(bollinger_val):
                 # 必须 BB 共振
                 return f'🔍 P2-**BB&RSI共振** (RSI $\le {STRONG_RSI_THRESHOLD_P2:.0f}$)'
            else:
                 return f'P2-关注 (RSI $\le {STRONG_RSI_THRESHOLD_P2:.0f}$)'
        
        # P3
        else:
            if bollinger_val is not np.nan and bollinger_val >= 0.8:
                 # 排除掉价格已经接近或到达上轨的基金 (防止追高回调)
                 return '⚠️ 高回撤但**接近上轨** (观望)'
            elif is_near_lower_band(bollinger_val):
                 return '观察中 (BB超卖但RSI未达标)'
            return '观察中 (RSI未超卖)'


    # 次要筛选：基础回撤 6% <= 回撤 < 10%
    if mdd_recent_month >= MIN_MONTH_DRAWDOWN:
          # 在此范围内，若布林带已远离下轨（>0.8），则警惕。
          if bollinger_val is not np.nan and bollinger_val >= 0.8:
              return f'⚠️ 基础回撤但**接近上轨**'
          return f'关注 (回撤 {mdd_recent_month:.2%})'
    
    return '不适用 (未达基础回撤)'

# --- 单基金分析 ---
def analyze_single_fund(filepath):
    """分析单只基金"""
    fund_code = os.path.splitext(os.path.basename(filepath))[0]
    df = pd.DataFrame()

    try:
        # 尝试使用 UTF-8 读取
        df = pd.read_csv(filepath, encoding='utf-8')
    except UnicodeDecodeError:
        try:
            # 尝试使用 GBK/GB2312 读取
            df = pd.read_csv(filepath, encoding='gbk')
        except Exception:
            return None
    except Exception:
          return None

    try:
        if 'date' not in df.columns or 'net_value' not in df.columns:
            return None
            
        df['date'] = pd.to_datetime(df['date'])
        # 确保按时间升序排列，并使用 'value' 作为净值列名
        df = df.sort_values(by='date', ascending=True).reset_index(drop=True)
        df = df.rename(columns={'net_value': 'value'})
        
        is_valid, msg = validate_fund_data(df, fund_code)
        if not is_valid: 
              return None
        
        df_recent_month = df['value'].tail(30)
        
        mdd_recent_month = calculate_max_drawdown(df_recent_month)
        
        tech_indicators = calculate_technical_indicators(df)
        
        action_prompt = get_action_prompt(
            tech_indicators.get('RSI', np.nan), 
            mdd_recent_month,
            tech_indicators.get('布林带位置值', np.nan),
            tech_indicators.get('K', np.nan),
            tech_indicators.get('当日跌幅', 0.0)
        )
        
        # 基础过滤条件：近一个月回撤 >= 6%
        if mdd_recent_month >= MIN_MONTH_DRAWDOWN:
            return {
                '基金代码': fund_code,
                '最大回撤': mdd_recent_month,
                '最大连续下跌': calculate_consecutive_drops(df['value'].tail(30)),
                **tech_indicators,
                '行动提示': action_prompt
            }
        return None
    except Exception as e:
        return None

# --- 所有基金分析 ---
def analyze_all_funds(target_codes=None):
    """分析所有基金数据"""
    try:
        # 查找 FUND_DATA_DIR 目录下的所有 .csv 文件
        csv_files = glob.glob(os.path.join(FUND_DATA_DIR, '*.csv'))
        
        if not csv_files:
            return []
            
        qualifying_funds = []
        for filepath in csv_files:
            result = analyze_single_fund(filepath)
            if result is not None:
                qualifying_funds.append(result)
        
        return qualifying_funds
    except Exception as e:
        return []

# --- 技术值格式化 ---
def format_technical_value(value, format_type='percent'):
    """格式化技术指标值用于显示"""
    if pd.isna(value): return '---'
    
    if format_type == 'report_daily_drop':
        # 负值（下跌）显示红色粗体，正值（上涨）正常显示
        if value < 0:
            return f"**{value:.2%}**"
        elif value > 0:
            return f"{value:.2%}"
        else:
            return "0.00%"
            
    if format_type == 'percent': return f"{value:.2%}"
    elif format_type == 'decimal2': return f"{value:.2f}"
    elif format_type == 'decimal4': return f"{value:.4f}"
    else: return str(value)

# --- 表格行格式化 ---
def format_table_row(index, row, table_part=1):
    """
    格式化 Markdown 表格行，包含颜色/符号标记。
    """
    latest_value = row.get('最新净值', 1.0)
    # 模拟下跌 3% 的试水买入价
    trial_price = latest_value * (1 - 0.03) 
    
    trend_display = row['MA50/MA250趋势']
    ma_ratio_display = format_technical_value(row['MA50/MA250'], 'decimal2')
    
    # 趋势风险警告
    if trend_display == '向下' and (pd.isna(row['MA50/MA250']) or row['MA50/MA250'] < 0.95):
          trend_display = f"⚠️ **{trend_display}**"
          ma_ratio_display = f"⚠️ **{ma_ratio_display}**"
    elif pd.isna(row['MA50/MA250']) or row['MA50/MA250趋势'] == '数据不足':
        trend_display = "---"
        ma_ratio_display = "---"
    else:
        trend_display = f"**{trend_display}**"
        ma_ratio_display = f"**{ma_ratio_display}**"
        
    daily_drop_display = format_technical_value(row['当日跌幅'], 'report_daily_drop')


    if table_part == 1:
        # 表格 1 (8列): 排名, 基金代码, 最大回撤 (1M), 当日涨跌幅, RSI(14), K(9), D(9), 行动提示
        return (
            f"| {index} | `{row['基金代码']}` | **{format_technical_value(row['最大回撤'], 'percent')}** | "
            f"{daily_drop_display} | **{row['RSI']:.2f}** | {row['K']:.2f} | {row['D']:.2f} | **{row['行动提示']}** |\n"
        )
    else:
        # 表格 2 (9列): 基金代码, MACD信号, KDJ信号, 布林带位置, 净值/MA50, MA50/MA250, 趋势, 净值/MA250, 试水买价 (跌3%)
        # 强化 KDJ 信号显示
        kdj_signal_display = row['KDJ信号']
        if kdj_signal_display == '超卖金叉':
             kdj_signal_display = f"🔥 **{kdj_signal_display}**"
        
        return (
            f"| `{row['基金代码']}` | {row['MACD信号']} | {kdj_signal_display} | {row['布林带位置']} | "
            f"{format_technical_value(row['净值/MA50'], 'decimal2')} | {ma_ratio_display} | {trend_display} | "
            f"{format_technical_value(row['净值/MA250'], 'decimal2') if not pd.isna(row['净值/MA250']) else '---'} | `{trial_price:.4f}` |\n"
        )

# --- 报告生成 ---
def generate_report(results, timestamp_str):
    """
    生成完整的Markdown格式报告。
    """
    if not results:
        return (f"# 基金预警报告 ({timestamp_str} UTC+8)\n\n"
                f"**恭喜，没有发现满足基础预警条件的基金。**")

    df_results = pd.DataFrame(results).sort_values(by='最大回撤', ascending=False).reset_index(drop=True)
    actual_total_count = len(results)

    report_parts = []
    report_parts.extend([
        f"# 基金预警报告 ({timestamp_str} UTC+8)\n\n",
        f"## 分析总结\n\n",
        f"本次分析共发现 **{actual_total_count}** 只基金满足基础预警条件（近 1 个月回撤 $\\ge {MIN_MONTH_DRAWDOWN*100:.0f}\\%$）。\n",
        f"**策略更新：已引入 KDJ 和布林带作为二次风控。P1/P2 强制要求布林带靠近下轨。**\n",
        f"\n---\n"
    ])

    df_base_elastic = df_results[
        (df_results['最大回撤'] >= HIGH_ELASTICITY_MIN_DRAWDOWN)
    ].copy()
    
    # 布林带在下轨区域 (位置值 <= 0.2)
    def is_near_lower_band_series(series):
        return series.apply(lambda x: not pd.isna(x) and x <= 0.2)
        
    df_p1 = df_base_elastic[df_base_elastic['RSI'] <= EXTREME_RSI_THRESHOLD_P1].copy()
    
    # P1/P2 严格过滤：必须处于下轨区域 (布林带位置值 <= 0.2)
    df_p1_filtered = df_p1[is_near_lower_band_series(df_p1['布林带位置值'])].copy()
    
    CRITICAL_DROP_INT = MIN_DAILY_DROP_PERCENT
    
    # P1A：即时恐慌买入 (当日跌幅 <= -3%)
    df_p1a = df_p1_filtered[df_p1_filtered['当日跌幅'] <= -CRITICAL_DROP_INT].copy() 
    # P1B：技术共振建仓 (当日跌幅 > -3%)
    df_p1b = df_p1_filtered[df_p1_filtered['当日跌幅'] > -CRITICAL_DROP_INT].copy() 
    
    # 定义两个表格的头部和对齐分隔符
    # 表格 1 (8列): 排名, 基金代码, 最大回撤 (1M), 当日涨跌幅, RSI(14), K(9), D(9), 行动提示
    TABLE_1_HEADER = f"| 排名 | 基金代码 | 最大回撤 (1M) | **当日涨跌幅** | RSI(14) | K(9) | D(9) | 行动提示 |\n"
    TABLE_1_SEPARATOR = f"| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |\n" 
    
    # 表格 2 (9列): 基金代码, MACD信号, KDJ信号, 布林带位置, 净值/MA50, MA50/MA250, 趋势, 净值/MA250, 试水买价 (跌3%)
    TABLE_2_HEADER = f"| 基金代码 | MACD信号 | KDJ信号 | 布林带位置 | 净值/MA50 | **MA50/MA250** | **趋势** | 净值/MA250 | 试水买价 (跌3%) |\n"
    TABLE_2_SEPARATOR = f"| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |\n" 
    
    
    # ----------------------------------------------------
    # 1. 🥇 第一优先级：RSI <= 29.0 & BB 下轨附近/下方
    # ----------------------------------------------------
    
    # --- 报告 P1A ---
    if not df_p1a.empty:
        df_p1a = df_p1a.sort_values(by=['当日跌幅', 'RSI'], ascending=[True, True]).reset_index(drop=True)
        df_p1a.index = df_p1a.index + 1
        
        report_parts.extend([
            f"\n## **🥇 第一优先级 A：【即时恐慌买入】** ({len(df_p1a)}只)\n\n",
            f"**条件：** 长期超跌 + **RSI极度超卖 ($\\le {EXTREME_RSI_THRESHOLD_P1:.0f}$) + 布林带共振** + **当日跌幅 $\\le -{MIN_DAILY_DROP_PERCENT*100:.0f}%**\n",
            r"**纪律：** 市场恐慌时出手，本金充足时应优先配置。**（最高优先级）**" + "\n\n",
            "### 核心指标 (1/2)\n",
            TABLE_1_HEADER,
            TABLE_1_SEPARATOR
        ])
        for index, row in df_p1a.iterrows():
            report_parts.append(format_table_row(index, row, table_part=1))
        
        report_parts.extend([
            "\n### 趋势与技术细节 (2/2)\n",
            TABLE_2_HEADER,
            TABLE_2_SEPARATOR
        ])
        for index, row in df_p1a.iterrows():
            report_parts.append(format_table_row(index, row, table_part=2))
        
        report_parts.append("\n---\n")

    # --- 报告 P1B ---
    if not df_p1b.empty:
        df_p1b = df_p1b.sort_values(by=['RSI', '最大回撤'], ascending=[True, False]).reset_index(drop=True)
        df_p1b.index = df_p1b.index + 1
        
        report_parts.extend([
            f"\n## **🥇 第一优先级 B：【技术共振建仓】** ({len(df_p1b)}只)\n\n",
            f"**条件：** 长期超跌 + **RSI极度超卖 ($\\le {EXTREME_RSI_THRESHOLD_P1:.0f}$) + 布林带共振** + **当日跌幅 $ > -{MIN_DAILY_DROP_PERCENT*100:.0f}%**\n",
            r"**纪律：** 极值超卖，适合在非大跌日进行建仓。**（第二高优先级）**" + "\n\n",
            "### 核心指标 (1/2)\n",
            TABLE_1_HEADER,
            TABLE_1_SEPARATOR
        ])
        for index, row in df_p1b.iterrows():
            report_parts.append(format_table_row(index, row, table_part=1))
            
        report_parts.extend([
            "\n### 趋势与技术细节 (2/2)\n",
            TABLE_2_HEADER,
            TABLE_2_SEPARATOR
        ])
        for index, row in df_p1b.iterrows():
            report_parts.append(format_table_row(index, row, table_part=2))
            
        report_parts.append("\n---\n")

    # ----------------------------------------------------
    # 2. 🥈 第二优先级：29.0 < RSI <= 35.0 & BB 下轨附近/下方
    # ----------------------------------------------------
    df_p2 = df_base_elastic[
        (df_base_elastic['RSI'] > EXTREME_RSI_THRESHOLD_P1) &
        (df_base_elastic['RSI'] <= STRONG_RSI_THRESHOLD_P2)
    ].copy()
    
    # 严格过滤：必须处于下轨区域 (布林带位置值 <= 0.2)
    df_p2_filtered = df_p2[is_near_lower_band_series(df_p2['布林带位置值'])].copy()
    
    if not df_p2_filtered.empty:
        df_p2_filtered = df_p2_filtered.sort_values(by=['RSI', '最大回撤'], ascending=[True, False]).reset_index(drop=True)
        df_p2_filtered.index = df_p2_filtered.index + 1
        
        report_parts.extend([
            f"\n## **🥈 第二优先级：【强力超卖观察池】** ({len(df_p2_filtered)}只)\n\n",
            f"**条件：** 长期超跌 + **强力超卖 ($>{EXTREME_RSI_THRESHOLD_P1:.0f}$ 且 $\\le {STRONG_RSI_THRESHOLD_P2:.0f}$) + 布林带共振**。\n",
            r"**纪律：** 接近极值，是良好的观察目标，需等待 RSI 进一步下行或 KDJ 配合。**（第三优先级）**" + "\n\n",
            "### 核心指标 (1/2)\n",
            TABLE_1_HEADER,
            TABLE_1_SEPARATOR
        ])

        for index, row in df_p2_filtered.iterrows():
            report_parts.append(format_table_row(index, row, table_part=1))
            
        report_parts.extend([
            "\n### 趋势与技术细节 (2/2)\n",
            TABLE_2_HEADER,
            TABLE_2_SEPARATOR
        ])
        for index, row in df_p2_filtered.iterrows():
            report_parts.append(format_table_row(index, row, table_part=2))
            
        report_parts.append("\n---\n")
    else:
        report_parts.extend([
            f"\n## **🥈 第二优先级：【强力超卖观察池】**\n\n",
            f"没有基金满足 **长期超跌** 且 **RSI/布林带共振** 的条件。" + "\n\n",
            f"---\n"
        ])


    # 3. 🥉 第三优先级：扩展观察池 (RSI > 35.0 或未通过 BB 过滤的 P1/P2)
    df_p3 = df_results[
        (df_results['最大回撤'] >= MIN_MONTH_DRAWDOWN) & 
        (
            (df_results['最大回撤'] < HIGH_ELASTICITY_MIN_DRAWDOWN) | # 低回撤范围 6%-10%
            (df_results['RSI'].isna()) | (df_results['RSI'] > STRONG_RSI_THRESHOLD_P2) | # RSI 未达标 (>35)
            # 未通过 BB 过滤的 P1/P2 基金 (高回撤但BB未达标)
            (
                (df_results['RSI'] <= STRONG_RSI_THRESHOLD_P2) &
                (~is_near_lower_band_series(df_results['布林带位置值'])) &
                (df_results['最大回撤'] >= HIGH_ELASTICITY_MIN_DRAWDOWN)
            )
        )
    ].copy()

    if not df_p3.empty:
        df_p3 = df_p3.sort_values(by='最大回撤', ascending=False).reset_index(drop=True)
        df_p3.index = df_p3.index + 1

        report_parts.extend([
            f"\n## **🥉 第三优先级：【扩展观察池】** ({len(df_p3)}只)\n\n",
            f"**条件：** 长期超跌（$\\ge 6\\% - 10\\%$）或 **技术指标未完全共振**。\n",
            r"**纪律：** 风险较高，仅作为观察和备选，等待 RSI/BB/KDJ 进一步进入共振区。**（最低优先级）**" + "\n\n",
            "### 核心指标 (1/2)\n",
            TABLE_1_HEADER,
            TABLE_1_SEPARATOR
        ])

        for index, row in df_p3.iterrows():
            report_parts.append(format_table_row(index, row, table_part=1))
            
        report_parts.extend([
            "\n### 趋势与技术细节 (2/2)\n",
            TABLE_2_HEADER,
            TABLE_2_SEPARATOR
        ])
        for index, row in df_p3.iterrows():
            report_parts.append(format_table_row(index, row, table_part=2))

        report_parts.append("\n---\n")
    
    # 策略执行纪律
    report_parts.extend([
        "\n---\n",
        f"## **⚠️ 强化执行纪律：风控与行业审查**\n\n",
        f"**1. 🛑 趋势健康度（MA50/MA250 决定能否买）：**\n",
        f"    * **MA50/MA250 $\\ge 0.95$ 且 趋势方向为 '向上' 或 '平稳'** 的基金，视为 **趋势健康**，允许试水。\n",
        f"    * **若基金趋势显示 ⚠️ 向下，或 MA50/MA250 $< 0.95$，** 则表明长期处于熊市通道，**必须放弃**，无论短期超跌有多严重。\n",
        f"    * **【新基金提示】**：对于数据不足 250 条的基金，MA50/MA250 相关指标将显示 **'---'**，需结合其他指标和人工审查来判断。\n",
        f"**2. 🔍 人工行业与K线审查（排除接飞刀风险）：**\n",
        r"    * **在买入前，必须查阅基金重仓行业。** 如果基金属于近期（如近 3-6 个月）**涨幅巨大、估值过高**的板块（例如：部分AI、半导体），则即使技术超卖，也应视为**高风险回调**，建议**放弃**或**大幅缩减**试水仓位。\n",
        r"    * **同时复核 K 线图：** 确认当前价格是否距离**近半年历史高点**太近。若是，则风险高。\n",
        f"**3. I 级试水建仓（RSI极值策略）：**\n",
        f"    * 仅当基金满足：**趋势健康** + **净值/MA50 $\\le 1.0$** + **RSI $\\le {EXTREME_RSI_THRESHOLD_P1:.0f}$** + **布林带共振** 时，才进行 $\\mathbf{{I}}$ 级试水。\n",
        f"**4. 风险控制：**\n",
        f"    * 严格止损线：平均成本价**跌幅达到 8%-10%**，立即清仓止损。\n"
    ])

    return "".join(report_parts)

# --- 主函数 (用于实际运行) ---
def main_run():
    """
    主运行函数，用于在实际环境中执行。
    它将查找 'fund_data' 目录下的所有 CSV 文件并生成报告。
    """
    # 确保 fund_data 目录存在
    os.makedirs(FUND_DATA_DIR, exist_ok=True)
    
    setup_logging()
    
    # 获取当前时间（北京/上海时区）
    try:
        tz = pytz.timezone('Asia/Shanghai')
        now = datetime.now(tz)
    except Exception:
        # 如果 pytz 不可用，使用 UTC 时间
        now = datetime.utcnow()
        tz = pytz.timezone('UTC')

    timestamp_for_report = now.strftime('%Y-%m-%d %H:%M:%S')

    # 实际运行分析，分析 fund_data 目录下的所有文件
    results = analyze_all_funds(target_codes=None)
    
    report_content = generate_report(results, timestamp_for_report)
    
    # 将报告内容打印到标准输出，以便被 GitHub Actions 或其他环境捕获
    print(report_content)

if __name__ == "__main__":
    main_run()