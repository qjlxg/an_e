import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime, timedelta
import logging

# --- 核心策略参数 (与 analyzer.py 保持一致) ---
FUND_DATA_DIR = 'fund_data'
HIGH_ELASTICITY_MIN_DRAWDOWN = 0.10  # 高弹性策略的基础回撤要求 (10%)
EXTREME_RSI_THRESHOLD_P1 = 29.0      # 第一优先级买入RSI阈值 (P1)
STRONG_RSI_THRESHOLD_P2 = 35.0       # 第二优先级买入RSI阈值 (P2)
MA_HEALTH_THRESHOLD = 0.95           # 趋势健康度阈值 (MA50/MA250 >= 0.95)
STOP_LOSS_PERCENT = 0.10             # 止损阈值 (10%)
HOLDING_PERIOD = 60                  # 固定持有周期 (60个交易日)

# --- 文件排除列表：跳过非净值数据文件 ---
EXCLUDE_FILES = [
    'fund_fee_result.csv', # 排除用户指定的文件
    # 可在此处添加其他不包含净值数据的文件名
]

# --- 设置日志 (与 analyzer.py 保持一致) ---
def setup_logging():
    """设置日志配置"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('backtest.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    logging.getLogger('backtest').setLevel(logging.INFO)
    return logging.getLogger('backtest')

# --- 最大回撤计算 (从 analyzer.py 复制) ---
def calculate_max_drawdown(series):
    """计算最大回撤"""
    if series.empty: return 0.0
    rolling_max = series.cummax()
    drawdown = (rolling_max - series) / rolling_max
    return drawdown.max()

# --- 连续下跌计算 (从 analyzer.py 复制) ---
def calculate_consecutive_drops(series):
    """计算净值序列中最大的连续下跌天数"""
    if series.empty or len(series) < 2: return 0
    # True 表示今日净值 < 昨日净值 (下跌)
    drops = (series.iloc[1:].values < series.iloc[:-1].values)
    max_drop_days = 0
    current_drop_days = 0
    for is_dropped in drops:
        if is_dropped:
            current_drop_days += 1
            max_drop_days = max(max_drop_days, current_drop_days)
        else:
            current_drop_days = 0
    return max_drop_days

# --- 核心指标计算 (适应历史回测) ---
def calculate_indicators_at_date(df, current_index):
    """
    计算特定日期 (current_index) 的所有指标
    """
    # 确保有足够数据进行MA250和RSI计算
    if current_index < 250: 
        return None 
    
    # 截止到当前日期的子集 (current_index + 1 天)
    df_window = df.iloc[:current_index + 1].copy()

    # 1. RSI (14)
    delta = df_window['value'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14, min_periods=1).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14, min_periods=1).mean()
    rs = gain / loss.replace(0, np.nan) 
    df_window['RSI'] = 100 - (100 / (1 + rs))
    rsi_val = df_window['RSI'].iloc[-1]

    # 2. MA50/MA250
    df_window['MA50'] = df_window['value'].rolling(window=50, min_periods=1).mean()
    df_window['MA250'] = df_window['value'].rolling(window=250, min_periods=1).mean()
    ma50_latest = df_window['MA50'].iloc[-1]
    ma250_latest = df_window['MA250'].iloc[-1]
    
    # 趋势方向判断 (基于MA50/MA250比率的斜率)
    trend_direction = '数据不足'
    if len(df_window) >= 250:
        recent_ratio = (df_window['MA50'] / df_window['MA250']).tail(20).dropna()
        if len(recent_ratio) >= 5:
            slope = np.polyfit(np.arange(len(recent_ratio)), recent_ratio.values, 1)[0]
            if slope > 0.001: trend_direction = '向上'
            elif slope < -0.001: trend_direction = '向下'
            else: trend_direction = '平稳'

    ma50_to_ma250 = ma50_latest / ma250_latest if ma250_latest and ma250_latest != 0 else np.nan
    
    # 3. 回撤指标 (近 30 天)
    df_recent_month = df_window.tail(30)['value']
    mdd_recent_month = calculate_max_drawdown(df_recent_month)
    
    # 4. 连续下跌 (近 5 天)
    df_recent_week = df_window.tail(5)['value']
    max_drop_days_week = calculate_consecutive_drops(df_recent_week)

    return {
        'RSI': rsi_val,
        'MA50/MA250': ma50_to_ma250,
        'MA50/MA250趋势': trend_direction,
        '最大回撤(1M)': mdd_recent_month,
        '近一周连跌': max_drop_days_week
    }


# --- 策略信号生成函数 (Buy Signal) ---
def check_buy_signal(indicators):
    """
    【调试版本】检查是否触发 P2 级买入信号。
    条件：高弹性 + RSI强力超卖 (<= 35.0) + 忽略趋势健康度。
    """
    if indicators is None:
        return False

    # Filter 1: 高弹性要求 (回撤 >= 10% 且 近一周连跌 == 1)
    is_elastic = (indicators['最大回撤(1M)'] >= HIGH_ELASTICITY_MIN_DRAWDOWN) and \
                 (indicators['近一周连跌'] == 1)

    # Filter 2: RSI 强力超卖 (使用 P2 阈值 35.0)
    is_p2_oversold = indicators['RSI'] <= STRONG_RSI_THRESHOLD_P2
    
    # Filter 3: 趋势健康度检查 (DEBUG: 暂时设置为 True，跳过检查)
    is_trend_healthy = True 
    
    # 策略买入条件：高弹性 + 强力超卖 (P2) + 忽略趋势健康度
    return is_elastic and is_p2_oversold and is_trend_healthy


# --- 历史回测主函数 ---
def backtest_strategy(start_date_str, end_date_str):
    """对所有基金进行历史回测"""
    LOG = setup_logging()
    
    trades = []
    csv_files = glob.glob(os.path.join(FUND_DATA_DIR, '*.csv'))
    
    if not csv_files:
        LOG.error(f"在目录 '{FUND_DATA_DIR}' 中未找到CSV文件")
        return []

    LOG.info(f"开始回测，时间范围: {start_date_str} 至 {end_date_str}")
    
    for filepath in csv_files:
        filename = os.path.basename(filepath)
        fund_code = os.path.splitext(filename)[0]
        
        # --- 关键：排除文件 ---
        if filename in EXCLUDE_FILES:
            LOG.info(f"跳过排除列表中的文件: {filename}")
            continue

        try:
            df = pd.read_csv(filepath)
            
            # 兼容 analyzer.py 的列名
            if 'net_value' in df.columns:
                df = df.rename(columns={'net_value': 'value'})
            elif 'value' not in df.columns:
                value_cols = [col for col in df.columns if 'value' in col or '净值' in col]
                if value_cols:
                    df = df.rename(columns={value_cols[0]: 'value'})
                else:
                    raise KeyError("CSV文件缺少 'date' 或 'net_value'/'value' 列")

            # 确保 'date' 列存在
            if 'date' not in df.columns:
                raise KeyError("CSV文件缺少 'date' 列")

            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values(by='date', ascending=True).reset_index(drop=True)
            
            # 过滤回测日期范围
            df_test = df[(df['date'] >= start_date_str) & (df['date'] <= end_date_str)].copy()
            
            if df_test.empty or len(df_test) < 250:
                LOG.warning(f"基金 {fund_code} 数据不足或不在回测范围内，跳过。")
                continue

            LOG.debug(f"开始回测基金: {fund_code}")
            
            active_position = None
            
            # 遍历数据
            for i in range(len(df_test)):
                original_df_index = df_test.index[i] 
                
                current_date = df_test.iloc[i]['date']
                current_price = df_test.iloc[i]['value']
                
                # --- 1. 处理现有持仓 (Exit Logic) ---
                if active_position:
                    entry_price = active_position['entry_price']
                    entry_date = active_position['buy_date']
                    
                    # 1.1. 止损判断
                    stop_loss_price = entry_price * (1 - STOP_LOSS_PERCENT)
                    is_stop_loss = current_price <= stop_loss_price
                    
                    # 1.2. 达到固定持有期
                    entry_index = active_position['entry_index']
                    is_time_up = (i - entry_index) >= HOLDING_PERIOD
                    
                    if is_stop_loss or is_time_up:
                        exit_price = current_price
                        exit_date = current_date
                        
                        trades.append({
                            '基金代码': fund_code,
                            '买入日期': entry_date.strftime('%Y-%m-%d'),
                            '卖出日期': exit_date.strftime('%Y-%m-%d'),
                            '买入净值': entry_price,
                            '卖出净值': exit_price,
                            '收益率': (exit_price - entry_price) / entry_price,
                            '退出原因': '止损' if is_stop_loss else '周期结束'
                        })
                        active_position = None 

                # --- 2. 检查买入信号 (Buy Logic) ---
                if active_position is None:
                    indicators = calculate_indicators_at_date(df, original_df_index)
                    
                    if check_buy_signal(indicators):
                        active_position = {
                            'buy_date': current_date,
                            'entry_price': current_price,
                            'entry_index': i 
                        }
        
        # 捕获因数据格式错误导致的 KeyError
        except KeyError as e:
            LOG.error(f"处理基金 {fund_code} 时发生数据列错误: {e}")
            continue
        except Exception as e:
            LOG.error(f"处理基金 {fund_code} 时发生未知错误: {e}")
            continue

    return trades

# --- 结果分析函数 ---
def analyze_results(trades):
    """计算回测结果统计"""
    if not trades:
        return "回测结果为空，没有发生交易。" # 确保返回类型是 str

    df_trades = pd.DataFrame(trades)
    
    # 总体收益率 (简单平均)
    avg_return = df_trades['收益率'].mean()
    
    # 交易统计
    total_trades = len(df_trades)
    winning_trades = len(df_trades[df_trades['收益率'] > 0])
    losing_trades = total_trades - winning_trades
    win_rate = winning_trades / total_trades
    
    # 格式化报告
    report = [
        "## 历史回测结果\n",
        f"**总交易次数:** {total_trades}",
        f"**获胜次数:** {winning_trades}",
        f"**失败次数:** {losing_trades}",
        f"**胜率:** {win_rate:.2%}",
        f"**平均单次收益率:** {avg_return:.2%}\n",
        "### 交易详情\n",
        "| 基金代码 | 买入日期 | 卖出日期 | 退出原因 | 收益率 | 买入净值 | 卖出净值 |\n",
        "| :---: | :---: | :---: | :---: | :---: | :---: | :---: |\n"
    ]
    
    # 限制输出的交易记录数量，避免滚动条
    for _, row in df_trades.sort_values(by='收益率', ascending=False).head(50).iterrows():
        report.append(
            f"| `{row['基金代码']}` | {row['买入日期']} | {row['卖出日期']} | {row['退出原因']} | **{row['收益率']:.2%}** | {row['买入净值']:.4f} | {row['卖出净值']:.4f} |\n"
        )
    
    if total_trades > 50:
         report.append(f"|...|...|...|...|...|...|...|\n")
         report.append(f"**（仅显示收益率最高的 50 笔交易，总计 {total_trades} 笔交易）**\n")

    return "".join(report)


if __name__ == '__main__':
    # --- 回测配置 ---
    END_DATE = datetime.now() 
    START_DATE = END_DATE - timedelta(days=365) 

    START_DATE_STR = START_DATE.strftime('%Y-%m-%d')
    END_DATE_STR = END_DATE.strftime('%Y-%m-%d')

    # 执行回测
    all_trades = backtest_strategy(START_DATE_STR, END_DATE_STR)
    
    # 生成报告
    final_report = analyze_results(all_trades)
    
    # 保存报告到文件
    report_filename = f"backtest_report_{END_DATE.strftime('%Y%m%d')}.md"
    try:
        with open(report_filename, 'w', encoding='utf-8') as f:
            f.write(final_report)
    except TypeError as e:
        print(f"写入报告文件时发生错误: {e}. 最终报告内容如下:\n")
        print(final_report)
        exit(1)

    print(f"\n--- 回测报告已生成 ---\n报告文件: {report_filename}\n")
    print("脚本执行完毕。")
