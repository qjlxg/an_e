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
MA_HEALTH_THRESHOLD = 0.95           # 趋势健康度阈值 (MA50/MA250 >= 0.95)
STOP_LOSS_PERCENT = 0.10             # 止损阈值 (10%)
HOLDING_PERIOD = 60                  # 固定持有周期 (60个交易日)

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
    """检查是否触发 P1 级买入信号"""
    if indicators is None:
        return False

    # Filter 1: 高弹性要求 (回撤 >= 10% 且 近一周连跌 == 1)
    is_elastic = (indicators['最大回撤(1M)'] >= HIGH_ELASTICITY_MIN_DRAWDOWN) and \
                 (indicators['近一周连跌'] == 1)

    # Filter 2: RSI P1 极度超卖
    is_p1_oversold = indicators['RSI'] <= EXTREME_RSI_THRESHOLD_P1
    
    # Filter 3: 趋势健康度检查
    is_trend_healthy = (indicators['MA50/MA250'] >= MA_HEALTH_THRESHOLD) and \
                       (indicators['MA50/MA250趋势'] != '向下')
    
    # 策略买入条件：高弹性 + 极度超卖 + 趋势健康
    return is_elastic and is_p1_oversold and is_trend_healthy


# --- 历史回测主函数 ---
def backtest_strategy(start_date_str, end_date_str):
    """对所有基金进行历史回测"""
    LOG = setup_logging()
    
    # 报告存储结构
    trades = []
    
    # 找到所有基金文件
    csv_files = glob.glob(os.path.join(FUND_DATA_DIR, '*.csv'))
    if not csv_files:
        LOG.error(f"在目录 '{FUND_DATA_DIR}' 中未找到CSV文件")
        return []

    LOG.info(f"开始回测，时间范围: {start_date_str} 至 {end_date_str}")
    
    for filepath in csv_files:
        fund_code = os.path.splitext(os.path.basename(filepath))[0]
        try:
            df = pd.read_csv(filepath)
            df['date'] = pd.to_datetime(df['date'])
            df = df.rename(columns={'net_value': 'value'}).sort_values(by='date', ascending=True).reset_index(drop=True)
            
            # 过滤回测日期范围
            df_test = df[(df['date'] >= start_date_str) & (df['date'] <= end_date_str)].copy()
            
            if df_test.empty or len(df_test) < 250:
                continue

            LOG.debug(f"开始回测基金: {fund_code}")
            
            # 交易记录
            active_position = None # {buy_date, entry_price, entry_index}
            
            # 遍历数据，从第一个有足够指标数据计算的日期开始
            for i in range(len(df_test)):
                current_date = df_test.iloc[i]['date']
                current_price = df_test.iloc[i]['value']
                
                # --- 1. 处理现有持仓 (Exit Logic) ---
                if active_position:
                    entry_price = active_position['entry_price']
                    entry_date = active_position['buy_date']
                    
                    # 1.1. 止损判断
                    stop_loss_price = entry_price * (1 - STOP_LOSS_PERCENT)
                    is_stop_loss = current_price <= stop_loss_price
                    
                    # 1.2. 达到固定持有期 (使用 index 差值近似交易日)
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
                        active_position = None # 清除持仓
                        LOG.debug(f"{fund_code} 卖出 ({'止损' if is_stop_loss else '周期结束'})")
                        # 卖出后当日不再买入

                # --- 2. 检查买入信号 (Buy Logic) ---
                if active_position is None:
                    # 必须计算当日指标，从原始 df 中切片以确保指标计算的准确性
                    indicators = calculate_indicators_at_date(df, df_test.index[i])
                    
                    if check_buy_signal(indicators):
                        active_position = {
                            'buy_date': current_date,
                            'entry_price': current_price,
                            'entry_index': i 
                        }
                        LOG.debug(f"{fund_code} 买入信号 ({current_date.strftime('%Y-%m-%d')})")
        
        except Exception as e:
            LOG.error(f"处理基金 {fund_code} 时发生错误: {e}")
            continue

    return trades

# --- 结果分析函数 ---
def analyze_results(trades):
    """计算回测结果统计"""
    if not trades:
        return "回测结果为空，没有发生交易。", 0.0, 0, 0, 0.0

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
    # 假设回测过去一年的数据
    END_DATE = datetime.now() 
    START_DATE = END_DATE - timedelta(days=365) 

    # 格式化日期字符串，用于数据加载
    START_DATE_STR = START_DATE.strftime('%Y-%m-%d')
    END_DATE_STR = END_DATE.strftime('%Y-%m-%d')

    # 执行回测
    all_trades = backtest_strategy(START_DATE_STR, END_DATE_STR)
    
    # 生成报告
    final_report = analyze_results(all_trades)
    
    # 保存报告到文件
    report_filename = f"backtest_report_{END_DATE.strftime('%Y%m%d')}.md"
    with open(report_filename, 'w', encoding='utf-8') as f:
        f.write(final_report)

    print(f"\n--- 回测报告已生成 ---\n报告文件: {report_filename}\n")
    print(final_report)
    print("------------------------\n脚本执行完毕。")
