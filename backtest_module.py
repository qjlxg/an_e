import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
# 从您的 sell_decision 模块导入必要的函数
from sell_decision import load_config, calculate_indicators, get_big_market_status, decide_sell

# --- 回测配置 ---
# 覆盖更长时间，这里假设从 2018 年开始，以便进行五年以上回测
START_DATE = '2018-01-01' 
END_DATE = datetime.now().strftime('%Y-%m-%d')

# --- 绩效分析函数 ---
def calculate_performance_metrics(nav_series, initial_capital, risk_free_rate=0.03):
    """
    计算关键绩效指标：年化收益、最大回撤、夏普比率。
    :param nav_series: 每日净值时间序列 (Series)
    :param initial_capital: 初始投入总额
    :param risk_free_rate: 无风险利率 (年化)
    :return: 包含指标的字典
    """
    if nav_series.empty or len(nav_series) < 2:
        return {}

    # 1. 累计收益率 (Total Return)
    total_return = (nav_series.iloc[-1] / nav_series.iloc[0]) - 1

    # 2. 年化收益率 (Annualized Return)
    start_date = nav_series.index[0]
    end_date = nav_series.index[-1]
    days = (end_date - start_date).days
    years = days / 365.25
    annualized_return = (1 + total_return) ** (1 / years) - 1

    # 3. 最大回撤 (Max Drawdown, MDD)
    cumulative_max = nav_series.cummax()
    drawdown = (nav_series / cumulative_max) - 1
    max_drawdown = drawdown.min()

    # 4. 年化波动率 (Annualized Volatility)
    # 每日收益率
    daily_returns = nav_series.pct_change().dropna()
    # 年化波动率 = 每日标准差 * sqrt(252)
    annualized_volatility = daily_returns.std() * np.sqrt(252)
    
    # 5. 夏普比率 (Sharpe Ratio)
    # 假设无风险利率 (Rf) 已年化
    if annualized_volatility == 0:
        sharpe_ratio = np.nan
    else:
        sharpe_ratio = (annualized_return - risk_free_rate) / annualized_volatility

    return {
        '总天数': days,
        '年化收益率 (%)': round(annualized_return * 100, 2),
        '最大回撤 (%)': round(abs(max_drawdown) * 100, 2),
        '年化波动率 (%)': round(annualized_volatility * 100, 2),
        '夏普比率': round(sharpe_ratio, 2),
        '累计收益率 (%)': round(total_return * 100, 2),
    }

# --- 历史回测核心逻辑 (已修改) ---
def run_backtest(fund_code, initial_cost_nav, params, fund_df, big_market_data, big_trend_df):
    
    # 1. 过滤回测日期范围并进行指标预处理
    fund_df = fund_df[(fund_df['date'] >= START_DATE) & (fund_df['date'] <= END_DATE)].copy()
    if fund_df.empty:
        print(f"警告: 基金 {fund_code} 在回测期内无数据。")
        return None, None

    # 预计算所有日期的指标，以避免循环中重复计算，提高效率
    fund_df = calculate_indicators(
        fund_df, 
        params.get('rsi_window', 14), 
        params.get('ma_window', 50), 
        params.get('bb_window', 20), 
        params.get('adx_window', 14)
    )
    
    # 2. 初始化持仓、现金和净值曲线
    transaction_log = []
    daily_nav_curve = []
    
    # 设定初始资金和份额
    initial_shares_amount = 1000 # 初始投入份额
    cash = 0.0
    shares = initial_shares_amount
    cost_nav = initial_cost_nav
    initial_investment = shares * cost_nav # 初始投入总额
    
    # 初始峰值 (用于移动止盈)
    current_peak_nav = fund_df.iloc[0]['net_value']
    
    # 假设大盘趋势预处理和指标计算在 main 中已完成
    
    # 3. 循环模拟每日决策
    for i in range(len(fund_df)):
        current_date = fund_df.iloc[i]['date']
        current_data_slice = fund_df.iloc[:i+1] # 截取当前及之前的数据
        
        # 获取当日净值和指标
        latest_nav_value = current_data_slice.iloc[-1]['net_value']
        
        # 更新峰值
        current_peak_nav = max(current_peak_nav, latest_nav_value)
        
        # 计算当日持仓状态
        value_assets = shares * latest_nav_value
        total_assets = value_assets + cash
        
        # 记录当日净值（以初始投入为基础）
        daily_nav_curve.append({
            'date': current_date,
            # 策略净值 = (总资产 / 初始投入)
            'equity_nav': total_assets / initial_investment, 
            'total_assets': total_assets 
        })
        
        # 仅在第二个交易日开始进行决策 (需要前一日数据)
        if i == 0:
            continue

        # 模拟当日持仓状态传递给决策函数
        current_cost = shares * cost_nav
        profit_rate = (value_assets - current_cost) / current_cost * 100 if current_cost > 0 else 0
        
        holding = {
            'value': value_assets,
            'cost_nav': cost_nav,
            'shares': shares,
            'latest_net_value': latest_nav_value,
            'profit': value_assets - current_cost,
            'profit_rate': profit_rate,
            'current_peak': current_peak_nav
        }
        
        # 获取大盘当日状态
        big_market_latest = big_market_data[big_market_data['date'] == current_date].iloc[-1] if current_date in big_market_data['date'].values else big_market_data.iloc[-1]
        big_trend = big_trend_df[big_trend_df['date'] == current_date].iloc[-1]['trend'] if current_date in big_trend_df['date'].values else '中性'
        
        # 4. 做出决策
        decision_result = decide_sell(fund_code, holding, current_data_slice, params, big_market_latest, big_market_data, big_trend)
        decision = decision_result['decision']

        # 5. 执行交易
        executed_shares = 0
        executed_amount = 0
        action = 'Hold'
        
        if '卖' in decision:
            # 提取卖出百分比
            try:
                if '卖出100%' in decision:
                    sell_pct = 1.0
                else:
                    sell_pct = float(decision.replace('卖', '').replace('%', '').replace('减仓', '').split()[0]) / 100.0
            except:
                # 针对 T1 止盈等特殊决策，默认取 40% (30% - 50% 范围)
                sell_pct = 0.4 
                
            executed_shares = shares * sell_pct
            executed_amount = executed_shares * latest_nav_value
            
            # 更新持仓
            cash += executed_amount
            shares -= executed_shares
            # 卖出后，成本净值不变（除非进行再投资，此处不做再投资）
            
            action = 'Sell'
            
            # 更新峰值（卖出后峰值可以重置，但移动止盈通常不需要）
            # current_peak_nav = latest_nav_value # 保持移动止盈的连续性，此处不重置

        # 6. 记录交易日志
        if action == 'Sell':
            transaction_log.append({
                'Date': current_date.strftime('%Y-%m-%d'),
                'Fund_Code': fund_code,
                'Action': action,
                'Shares_Change': -executed_shares,
                'Amount_Change': executed_amount,
                'Net_Value': round(latest_nav_value, 4),
                'Shares_Remaining': round(shares, 2),
                'Cash_Remaining': round(cash, 2),
                'Profit_Rate(%)': round(profit_rate, 2),
                'Decision_Reason': decision,
                'Total_Assets': round(total_assets, 2)
            })
            
    # 7. 汇总净值曲线和交易日志
    nav_df = pd.DataFrame(daily_nav_curve).set_index('date')
    trade_df = pd.DataFrame(transaction_log)
    
    # 计算绩效指标
    performance = calculate_performance_metrics(nav_df['equity_nav'], initial_investment)
    
    return trade_df, performance

def main():
    print(f"--- 长期绩效回测模块启动 ({START_DATE} 至 {END_DATE}) ---")
    
    # 1. 加载配置和参数
    params, holdings_config = load_config()
    
    # 2. 预加载大盘数据
    big_market_data, _, _ = get_big_market_status(params)
    
    # 预计算大盘趋势DF
    big_trend_df = big_market_data[['date']].copy()
    big_trend_df['net_value'] = big_market_data['net_value']
    big_trend_df['ma50'] = big_market_data['ma50']
    big_trend_df['rsi'] = big_market_data['rsi']
    big_trend_df['trend'] = np.where(
        (big_trend_df['rsi'] > 50) & (big_trend_df['net_value'] > big_trend_df['ma50']), '强势',
        np.where(
            (big_trend_df['rsi'] < 50) & (big_trend_df['net_value'] < big_trend_df['ma50']), '弱势',
            '中性'
        )
    )
    
    # 3. 运行回测
    all_trade_logs = []
    all_performance = []
    fund_data_dir = 'fund_data/'

    for code, cost_nav in holdings_config.items():
        fund_file = os.path.join(fund_data_dir, f"{code}.csv")
        if os.path.exists(fund_file):
            fund_df = pd.read_csv(fund_file, parse_dates=['date']).sort_values('date').reset_index(drop=True)
            print(f"开始回测基金: {code} (初始成本净值: {cost_nav})")
            
            initial_cost_nav = float(cost_nav)
            
            trade_log, performance_metrics = run_backtest(code, initial_cost_nav, params, fund_df, big_market_data, big_trend_df)
            
            if trade_log is not None:
                # 记录交易日志
                all_trade_logs.append(trade_log)
                
                # 记录绩效
                performance_metrics['基金代码'] = code
                all_performance.append(performance_metrics)
            else:
                print(f"基金 {code} 数据不足或回测失败。")

    # 4. 汇总并输出结果
    if all_performance:
        # A. 输出绩效统计报告
        performance_df = pd.DataFrame(all_performance)
        performance_output = 'backtest_performance_summary.csv'
        performance_df.to_csv(performance_output, index=False, encoding='utf_8_sig')
        print(f"\n✅ 绩效汇总报告已生成: {performance_output}")
        print("\n--- 绩效摘要 ---")
        print(performance_df[['基金代码', '年化收益率 (%)', '最大回撤 (%)', '夏普比率']])
        
        # B. 输出完整的交易日志
        if all_trade_logs:
            final_trade_df = pd.concat(all_trade_logs, ignore_index=True)
            final_trade_df = final_trade_df.sort_values(['Fund_Code', 'Date'])
            trade_log_output = 'backtest_transaction_log.csv'
            final_trade_df.to_csv(trade_log_output, index=False, encoding='utf_8_sig')
            print(f"✅ 完整交易日志已生成: {trade_log_output}")
    else:
        print("\n⚠️ 所有基金回测均失败，未生成结果文件。")

if __name__ == '__main__':
    # 设置 Pandas 显示选项
    pd.set_option('display.float_format', lambda x: '%.4f' % x)
    
    # ⚠️ 请确保您的基金数据文件 (fund_data/*.csv) 包含从 2018-01-01 开始的数据
    main()
