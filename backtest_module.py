import pandas as pd
import numpy as np
import os
import re # 导入正则表达式库
from datetime import datetime, timedelta
# 从您的 sell_decision 模块导入必要的函数
# 注意：该文件假设 sell_decision.py 中的函数已正确实现且可用
from sell_decision import load_config, calculate_indicators, get_big_market_status, decide_sell

# --- 回测配置 ---
# 覆盖更长时间，这里假设从 2018 年开始，以便进行五年以上回测
START_DATE = '2018-01-01' 
END_DATE = datetime.now().strftime('%Y-%m-%d')
# 修正：固定初始投入资金，用于净值基准计算 (Issue 1 & 2)
INITIAL_CAPITAL = 10000.0

# --- 绩效分析函数 (保持不变) ---
def calculate_performance_metrics(nav_series, initial_capital, risk_free_rate=0.03):
    """
    计算关键绩效指标：年化收益、最大回撤、夏普比率。
    :param nav_series: 每日净值时间序列 (Series)
    :param initial_capital: 初始投入总额 (虽然函数签名中有，但实际使用 nav_series[0] 作为基准)
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
    # 确保 years 不为零
    annualized_return = (1 + total_return) ** (1 / years) - 1 if years > 0 else 0

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

# --- 历史回测核心逻辑 (已修正) ---
def run_backtest(fund_code, initial_cost_nav, params, fund_df, big_market_data, big_trend_df):
    
    # 1. 过滤回测日期范围并进行指标预处理
    fund_df = fund_df[(fund_df['date'] >= START_DATE) & (fund_df['date'] <= END_DATE)].copy()
    if fund_df.empty:
        print(f"警告: 基金 {fund_code} 在回测期内无数据。")
        return None, None

    # 预计算所有日期的指标
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
    
    # 设定初始资金和份额 (Issue 1 & 2: 固定初始资金)
    initial_investment = INITIAL_CAPITAL
    shares = initial_investment / initial_cost_nav # 计算初始份额
    cash = 0.0
    # Issue 3: 动态跟踪持仓的总成本
    total_cost = initial_investment 
    
    # 初始峰值 (用于移动止盈)
    current_peak_nav = fund_df.iloc[0]['net_value']
    
    # 3. 循环模拟每日决策
    for i in range(len(fund_df)):
        current_date = fund_df.iloc[i]['date']
        current_data_slice = fund_df.iloc[:i+1] # 截取当前及之前的数据
        
        # 获取当日净值
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
        current_holding_cost = total_cost # 当前持有资产的总成本
        
        # 修正：基于总成本计算收益 (Issue 3)
        profit = value_assets - current_holding_cost
        profit_rate = (profit / current_holding_cost) * 100 if current_holding_cost > 0 else 0
        
        holding = {
            'value': value_assets,
            # 传递当前平均成本净值
            'cost_nav': total_cost / shares if shares > 0 else 0.0, 
            'shares': shares,
            'latest_net_value': latest_nav_value,
            'profit': profit,
            'profit_rate': profit_rate,
            'current_peak': current_peak_nav
        }
        
        # 获取大盘当日状态 (Issue 4: 使用 .asof() 安全地获取数据)
        try:
            # .asof() 查找索引中小于或等于给定日期 current_date 的最后一个有效值
            big_market_latest = big_market_data.asof(current_date)
            big_trend_latest = big_trend_df.asof(current_date)
            
            # 检查是否获取到有效数据
            if big_market_latest is None or big_market_latest.empty or shares == 0:
                # 如果没有大盘数据或已清仓，则决策不进行卖出
                big_trend = '中性'
            else:
                big_trend = big_trend_latest['trend']
                
        except Exception:
            # 容错处理
            big_market_latest = pd.Series()
            big_trend = '中性'
            
        # 4. 做出决策
        # 如果已清仓，则不进行任何决策
        if shares == 0:
            decision = "持仓为零，无操作"
            decision_result = {'decision': decision}
        else:
            decision_result = decide_sell(fund_code, holding, current_data_slice, params, big_market_latest, big_market_data, big_trend)
            decision = decision_result['decision']

        # 5. 执行交易
        executed_shares = 0
        executed_amount = 0
        action = 'Hold'
        
        if '卖' in decision and shares > 0:
            sell_pct = 0.4 # T1止盈等模糊决策的默认值
            
            # Issue 5: 使用正则表达式尝试提取百分比
            match = re.search(r'卖出\s*(\d+)\s*%', decision)
            if match:
                sell_pct = float(match.group(1)) / 100.0
            elif '卖出100%' in decision or '清仓' in decision or '绝对止损' in decision and '100%' in decision:
                sell_pct = 1.0
            
            # 确保卖出百分比不超过 100%
            sell_pct = min(sell_pct, 1.0)
            
            if sell_pct > 0:
                executed_shares = shares * sell_pct
                executed_amount = executed_shares * latest_nav_value
                
                # 更新持仓和总成本 (Issue 3: 成本按比例减少)
                cost_reduction = total_cost * sell_pct
                
                cash += executed_amount
                shares -= executed_shares
                total_cost -= cost_reduction
                
                action = 'Sell'
                
        # 6. 记录交易日志
        if action == 'Sell':
            # 重新计算卖出后的平均成本净值，用于日志记录
            remaining_cost_nav = total_cost / shares if shares > 0 else 0.0
            
            transaction_log.append({
                'Date': current_date.strftime('%Y-%m-%d'),
                'Fund_Code': fund_code,
                'Action': action,
                'Shares_Change': -round(executed_shares, 2),
                'Amount_Change': round(executed_amount, 2),
                'Net_Value': round(latest_nav_value, 4),
                'Shares_Remaining': round(shares, 2),
                'Avg_Cost_Nav_Remaining': round(remaining_cost_nav, 4), # 记录剩余份额的平均成本
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
    # 假设 get_big_market_status 已经计算了 RSI 和 MA50
    big_trend_df = big_market_data[['date', 'net_value', 'ma50', 'rsi']].copy()
    
    # 计算大盘趋势状态
    big_trend_df['trend'] = np.where(
        (big_trend_df['rsi'] > 50) & (big_trend_df['net_value'] > big_trend_df['ma50']), '强势',
        np.where(
            (big_trend_df['rsi'] < 50) & (big_trend_df['net_value'] < big_trend_df['ma50']), '弱势',
            '中性'
        )
    )
    
    # Issue 4 修正：设置 date 列为索引，以支持 .asof() 查找
    big_market_data.set_index('date', inplace=True)
    big_trend_df.set_index('date', inplace=True) 
    
    # 3. 运行回测
    all_trade_logs = []
    all_performance = []
    fund_data_dir = 'fund_data/'

    for code, cost_nav in holdings_config.items():
        fund_file = os.path.join(fund_data_dir, f"{code}.csv")
        if os.path.exists(fund_file):
            fund_df = pd.read_csv(fund_file, parse_dates=['date']).sort_values('date').reset_index(drop=True)
            print(f"开始回测基金: {code} (初始成本净值: {cost_nav})")
            
            try:
                initial_cost_nav = float(cost_nav)
            except ValueError:
                print(f"警告: 基金 {code} 的成本净值 '{cost_nav}' 无法转换为浮点数，跳过。")
                continue
            
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
