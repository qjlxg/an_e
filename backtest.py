import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime, timedelta
import pytz

# --- 配置参数 (基于原脚本的双重筛选条件和策略纪律) ---
FUND_DATA_DIR = 'fund_data'  # 基金数据目录
INITIAL_CAPITAL = 100000.0   # 初始资金 (元)
TRIAL_BUY_AMOUNT = 500.0     # 试水买入金额 (元)
MAX_ADD_AMOUNT = 1000.0      # 最大加仓金额 (元)
ADD_DROP_THRESHOLD = 0.05    # 加仓触发：从试水价下跌5%
ADD_RSI_THRESHOLD = 20       # 加仓RSI阈值 <20
PROFIT_TAKE_PARTIAL = 0.05   # 分批止盈：+5%赎回50%
PROFIT_TAKE_FULL = 0.05      # 清仓止盈：利润>5%
STOP_LOSS_DROP = 0.10        # 止损：平均成本下跌10%
MIN_CONSECUTIVE_DROP_DAYS = 3  # 连续下跌天数阈值
MIN_MONTH_DRAWDOWN = 0.06     # 1个月回撤阈值 (6%)
HIGH_ELASTICITY_MIN_DRAWDOWN = 0.10  # 高弹性最低回撤 (10%)
MIN_DAILY_DROP_PERCENT = 0.03  # 当日跌幅最低阈值 (3%)
REPORT_BASE_NAME = 'fund_backtest_report'

# --- 新增：赎回费用率 ---
FEE_RATE_SHORT = 0.0015  # 持有≤7天，1.5‰
FEE_RATE_LONG = 0.0005   # 持有>7天，0.5‰

# --- 复用原脚本的函数：计算技术指标 ---
def calculate_technical_indicators(df):
    if 'value' not in df.columns or len(df) < 50:
        return {
            'RSI': np.nan, 'MACD信号': '数据不足', '净值/MA50': np.nan,
            '布林带位置': '数据不足', '最新净值': df['value'].iloc[0] if not df.empty else np.nan,
            '当日跌幅': np.nan
        }
    
    df_asc = df.iloc[::-1].copy()
    
    # RSI (14)
    delta = df_asc['value'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss.replace(0, np.nan) 
    df_asc['RSI'] = 100 - (100 / (1 + rs))
    rsi_latest = df_asc['RSI'].iloc[-1]
    
    # MACD
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

    # MA50
    df_asc['MA50'] = df_asc['value'].rolling(window=50).mean()
    ma50_latest = df_asc['MA50'].iloc[-1]
    value_latest = df_asc['value'].iloc[-1]
    net_to_ma50 = value_latest / ma50_latest if ma50_latest and ma50_latest != 0 else np.nan

    # 布林带
    df_asc['MA20'] = df_asc['value'].rolling(window=20).mean()
    df_asc['StdDev'] = df_asc['value'].rolling(window=20).std()
    ma20_latest = df_asc['MA20'].iloc[-1]
    std_latest = df_asc['StdDev'].iloc[-1]
    
    bollinger_pos = '数据不足'
    if not np.isnan(ma20_latest) and not np.isnan(std_latest):
        upper_latest = ma20_latest + (std_latest * 2)
        lower_latest = ma20_latest - (std_latest * 2)
        
        if value_latest > upper_latest:
            bollinger_pos = '上轨上方'
        elif value_latest < lower_latest:
            bollinger_pos = '下轨下方'
        elif value_latest > ma20_latest:
            bollinger_pos = '中轨上方'
        else:
            bollinger_pos = '中轨下方/中轨'
            
    # 当日跌幅
    daily_drop = 0.0
    if len(df_asc) >= 2:
        value_t_minus_1 = df_asc['value'].iloc[-2]
        if value_t_minus_1 > 0:
            daily_drop = (value_t_minus_1 - value_latest) / value_t_minus_1

    return {
        'RSI': round(rsi_latest, 2) if not np.isnan(rsi_latest) else np.nan,
        'MACD信号': macd_signal,
        '净值/MA50': round(net_to_ma50, 2) if not np.isnan(net_to_ma50) else np.nan,
        '布林带位置': bollinger_pos,
        '最新净值': round(value_latest, 4) if not np.isnan(value_latest) else np.nan,
        '当日跌幅': round(daily_drop, 4)
    }

# --- 复用原脚本的函数：计算连续下跌天数 ---
def calculate_consecutive_drops(series):
    if series.empty or len(series) < 2:
        return 0
    drops = (series.iloc[1:].values < series.iloc[:-1].values).astype(int)
    max_drop_days = 0
    current_drop_days = 0
    for val in drops:
        if val == 1:
            current_drop_days += 1
        else:
            max_drop_days = max(max_drop_days, current_drop_days)
            current_drop_days = 0
    max_drop_days = max(max_drop_days, current_drop_days)
    return max_drop_days

# --- 复用原脚本的函数：计算最大回撤 ---
def calculate_max_drawdown(series):
    if series.empty:
        return 0.0
    rolling_max = series.cummax()
    drawdown = (rolling_max - series) / rolling_max
    mdd = drawdown.max()
    return mdd

# --- 回测核心函数：对单个基金进行回测 ---
def backtest_single_fund(fund_code, df_full):
    """
    对单个基金的历史数据进行回测，模拟交易。
    df_full: 完整的历史数据，按日期升序排列。
    返回: 交易记录列表、最终资金、绩效指标。
    """
    df_full = df_full.sort_values(by='date').reset_index(drop=True)  # 确保升序
    transactions = []  # 记录交易: {'date': date, 'action': 'buy/add/sell/stop', 'amount': amount, 'price': price, 'shares': shares, 'profit': profit, 'fee': fee}
    positions = []     # 当前持仓: 列表 of {'buy_date': date, 'buy_price': price, 'shares': shares}
    capital = INITIAL_CAPITAL
    total_profit = 0.0
    max_drawdown = 0.0
    win_trades = 0
    total_trades = 0
    
    # 辅助函数：处理卖出（支持部分卖出），计算费用
    def sell_positions(current_date, current_price, sell_fraction=1.0, action='清仓止盈'):
        nonlocal capital, total_profit, total_trades, win_trades
        total_sell_amount = 0.0
        total_fee = 0.0
        total_cost = 0.0
        total_shares_sold = 0.0
        remaining_positions = []
        
        for pos in positions:
            sell_shares = pos['shares'] * sell_fraction
            sell_amount = sell_shares * current_price
            days_held = (current_date - pos['buy_date']).days
            fee_rate = FEE_RATE_SHORT if days_held <= 7 else FEE_RATE_LONG
            fee = sell_amount * fee_rate
            total_fee += fee
            total_sell_amount += sell_amount
            cost = sell_shares * pos['buy_price']
            total_cost += cost
            total_shares_sold += sell_shares
            
            # 如果部分卖出，保留剩余份额
            if sell_fraction < 1.0:
                pos['shares'] -= sell_shares
                if pos['shares'] > 0:
                    remaining_positions.append(pos)
        
        profit = (total_sell_amount - total_fee) - total_cost
        capital += total_sell_amount - total_fee
        total_profit += profit
        total_trades += 1
        if profit > 0:
            win_trades += 1
        
        transactions.append({
            'date': current_date, 
            'action': action, 
            'amount': total_sell_amount, 
            'price': current_price, 
            'shares': total_shares_sold, 
            'profit': profit,
            'fee': total_fee
        })
        
        return remaining_positions  # 返回剩余持仓
    
    # 从数据第50天开始（确保有足够数据计算指标）
    for i in range(50, len(df_full)):
        current_date = df_full['date'].iloc[i]
        df_up_to_now = df_full.iloc[:i+1].sort_values(by='date', ascending=False)  # 降序，模拟当前最新数据
        
        # 计算指标（基于截至当前日期的数据）
        df_recent_month = df_up_to_now.head(30)
        df_recent_week = df_up_to_now.head(5)
        max_drop_days_month = calculate_consecutive_drops(df_recent_month['value'])
        mdd_recent_month = calculate_max_drawdown(df_recent_month['value'])
        max_drop_days_week = calculate_consecutive_drops(df_recent_week['value'])
        tech_indicators = calculate_technical_indicators(df_up_to_now)
        
        current_price = tech_indicators['最新净值']
        daily_drop = tech_indicators['当日跌幅']
        rsi = tech_indicators['RSI']
        macd_signal = tech_indicators['MACD信号']
        net_to_ma50 = tech_indicators['净值/MA50']
        
        # 检查核心筛选条件
        if max_drop_days_month < MIN_CONSECUTIVE_DROP_DAYS or mdd_recent_month < MIN_MONTH_DRAWDOWN:
            continue  # 不满足基本预警条件，跳过
        
        # 1. 检查买入信号（无持仓时）
        if not positions:
            buy_signal_1 = (mdd_recent_month >= HIGH_ELASTICITY_MIN_DRAWDOWN and 
                            max_drop_days_week == 1 and 
                            rsi < 30 and 
                            daily_drop >= MIN_DAILY_DROP_PERCENT)
            buy_signal_2 = (mdd_recent_month >= HIGH_ELASTICITY_MIN_DRAWDOWN and 
                            max_drop_days_week == 1 and 
                            rsi < 35 and 
                            daily_drop < MIN_DAILY_DROP_PERCENT)
            
            if buy_signal_1 or buy_signal_2:
                # 试水买入（无费用）
                if capital >= TRIAL_BUY_AMOUNT:
                    shares = TRIAL_BUY_AMOUNT / current_price
                    capital -= TRIAL_BUY_AMOUNT
                    positions.append({'buy_date': current_date, 'buy_price': current_price, 'shares': shares})
                    transactions.append({'date': current_date, 'action': '试水买入', 'amount': TRIAL_BUY_AMOUNT, 'price': current_price, 'shares': shares, 'profit': 0, 'fee': 0})
        
        # 2. 检查加仓（有持仓时）
        if positions:
            # 计算平均成本和当前持仓价值
            total_shares = sum(pos['shares'] for pos in positions)
            avg_cost = sum(pos['buy_price'] * pos['shares'] for pos in positions) / total_shares
            current_value = total_shares * current_price
            drawdown_from_avg = (avg_cost - current_price) / avg_cost
            
            # 加仓条件：从试水价下跌>=5% 且 RSI<20
            trial_price = positions[0]['buy_price']  # 假设第一个是试水
            drop_from_trial = (trial_price - current_price) / trial_price
            if drop_from_trial >= ADD_DROP_THRESHOLD and rsi < ADD_RSI_THRESHOLD and len(positions) == 1:  # 只加仓一次
                if capital >= MAX_ADD_AMOUNT:
                    shares_add = MAX_ADD_AMOUNT / current_price
                    capital -= MAX_ADD_AMOUNT
                    positions.append({'buy_date': current_date, 'buy_price': current_price, 'shares': shares_add})
                    transactions.append({'date': current_date, 'action': '最大加仓', 'amount': MAX_ADD_AMOUNT, 'price': current_price, 'shares': shares_add, 'profit': 0, 'fee': 0})
        
        # 3. 检查止盈/清仓/止损（有持仓时）
        if positions:
            profit_ratio = (current_price - avg_cost) / avg_cost
            
            # 止损
            if drawdown_from_avg >= STOP_LOSS_DROP:
                positions = sell_positions(current_date, current_price, sell_fraction=1.0, action='止损清仓')
                continue
            
            # 分批止盈：MACD金叉 且 利润>=5%
            if macd_signal == '金叉' and profit_ratio >= PROFIT_TAKE_PARTIAL:
                # 赎回50%
                positions = sell_positions(current_date, current_price, sell_fraction=0.5, action='分批止盈')
            
            # 清仓：MACD死叉 或 净值<MA50 且 利润>5%
            if (macd_signal == '死叉' or net_to_ma50 < 1.0) and profit_ratio > PROFIT_TAKE_FULL:
                positions = sell_positions(current_date, current_price, sell_fraction=1.0, action='清仓止盈')
        
        # 更新最大回撤（基于总资金，忽略费用对回撤的影响）
        portfolio_value = capital + (sum(pos['shares'] for pos in positions) * current_price if positions else 0)
        if i > 50:
            prev_max = max(prev_max, portfolio_value) if 'prev_max' in locals() else portfolio_value
            dd = (prev_max - portfolio_value) / prev_max
            max_drawdown = max(max_drawdown, dd)
    
    # 回测结束，清仓剩余持仓
    if positions:
        current_date = df_full['date'].iloc[-1]
        current_price = df_full['value'].iloc[-1]
        positions = sell_positions(current_date, current_price, sell_fraction=1.0, action='结束清仓')
    
    final_capital = capital
    win_rate = win_trades / total_trades if total_trades > 0 else 0
    total_return = (final_capital - INITIAL_CAPITAL) / INITIAL_CAPITAL
    
    # 计算年化收益率（假设数据跨度）
    start_date = df_full['date'].iloc[0]
    end_date = df_full['date'].iloc[-1]
    years = (end_date - start_date).days / 365.25
    annualized_return = (1 + total_return) ** (1 / years) - 1 if years > 0 else 0
    
    return transactions, final_capital, {
        'total_return': total_return,
        'annualized_return': annualized_return,
        'max_drawdown': max_drawdown,
        'win_rate': win_rate,
        'total_trades': total_trades
    }

# --- 生成回测报告（新增费用列） ---
def generate_backtest_report(all_results, timestamp_str):
    report = f"# 基金策略回测报告 ({timestamp_str} UTC+8)\n\n"
    
    total_funds = len(all_results)
    overall_capital = sum(res['final_capital'] for res in all_results.values()) / total_funds if total_funds > 0 else INITIAL_CAPITAL  # 平均
    overall_return = (overall_capital - INITIAL_CAPITAL) / INITIAL_CAPITAL
    
    report += f"## 总体绩效\n\n"
    report += f"回测基金数量: {total_funds}\n"
    report += f"初始资金 (每基金): {INITIAL_CAPITAL:.2f} 元\n"
    report += f"平均最终资金: {overall_capital:.2f} 元\n"
    report += f"平均总收益率: {overall_return:.2%}\n\n"
    
    for fund_code, res in all_results.items():
        transactions, final_capital, metrics = res['transactions'], res['final_capital'], res['metrics']
        
        report += f"## 基金 {fund_code} 回测结果\n\n"
        report += f"最终资金: {final_capital:.2f} 元\n"
        report += f"总收益率: {metrics['total_return']:.2%}\n"
        report += f"年化收益率: {metrics['annualized_return']:.2%}\n"
        report += f"最大回撤: {metrics['max_drawdown']:.2%}\n"
        report += f"胜率: {metrics['win_rate']:.2%}\n"
        report += f"总交易次数: {metrics['total_trades']}\n\n"
        
        if transactions:
            report += "| 日期 | 动作 | 金额 | 价格 | 份额 | 利润 | 费用 |\n"
            report += "| :---: | :---: | ---: | ---: | ---: | ---: | ---: |\n"
            for tx in transactions:
                report += f"| {tx['date']} | {tx['action']} | {tx['amount']:.2f} | {tx['price']:.4f} | {tx['shares']:.2f} | {tx['profit']:.2f} | {tx['fee']:.2f} |\n"
        else:
            report += "无交易记录。\n"
        
        report += "\n---\n"
    
    return report

# --- 主函数：回测所有基金 ---
def backtest_all_funds():
    csv_files = glob.glob(os.path.join(FUND_DATA_DIR, '*.csv'))
    if not csv_files:
        print(f"警告：在目录 '{FUND_DATA_DIR}' 中未找到任何 CSV 文件。")
        return {}
    
    print(f"找到 {len(csv_files)} 个基金数据文件，开始回测...")
    
    all_results = {}
    
    for filepath in csv_files:
        try:
            fund_code = os.path.splitext(os.path.basename(filepath))[0]
            df = pd.read_csv(filepath)
            df['date'] = pd.to_datetime(df['date'])
            if len(df) < 50:
                continue
            
            transactions, final_capital, metrics = backtest_single_fund(fund_code, df)
            all_results[fund_code] = {
                'transactions': transactions,
                'final_capital': final_capital,
                'metrics': metrics
            }
        except Exception as e:
            print(f"回测基金 {fund_code} 时发生错误: {e}")
            continue
    
    return all_results

if __name__ == '__main__':
    # 获取时间戳
    tz = pytz.timezone('Asia/Shanghai')
    now = datetime.now(tz)
    timestamp_for_report = now.strftime('%Y-%m-%d %H:%M:%S')
    timestamp_for_filename = now.strftime('%Y%m%d_%H%M%S')
    DIR_NAME = now.strftime('%Y%m')
    
    os.makedirs(DIR_NAME, exist_ok=True)
    REPORT_FILE = os.path.join(DIR_NAME, f"{REPORT_BASE_NAME}_{timestamp_for_filename}.md")
    
    # 执行回测
    all_results = backtest_all_funds()
    
    # 生成报告
    report_content = generate_backtest_report(all_results, timestamp_for_report)
    
    with open(REPORT_FILE, 'w', encoding='utf-8') as f:
        f.write(report_content)
    
    print(f"回测完成，报告已保存到 {REPORT_FILE}")
