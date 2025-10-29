import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime, timedelta
import pytz

# --- 配置参数 (与 analyzer.py 保持一致) ---
FUND_DATA_DIR = 'fund_data'
MIN_CONSECUTIVE_DROP_DAYS = 3  # 连续下跌天数的阈值
MIN_MONTH_DRAWDOWN = 0.06      # 1个月回撤的阈值 (6%)
HIGH_ELASTICITY_MIN_DRAWDOWN = 0.10 # 高弹性筛选的最低回撤阈值 (10%)
MIN_DAILY_DROP_PERCENT = 0.03 # 当日跌幅的最低阈值 (3%)

# --- 新增回测参数 ---
START_DATE = '2020-01-01'  # 回测起始日期
END_DATE = '2024-12-31'    # 回测结束日期
INITIAL_CAPITAL = 100000   # 初始资金 (元)
UNIT_PURCHASE = 10000      # 每次买入的金额 (元)
MAX_HOLDINGS = 5           # 最大持仓基金数量
MAX_FUNDS_FOR_DEBUG = 10   # 【调试限制】限制参与回测的基金数量

# --- 费用配置 (根据用户要求修改) ---
PURCHASE_FEE_RATE = 0.0           # 申购费（买入费）：0%
REDEMPTION_FEE_RATE_SHORT = 0.015 # 赎回费：<= 7天 (1.5%)
REDEMPTION_FEE_RATE_LONG = 0.005  # 赎回费：> 7天 (0.5%)

# --- 从 analyzer.py 引入的关键计算函数 ---

def calculate_consecutive_drops(series):
    """计算时间窗口内的最长连续下跌天数"""
    if series.empty or len(series) < 2:
        return 0
    # 净值下跌：当前值 < 前一个值
    drops = (series.iloc[1:].values < series.iloc[:-1].values)
    drops_int = drops.astype(int)
    max_drop_days = 0
    current_drop_days = 0
    for val in drops_int:
        if val == 1:
            current_drop_days += 1
        else:
            max_drop_days = max(max_drop_days, current_drop_days)
            current_drop_days = 0
    max_drop_days = max(max_drop_days, current_drop_days)
    return max_drop_days

def calculate_max_drawdown(series):
    """计算最大回撤"""
    if series.empty:
        return 0.0
    rolling_max = series.cummax()
    drawdown = (rolling_max - series) / rolling_max
    mdd = drawdown.max()
    return mdd

def calculate_technical_indicators(df):
    """
    计算基金净值的RSI(14)、MACD、MA50。
    要求df必须按日期降序排列。
    """
    if 'value' not in df.columns or len(df) < 50:
        return {'RSI': np.nan, 'MACD信号': '数据不足', '最新净值': df['value'].iloc[0] if not df.empty else np.nan, '当日跌幅': np.nan}
    
    df_asc = df.iloc[::-1].copy()
    
    # 1. RSI (14)
    delta = df_asc['value'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
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

    # 3. MA50
    df_asc['MA50'] = df_asc['value'].rolling(window=50).mean()
    value_latest = df_asc['value'].iloc[-1]
    ma50_latest = df_asc['MA50'].iloc[-1]
    
    # 4. 计算当日跌幅 (T日 vs T-1日)
    daily_drop = 0.0
    if len(df_asc) >= 2:
        value_t_minus_1 = df_asc['value'].iloc[-2]
        if value_t_minus_1 > 0:
            daily_drop = (value_t_minus_1 - value_latest) / value_t_minus_1

    return {
        'RSI': rsi_latest,
        'MACD信号': macd_signal,
        '净值/MA50': value_latest / ma50_latest if ma50_latest and ma50_latest != 0 else np.nan,
        '最新净值': value_latest,
        '当日跌幅': daily_drop
    }

def load_all_fund_data():
    """加载所有基金数据并整理成 {代码: DataFrame} 字典，并限制数量。"""
    all_funds_data = {}
    # 使用 sorted() 确保每次调试时加载的基金列表是固定的，方便比较
    csv_files = sorted(glob.glob(os.path.join(FUND_DATA_DIR, '*.csv')))
    
    # 【重点修改】限制文件数量
    files_to_load = csv_files[:MAX_FUNDS_FOR_DEBUG]
    print(f"检测到 {len(csv_files)} 个基金文件，调试模式下仅加载前 {len(files_to_load)} 个文件。")
    
    for filepath in files_to_load:
        try:
            fund_code = os.path.splitext(os.path.basename(filepath))[0]
            df = pd.read_csv(filepath)
            df['date'] = pd.to_datetime(df['date'])
            df = df.rename(columns={'net_value': 'value'})
            df = df.set_index('date').sort_index()
            all_funds_data[fund_code] = df
        except Exception as e:
            print(f"加载文件 {filepath} 错误: {e}")
            continue
            
    return all_funds_data

# --- 核心回测逻辑 ---

def run_backtest():
    """执行高弹性策略回测"""
    print("--- 启动高弹性策略回测 (调试模式: 限制基金数量, 严格费用计算) ---")
    all_funds_data = load_all_fund_data()
    
    if not all_funds_data:
        print("没有可用的基金数据，回测中止。")
        return

    # 获取所有基金的交易日集合
    all_dates = pd.to_datetime([])
    for df in all_funds_data.values():
        all_dates = all_dates.union(df.index)

    start_dt = pd.to_datetime(START_DATE)
    end_dt = pd.to_datetime(END_DATE)
    trade_dates = all_dates[(all_dates >= start_dt) & (all_dates <= end_dt)].sort_values().tolist()

    # 初始化账户
    account = {
        'cash': INITIAL_CAPITAL,
        # 'purchase_date' 字段用于计算持有天数
        'holdings': {},  # {code: {'units': float, 'cost': float, 'purchase_date': date}}
        'nav_history': {start_dt: INITIAL_CAPITAL},
        'portfolio_value': INITIAL_CAPITAL
    }
    
    print(f"回测日期范围: {trade_dates[0].strftime('%Y-%m-%d')} 到 {trade_dates[-1].strftime('%Y-%m-%d')}")
    print(f"费用配置: 申购费 {PURCHASE_FEE_RATE:.2%}, 赎回费 <=7天 {REDEMPTION_FEE_RATE_SHORT:.2%}, >7天 {REDEMPTION_FEE_RATE_LONG:.2%}")
    
    # 交易模拟
    for i, date in enumerate(trade_dates):
        
        if date < start_dt:
            continue
            
        # 1. 估算当前资产净值 & 检查卖出信号
        current_value = account['cash']
        funds_to_sell = []
        
        for code, holding in list(account['holdings'].items()):
            fund_df = all_funds_data.get(code)
            if fund_df is not None and date in fund_df.index:
                latest_value = fund_df.loc[date, 'value']
                current_value += holding['units'] * latest_value
                
                cost = holding['cost'] / holding['units'] # 平均成本价

                # --- 止盈/止损/清仓 逻辑 ---
                if latest_value / cost < 0.92: # 8% 止损
                    funds_to_sell.append({'code': code, 'units': holding['units'], 'reason': '止损'})
                
                # 清仓/止盈判断需要充足数据
                if len(fund_df.loc[:date]) >= 50:
                    df_up_to_today = fund_df.loc[:date].iloc[::-1]
                    tech = calculate_technical_indicators(df_up_to_today)
                    
                    if latest_value / cost >= 1.05: # 盈利 5% 以上才考虑技术清仓
                        if tech['MACD信号'] == '死叉':
                            funds_to_sell.append({'code': code, 'units': holding['units'], 'reason': 'MACD死叉清仓'})
                        elif tech.get('净值/MA50', 2.0) < 1.0:
                            funds_to_sell.append({'code': code, 'units': holding['units'], 'reason': '净值跌破MA50清仓'})
                    
                    elif latest_value / cost >= 1.05 and tech['MACD信号'] == '金叉': # MACD金叉分批止盈
                        funds_to_sell.append({'code': code, 'units': holding['units'] * 0.5, 'reason': 'MACD金叉分批止盈'})
                        
        
        # 2. 执行卖出 (清仓/止盈) - 赎回费计算
        for sale in funds_to_sell:
            code = sale['code']
            units_to_sell = sale['units']
            
            if units_to_sell <= 1e-6:
                continue

            fund_df = all_funds_data.get(code)
            
            if fund_df is not None and code in account['holdings'] and date in fund_df.index:
                sale_value = fund_df.loc[date, 'value']
                
                # --- 计算持有天数和赎回费率 ---
                purchase_date = account['holdings'][code]['purchase_date']
                # 计算持有天数 (交易日 - 购买日)
                holding_days = (date - purchase_date).days
                
                if holding_days <= 7:
                    fee_rate = REDEMPTION_FEE_RATE_SHORT # 1.5%
                else:
                    fee_rate = REDEMPTION_FEE_RATE_LONG # 0.5%
                
                sale_amount_gross = units_to_sell * sale_value
                redemption_fee = sale_amount_gross * fee_rate
                sale_amount_net = sale_amount_gross - redemption_fee
                
                # 更新现金和持仓
                account['cash'] += sale_amount_net
                account['holdings'][code]['units'] -= units_to_sell
                
                if account['holdings'][code]['units'] <= 1e-6:
                    del account['holdings'][code]
                else:
                    # 简单按比例调整成本
                    account['holdings'][code]['cost'] *= (account['holdings'][code]['units'] / (account['holdings'][code]['units'] + units_to_sell))
                
        # 重新计算当日总资产净值
        current_value = account['cash']
        for code, holding in account['holdings'].items():
            fund_df = all_funds_data.get(code)
            if fund_df is not None and date in fund_df.index:
                current_value += holding['units'] * fund_df.loc[date, 'value']
        account['portfolio_value'] = current_value
        account['nav_history'][date] = current_value


        # 3. 每日筛选买入信号
        next_trade_date = trade_dates[i+1] if i + 1 < len(trade_dates) else None
        if next_trade_date is None:
            break
        
        potential_buys = []
        for code, df in all_funds_data.items(): 
            if date in df.index and len(df.loc[:date]) >= 50:
                df_up_to_today_desc = df.loc[:date].iloc[::-1]
                if len(df_up_to_today_desc) < 50: continue
                
                df_recent_month = df_up_to_today_desc.head(30)
                df_recent_week = df_up_to_today_desc.head(5)
                
                mdd_recent_month = calculate_max_drawdown(df_recent_month['value'])
                max_drop_days_week = calculate_consecutive_drops(df_recent_week['value'])

                # 核心预警条件
                if mdd_recent_month < MIN_MONTH_DRAWDOWN: continue
                    
                tech = calculate_technical_indicators(df_up_to_today_desc)
                rsi_val = tech.get('RSI', np.nan)
                daily_drop_val = tech.get('当日跌幅', 0.0)
                
                # 高弹性基础条件：最大回撤 >= 10% 且 近一周连跌天数 == 1 (低位企稳)
                is_base_elastic = (mdd_recent_month >= HIGH_ELASTICITY_MIN_DRAWDOWN) and (max_drop_days_week == 1)
                
                if is_base_elastic and not pd.isna(rsi_val):
                    # 🥇 第一优先级：即时恐慌买入 (RSI超卖 AND 当日大跌)
                    is_buy_signal_1 = (rsi_val < 35) and (daily_drop_val >= MIN_DAILY_DROP_PERCENT)
                    # 🥈 第二优先级：技术共振建仓 (RSI超卖 AND 当日跌幅较小)
                    is_buy_signal_2 = (rsi_val < 35) and (daily_drop_val < MIN_DAILY_DROP_PERCENT)
                    
                    if is_buy_signal_1 or is_buy_signal_2:
                        potential_buys.append({
                            'code': code,
                            'priority': 1 if is_buy_signal_1 else 2,
                            'rsi': rsi_val,
                            'daily_drop': daily_drop_val
                        })

        # 4. 执行买入 - 申购费为 0
        if potential_buys and len(account['holdings']) < MAX_HOLDINGS and account['cash'] >= UNIT_PURCHASE:
            
            # 排序： 1. 优先级 (1>2) 2. 当日跌幅 (高->低) 3. RSI (低->高)
            potential_buys.sort(key=lambda x: (-x['priority'], -x['daily_drop'], x['rsi']))
            
            for fund in potential_buys:
                code = fund['code']
                
                if code in account['holdings'] or account['cash'] < UNIT_PURCHASE:
                    continue
                fund_df = all_funds_data.get(code)
                if fund_df is None or next_trade_date not in fund_df.index:
                    continue
                
                buy_value = fund_df.loc[next_trade_date, 'value']
                
                # 申购费为 0
                net_purchase_amount = UNIT_PURCHASE 
                
                units_bought = net_purchase_amount / buy_value
                
                account['cash'] -= UNIT_PURCHASE
                
                account['holdings'][code] = {
                    'units': units_bought,
                    'cost': UNIT_PURCHASE, 
                    'purchase_date': next_trade_date # 记录交易日
                }
                
                if len(account['holdings']) >= MAX_HOLDINGS:
                    break
        
        account['nav_history'][date] = account['portfolio_value']
        
    # --- 最终业绩计算 ---
    nav_series = pd.Series(account['nav_history']).sort_index()
    if nav_series.empty or len(nav_series) < 2:
        print("\n回测数据不足，无法计算业绩。")
        return

    cumulative_return = (nav_series.iloc[-1] / nav_series.iloc[0]) - 1
    daily_returns = nav_series.pct_change().dropna()
    
    peak = nav_series.expanding().max()
    drawdown = (nav_series - peak) / peak
    max_drawdown = drawdown.min()
    
    total_days = (nav_series.index[-1] - nav_series.index[0]).days
    annualized_return = ((1 + cumulative_return) ** (365.0 / total_days)) - 1 if total_days > 0 else 0
    
    # 夏普比率 (假设无风险收益率为 2% / 252个交易日)
    risk_free_rate_daily = 0.02 / 252.0
    sharpe_ratio = (daily_returns.mean() - risk_free_rate_daily) / daily_returns.std() * np.sqrt(252)

    # --- 输出报告 ---
    print("\n" + "="*40)
    print("        🚀 策略回测报告 (最终版) 🚀")
    print("="*40)
    print(f"**回测范围**: {nav_series.index[0].strftime('%Y-%m-%d')} - {nav_series.index[-1].strftime('%Y-%m-%d')}")
    print(f"**测试基金数量**: {len(all_funds_data)} 支 (限制为 {MAX_FUNDS_FOR_DEBUG})")
    print(f"**申购费率**: {PURCHASE_FEE_RATE:.2%}")
    print(f"**赎回费率**: <=7天 {REDEMPTION_FEE_RATE_SHORT:.2%}, >7天 {REDEMPTION_FEE_RATE_LONG:.2%}")
    print("-" * 40)
    print(f"**起始资金**: {INITIAL_CAPITAL:.2f} 元")
    print(f"**最终资产**: {nav_series.iloc[-1]:.2f} 元")
    print("-" * 40)
    print(f"**累计收益率**: {cumulative_return:.2%}")
    print(f"**年化收益率**: {annualized_return:.2%}")
    print(f"**最大回撤**: {max_drawdown:.2%}")
    print(f"**夏普比率**: {sharpe_ratio:.2f}")
    print("="*40 + "\n")
    
    if account['holdings']:
        print("**当前持仓 (回测结束时):**")
        for code, holding in account['holdings'].items():
            print(f"- 基金代码: {code}, 份额: {holding['units']:.2f}, 成本: {holding['cost']:.2f}, 购买日: {holding['purchase_date'].strftime('%Y-%m-%d')}")
    else:
        print("**回测结束时无持仓。**")
        
    print("\n--- 注意: 本回测脚本已严格按照用户要求设置了交易费用和基金数量限制。 ---")
    
if __name__ == '__main__':
    run_backtest()
