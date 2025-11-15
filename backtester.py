# backtester_v4.py (V4.4 网格策略核心逻辑回测)

import pandas as pd
import glob
import os
import numpy as np
import logging
import math
import pytz
from datetime import datetime

# --- 配置参数 (模拟 V4.4 策略设定) ---
FUND_DATA_DIR = 'fund_data'
BACKTEST_START_DATE = '2020-01-01'  # 回测起始日期
BACKTEST_END_DATE = '2024-12-31'    # 回测结束日期
INITIAL_CAPITAL = 100000.0          # 初始总资金 (包含基础仓位和预备金)
BUY_AMOUNT_PER_TRADE = 10000.0      # 每次买入金额 (模拟网格补仓金额)
REPORT_FILE_NAME = 'fund_backtest_v4_report.md'

# --- V4.4 策略核心纪律配置 ---
# 基于 V4.4 讨论，使用 RSI(6) <= 30 作为主要信号
RSI_BUY_THRESHOLD = 30.0
# 使用 V4.4 B核的网格间距作为默认补仓触发点
GRID_STEP_PERCENT = 0.04  # 价格相对平均成本下跌 4% 时触发信号过滤 (Level 1 触发)
TREND_RATIO_MIN = 0.95    # MA50/MA250 必须大于等于 0.95 (风控过滤)
STOP_LOSS_PERCENT = 0.08  # 止损阈值 (8%低于平均成本)
STOP_PROFIT_PERCENT = 0.15 # 止盈阈值 (15%高于平均成本)


# --- 复制 analyzer.py 中所需的指标函数 ---
# 确保回测和预警脚本使用相同的指标计算逻辑

def calculate_bollinger_bands(series, window=20):
    """ 计算布林带位置 (简化，仅用于完整复制 calculate_technical_indicators) """
    if len(series) < window: return "数据不足"
    df_temp = pd.DataFrame({'value': series.values})
    df_temp['MA20'] = df_temp['value'].rolling(window=window).mean()
    df_temp['STD20'] = df_temp['value'].rolling(window=window).std()
    latest_value = df_temp['value'].iloc[-1]
    latest_lower = df_temp['MA20'].iloc[-1] - (df_temp['STD20'].iloc[-1] * 2)
    latest_upper = df_temp['MA20'].iloc[-1] + (df_temp['STD20'].iloc[-1] * 2)
    # 返回一个简单的布尔值或字符串，用于趋势判断
    if latest_value <= latest_lower: return True # 触及或跌破下轨
    return False

def calculate_technical_indicators(df):
    """ 计算V4.4所需的RSI(6)和MA趋势指标 """
    df_asc = df.copy()
    if 'value' not in df_asc.columns or len(df_asc) < 60:
        return {'RSI(6)': np.nan, 'MA50/MA250': np.nan, 'MA50/MA250趋势': '数据不足'}

    delta = df_asc['value'].diff()

    # 1. RSI (6) - V4.4 核心信号
    gain_6 = (delta.where(delta > 0, 0)).rolling(window=6, min_periods=1).mean()
    loss_6 = (-delta.where(delta < 0, 0)).rolling(window=6, min_periods=1).mean()
    rs_6 = gain_6 / loss_6.replace(0, np.nan) 
    df_asc['RSI_6'] = 100 - (100 / (1 + rs_6))
    rsi_6_latest = df_asc['RSI_6'].iloc[-1]
    
    # 2. 移动平均线和趋势分析 (V4.4 趋势风控)
    df_asc['MA50'] = df_asc['value'].rolling(window=50, min_periods=1).mean()
    df_asc['MA250'] = df_asc['value'].rolling(window=250, min_periods=1).mean() 
    
    ma50_latest = df_asc['MA50'].iloc[-1]
    ma250_latest = df_asc['MA250'].iloc[-1]
    
    ma50_to_ma250 = np.nan
    trend_direction = '数据不足'
    
    if len(df_asc) >= 250 and ma250_latest and ma250_latest != 0:
        ma50_to_ma250 = ma50_latest / ma250_latest
        
        # MA50/MA250 趋势方向判断 (复制 analyzer.py 逻辑)
        recent_ratio = (df_asc['MA50'] / df_asc['MA250']).tail(20).dropna()
        if len(recent_ratio) >= 5:
            slope = np.polyfit(np.arange(len(recent_ratio)), recent_ratio.values, 1)[0]
            if slope > 0.001: trend_direction = '向上'
            elif slope < -0.001: trend_direction = '向下'
            else: trend_direction = '平稳'

    return {
        'RSI(6)': round(rsi_6_latest, 2) if not math.isnan(rsi_6_latest) else np.nan,
        'MA50/MA250': round(ma50_to_ma250, 2) if not math.isnan(ma50_to_ma250) else np.nan, 
        'MA50/MA250趋势': trend_direction,
    }

def calculate_max_drawdown(series):
    """ 计算最大回撤 """
    if series.empty: return 0.0
    rolling_max = series.cummax()
    drawdown = (rolling_max - series) / rolling_max
    return drawdown.max()

# --- V4.4 核心回测逻辑 ---

def run_backtest_v4(df_fund, fund_code):
    """
    对单只基金运行 V4.4 网格补仓策略。
    策略：(跌幅 >= 4%) AND (RSI(6) <= 30) AND (趋势 OK) 时，买入固定金额。
    卖出：达到止盈或止损时，卖出所有持仓。
    """
    df = df_fund.copy()
    
    # 1. 筛选回测周期并计算指标
    df = df[(df['date'] >= BACKTEST_START_DATE) & (df['date'] <= BACKTEST_END_DATE)].copy()
    if df.empty or len(df) < 250: # V4.4 策略依赖 MA250，因此数据不足时跳过
        logging.warning(f"基金 {fund_code} 数据不足 250 条，跳过 V4.4 回测。")
        return None

    df_tech = pd.DataFrame([calculate_technical_indicators(df.iloc[:i+1]) for i in range(len(df))])
    df = pd.concat([df.reset_index(drop=True), df_tech], axis=1)
    
    df = df.dropna(subset=['RSI(6)']).reset_index(drop=True)
    if df.empty: return None

    # 2. 初始化回测变量
    initial_capital = INITIAL_CAPITAL
    cash = initial_capital
    shares = 0.0        # 持有份额
    avg_cost_per_share = 0.0 # 平均持仓成本（每份额）
    
    trade_log = []
    equity_values = []
    
    # 3. 逐日回测
    for index, row in df.iterrows():
        current_date = row['date']
        current_value = row['value']
        current_rsi_6 = row['RSI(6)']
        ma_ratio = row['MA50/MA250']
        trend_dir = row['MA50/MA250趋势']
        
        # 计算当前总资产 (净值 * 份额 + 现金)
        market_value = shares * current_value
        total_equity = cash + market_value
        equity_values.append(total_equity)

        # --- 卖出判断 (止盈/止损) ---
        if shares > 0:
            current_holding_cost = shares * avg_cost_per_share
            current_profit_ratio = (market_value - current_holding_cost) / current_holding_cost
            
            # 止损信号: 跌幅 >= 8% (STOP_LOSS_PERCENT)
            if current_profit_ratio <= -STOP_LOSS_PERCENT:
                # 执行清仓
                sale_amount = market_value
                cash += sale_amount
                trade_log.append({
                    'Date': current_date, 'Action': 'SELL (Stop Loss)', 
                    'Shares': shares, 'Value': current_value,
                    'Gain_Ratio': current_profit_ratio, 'Equity': total_equity
                })
                shares = 0.0
                avg_cost_per_share = 0.0
                continue 

            # 止盈信号: 涨幅 >= 15% (STOP_PROFIT_PERCENT)
            if current_profit_ratio >= STOP_PROFIT_PERCENT:
                # 执行清仓
                sale_amount = market_value
                cash += sale_amount
                trade_log.append({
                    'Date': current_date, 'Action': 'SELL (Take Profit)', 
                    'Shares': shares, 'Value': current_value,
                    'Gain_Ratio': current_profit_ratio, 'Equity': total_equity
                })
                shares = 0.0
                avg_cost_per_share = 0.0
                continue 
        
        # --- V4.4 买入判断 (网格 & 信号 & 趋势) ---
        
        # 1. 初始建仓（模拟任务驱动，仅执行一次，占总资金的约 10%）
        if shares == 0 and cash >= BUY_AMOUNT_PER_TRADE:
            buy_shares = BUY_AMOUNT_PER_TRADE / current_value
            shares += buy_shares
            avg_cost_per_share = current_value
            cash -= BUY_AMOUNT_PER_TRADE
            trade_log.append({
                'Date': current_date, 'Action': 'BUY (Initial)', 
                'Shares': buy_shares, 'Value': current_value,
                'RSI': current_rsi_6, 'Equity': total_equity
            })
            continue # 完成交易，跳过当日补仓判断
            
        # 2. 网格补仓（信号驱动）
        if shares > 0 and cash >= BUY_AMOUNT_PER_TRADE:
            
            # 2.1. 趋势安全垫过滤 (趋势为向下 或 MA50/MA250 比值过低时，放弃补仓)
            if trend_dir == '向下' or ma_ratio < TREND_RATIO_MIN:
                continue

            # 2.2. 价格到位 (网格触发 - Level 1)
            # 价格必须相对平均成本下跌达到网格步长
            current_drop_from_avg = (avg_cost_per_share - current_value) / avg_cost_per_share
            if current_drop_from_avg < GRID_STEP_PERCENT:
                continue # 跌幅不足，跳过

            # 2.3. 质量过滤 (RSI(6) 极值 - Level 2)
            if current_rsi_6 <= RSI_BUY_THRESHOLD:
                # 触发买入
                buy_shares = BUY_AMOUNT_PER_TRADE / current_value
                
                # 更新成本和份额
                total_buy_cost = shares * avg_cost_per_share + BUY_AMOUNT_PER_TRADE
                shares += buy_shares
                avg_cost_per_share = total_buy_cost / shares
                cash -= BUY_AMOUNT_PER_TRADE
                
                trade_log.append({
                    'Date': current_date, 'Action': 'BUY (Grid)', 
                    'Shares': buy_shares, 'Value': current_value,
                    'RSI(6)': current_rsi_6, 'Trend': trend_dir, 'Equity': total_equity
                })

    # --- 最终结算与性能指标计算 ---
    
    final_equity = cash + shares * df['value'].iloc[-1]
    equity_values[-1] = final_equity
    
    df_equity = pd.Series(equity_values, index=df['date'])
    df_equity = df_equity.replace(0, np.nan).dropna()
    
    total_return = (final_equity - initial_capital) / initial_capital
    max_drawdown = calculate_max_drawdown(df_equity)
    
    # 简化年化收益率和夏普比率计算
    years = (df_equity.index[-1] - df_equity.index[0]).days / 365.25
    annual_return = (1 + total_return) ** (1 / years) - 1 if years > 0 else 0
    
    daily_returns = df_equity.pct_change().dropna()
    annual_volatility = daily_returns.std() * np.sqrt(252)
    risk_free_rate = 0.02
    sharpe_ratio = (annual_return - risk_free_rate) / annual_volatility if annual_volatility != 0 else np.nan

    return {
        '基金代码': fund_code,
        '起始资金': initial_capital,
        '最终资产': round(final_equity, 2),
        '总收益率': round(total_return, 4),
        '最大回撤': round(max_drawdown, 4),
        '年化收益率': round(annual_return, 4),
        '夏普比率': round(sharpe_ratio, 2),
        '买入次数': len([t for t in trade_log if 'BUY' in t['Action']]),
        '卖出次数': len([t for t in trade_log if 'SELL' in t['Action']])
    }

# --- 数据加载、报告生成和主函数 (与原脚本类似，但更新配置和运行函数) ---

def load_fund_data(filepath, fund_code):
    """ 加载和清洗数据 """
    try:
        # 尝试默认 UTF-8 编码加载，并检查列名
        df = pd.read_csv(filepath)
    except UnicodeDecodeError:
        df = pd.read_csv(filepath, encoding='gbk')
    except Exception as e:
        logging.error(f"加载基金 {filepath} 失败: {e}")
        return None

    # 检查关键列是否存在（与您提供的 008327.csv 格式兼容）
    if 'date' not in df.columns or 'net_value' not in df.columns:
        return None
        
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(by='date', ascending=True).reset_index(drop=True)
    df = df.rename(columns={'net_value': 'value'})
    
    if len(df) < 250: # V4.4 策略依赖 MA250，数据不足时返回 None
         return None
         
    return df

def generate_backtest_report(df_results):
    """ 生成 V4.4 回测报告 Markdown 文件 """
    report_parts = []
    
    report_parts.extend([
        f"# V4.4 网格策略回测报告 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})\n\n",
        f"**回测周期:** {BACKTEST_START_DATE} 至 {BACKTEST_END_DATE}\n",
        f"**策略:** V4.4 双重过滤网格\n",
        f"**买入信号 (需同时满足):**\n",
        f"1. **价格到位 (Level 1)**: 相对平均成本下跌 $\\ge {GRID_STEP_PERCENT*100:.0f}\\%$\n",
        f"2. **质量过滤 (Level 2)**: RSI(6) $\\le {RSI_BUY_THRESHOLD:.0f}$\n",
        f"3. **趋势过滤 (风控)**: MA50/MA250 $\\ge {TREND_RATIO_MIN:.2f}$ 且趋势非 '向下'\n",
        f"**风控:** 止损 $\\le -{STOP_LOSS_PERCENT*100:.0f}\\%$；止盈 $\\ge {STOP_PROFIT_PERCENT*100:.0f}\\%$；每次补仓 $\\yen {BUY_AMOUNT_PER_TRADE:.0f}$。\n\n",
        f"## 📊 总体性能指标\n\n"
    ])

    TABLE_HEADER = "| 基金代码 | 最终资产 (¥) | **总收益率** | **年化收益率** | 最大回撤 | **夏普比率** | 总交易次数 |\n"
    TABLE_SEPARATOR = "| :---: | :---: | :---: | :---: | :---: | :---: | :---: |\n"
    report_parts.append(TABLE_HEADER)
    report_parts.append(TABLE_SEPARATOR)

    for index, row in df_results.iterrows():
        total_trades = int(row['买入次数']) + int(row['卖出次数'])
        report_parts.append(
            f"| `{row['基金代码']}` | {row['最终资产']:.2f} | **{row['总收益率']:.2%}** | **{row['年化收益率']:.2%}** | "
            f"{row['最大回撤']:.2%} | **{row['夏普比率']:.2f}** | {total_trades} |\n"
        )
        
    with open(REPORT_FILE_NAME, 'w', encoding='utf-8') as f:
        f.write("".join(report_parts))
        
    logging.info(f"V4.4 回测完成，报告已保存到 {REPORT_FILE_NAME}")


def main_backtester():
    """ V4.4 回测主函数 """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
    logging.info("--- V4.4 网格策略回测脚本启动 ---")
    
    csv_files = glob.glob(os.path.join(FUND_DATA_DIR, '*.csv'))
    if not csv_files:
        logging.error(f"在目录 '{FUND_DATA_DIR}' 中未找到CSV文件。")
        return

    results = []
    
    for filepath in csv_files:
        fund_code = os.path.splitext(os.path.basename(filepath))[0]
        logging.info(f"开始回测基金: {fund_code}...")
        
        df_fund = load_fund_data(filepath, fund_code)
        if df_fund is not None:
            backtest_result = run_backtest_v4(df_fund, fund_code)
            if backtest_result:
                results.append(backtest_result)
    
    if results:
        df_results = pd.DataFrame(results).sort_values(by='夏普比率', ascending=False)
        generate_backtest_report(df_results)
    else:
        logging.info("没有基金数据满足 V4.4 回测要求 (数据需 > 250 条)。")

if __name__ == '__main__':
    main_backtester()
    print("V4.4 回测脚本执行完毕。")
