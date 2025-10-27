import pandas as pd
import numpy as np
import os
import json
from datetime import datetime, timedelta

# 加载配置文件
# 注意: 运行此脚本需要 'holdings_config.json' 文件
# with open('holdings_config.json', 'r', encoding='utf-8') as f:
#     holdings_config = json.load(f)

# 假设 holdings_config.json 文件内容如下，以便脚本可以运行和测试
holdings_config = {
    "parameters": {
        "rsi_window": 14,
        "ma_window": 50,
        "bb_window": 20,
        "rsi_overbought_threshold": 80,
        "consecutive_days_threshold": 3,
        "profit_lock_days": 14,
        "volatility_window": 7,
        "volatility_threshold": 0.03,
        "decline_days_threshold": 5,
        "trailing_stop_loss_pct": 0.08, # 移动止盈回撤比例 8%
        "macd_divergence_window": 60,
        "adx_window": 14,
        "adx_threshold": 30
    },
    "161005": 1.2500, # 基金代码: 成本净值
    "001186": 1.5000 
}

# 获取可配置参数
params = holdings_config.get('parameters', {})
rsi_window = params.get('rsi_window', 14)
ma_window = params.get('ma_window', 50)
bb_window = params.get('bb_window', 20)
rsi_overbought_threshold = params.get('rsi_overbought_threshold', 80)
consecutive_days_threshold = params.get('consecutive_days_threshold', 3)
profit_lock_days = params.get('profit_lock_days', 14)
volatility_window = params.get('volatility_window', 7)
volatility_threshold = params.get('volatility_threshold', 0.03)
decline_days_threshold = params.get('decline_days_threshold', 5)
trailing_stop_loss_pct = params.get('trailing_stop_loss_pct', 0.08)
macd_divergence_window = params.get('macd_divergence_window', 60)
adx_window = params.get('adx_window', 14)
adx_threshold = params.get('adx_threshold', 30)

# 数据路径
big_market_path = 'index_data/000300.csv'
fund_data_dir = 'fund_data/'

# --- 模拟加载数据（实际运行时需要确保文件存在）---
# 模拟大盘数据
if not os.path.exists(big_market_path):
    # 模拟数据，以便脚本可以运行
    dates = pd.date_range(end=datetime.now(), periods=100, freq='D')
    net_values = np.linspace(1.0, 1.5, 100) + np.random.randn(100) * 0.02
    big_market = pd.DataFrame({'date': dates, 'net_value': net_values.cumsum() * 0.01 + 1})
    big_market = big_market.sort_values('date').reset_index(drop=True)
    print("注意: 大盘数据文件未找到，使用模拟数据。")
else:
    big_market = pd.read_csv(big_market_path, parse_dates=['date'])
    big_market = big_market.sort_values('date').reset_index(drop=True)
    if big_market['net_value'].max() > 10 or big_market['net_value'].min() < 0:
        print("警告: 大盘数据净值异常，请检查 index_data/000300.csv")
# ------------------------------------------------

# 计算ADX指标
def calculate_adx(df, window):
    df = df.copy()
    # 简化：使用净值作为高、低、收盘价的近似
    high = df['net_value']
    low = df['net_value']
    close = df['net_value']
    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    df['tr'] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df['+dm'] = (high - high.shift(1)).apply(lambda x: x if x > 0 else 0)
    df['-dm'] = (low.shift(1) - low).apply(lambda x: x if x > 0 else 0)
    df['+dm'] = np.where((df['+dm'] > df['-dm']), df['+dm'], 0)
    df['-dm'] = np.where((df['+dm'] < df['-dm']), df['-dm'], 0)
    atr = df['tr'].ewm(span=window, adjust=False, min_periods=window).mean()
    pdm = df['+dm'].ewm(span=window, adjust=False, min_periods=window).mean()
    mdm = df['-dm'].ewm(span=window, adjust=False, min_periods=window).mean()
    # 避免除以零
    pdi = np.where(atr != 0, (pdm / atr) * 100, 0)
    mdi = np.where(atr != 0, (mdm / atr) * 100, 0)
    
    pdi_plus_mdi = pdi + mdi
    dx = np.where(pdi_plus_mdi != 0, (abs(pdi - mdi) / pdi_plus_mdi) * 100, 0)
    
    df['adx'] = pd.Series(dx).ewm(span=window, adjust=False, min_periods=window).mean()
    return df['adx']

# 计算指标（新增布林带上轨突破计数）
def calculate_indicators(df, rsi_win, ma_win, bb_win, adx_win):
    df = df.copy()
    delta = df['net_value'].diff()
    # 使用ewm平滑RSI计算，更符合经典RSI的平滑逻辑
    up = delta.where(delta > 0, 0)
    down = -delta.where(delta < 0, 0)
    avg_up = up.ewm(com=rsi_win - 1, adjust=False, min_periods=rsi_win).mean()
    avg_down = down.ewm(com=rsi_win - 1, adjust=False, min_periods=rsi_win).mean()
    rs = avg_up / avg_down
    # 处理 rs 为 0 或 Inf 的情况
    rs.replace([np.inf, -np.inf], np.nan, inplace=True)
    rs.fillna(0, inplace=True)
    
    df['rsi'] = 100 - (100 / (1 + rs))
    
    df['ma50'] = df['net_value'].rolling(window=ma_win, min_periods=1).mean()
    exp12 = df['net_value'].ewm(span=12, adjust=False).mean()
    exp26 = df['net_value'].ewm(span=26, adjust=False).mean()
    df['macd'] = 2 * (exp12 - exp26) # 默认使用2倍平滑，保持与原代码一致
    df['signal'] = df['macd'].ewm(span=9, adjust=False).mean()
    df['bb_mid'] = df['net_value'].rolling(window=bb_win, min_periods=1).mean()
    df['bb_std'] = df['net_value'].rolling(window=bb_win, min_periods=1).std()
    df['bb_upper'] = df['bb_mid'] + (df['bb_std'] * 2)
    df['bb_lower'] = df['bb_mid'] - (df['bb_std'] * 2)
    df['daily_return'] = df['net_value'].pct_change()
    df['volatility'] = df['daily_return'].rolling(window=bb_win).std()
    # 新增：布林带上轨突破标志
    df['bb_break_upper'] = df['net_value'] > df['bb_upper']
    if len(df) > adx_win:
        df['adx'] = calculate_adx(df, adx_win)
    else:
        df['adx'] = np.nan
    return df

# 加载大盘指标
big_market_data = calculate_indicators(big_market, rsi_window, ma_window, bb_window, adx_window)
big_market_latest = big_market_data.iloc[-1]

# 动态调整波动率
volatility_window_adjusted = volatility_window
# 优化：大盘趋势判断更准确，同时影响波动率阈值
big_nav = big_market_latest['net_value']
big_ma50 = big_market_latest['ma50']

if big_market_latest['rsi'] > 50 and big_nav > big_ma50: # 强势
    volatility_threshold = 0.03
    big_trend = '强势'
elif big_market_latest['rsi'] < 50 and big_nav < big_ma50: # 弱势
    volatility_threshold = 0.025
    volatility_window_adjusted = 10
    big_trend = '弱势'
else: # 中性/震荡
    volatility_threshold = 0.03
    big_trend = '中性'

# 大盘趋势
big_rsi = big_market_latest['rsi']
big_macd = big_market_latest['macd']
big_signal = big_market_latest['signal']
macd_dead_cross = False
if len(big_market_data) >= 2:
    recent_macd = big_market_data.tail(2)
    # 优化：如果大盘趋势已经判定为弱势，则无需额外增加判断
    if big_trend != '弱势':
        if (recent_macd['macd'] < recent_macd['signal']).all():
            macd_dead_cross = True
            if big_rsi > 75:
                 big_trend = '弱势' # 强势中的快速死叉判定为弱势
        
# 加载基金数据
fund_nav_data = {}
holdings = {}
for code, cost_nav in holdings_config.items():
    if code == 'parameters':
        continue
    # --- 模拟基金数据加载（实际运行时需要确保文件存在）---
    fund_file = os.path.join(fund_data_dir, f"{code}.csv")
    if os.path.exists(fund_file):
        fund_df = pd.read_csv(fund_file, parse_dates=['date'])
        if fund_df['net_value'].max() > 10 or fund_df['net_value'].min() < 0:
            print(f"警告: 基金 {code} 数据净值异常，请检查 {fund_file}")
    else:
        # 模拟数据，以便脚本可以运行
        dates = pd.date_range(end=datetime.now(), periods=100, freq='D')
        # 针对 161005 模拟一个上涨后回撤的数据，以便测试移动止盈
        if code == '161005':
            net_values = np.linspace(1.25, 1.60, 90)
            net_values = np.append(net_values, np.linspace(1.55, 1.50, 10)) # 从 1.60 回撤到 1.50
        # 针对 001186 模拟一个亏损数据，以便测试绝对止损
        elif code == '001186':
             net_values = np.linspace(1.50, 1.20, 100) # 从 1.50 亏损到 1.20
        else:
            net_values = np.linspace(1.0, 1.5, 100)
            
        fund_df = pd.DataFrame({'date': dates, 'net_value': net_values + np.random.randn(100) * 0.005})
        fund_df = fund_df.sort_values('date').reset_index(drop=True)
        print(f"注意: 基金数据文件 {fund_file} 未找到，使用模拟数据。")
    # ------------------------------------------------

    # 使用调整后的波动率窗口
    full_fund_data = calculate_indicators(fund_df, rsi_window, ma_window, volatility_window_adjusted, adx_window)
    fund_nav_data[code] = full_fund_data
    latest_nav_data = full_fund_data.iloc[-1]
    latest_nav_value = float(latest_nav_data['net_value'])
    shares = 1
    value = shares * latest_nav_value
    cost = shares * cost_nav
    profit = value - cost
    profit_rate = (profit / cost) * 100 if cost > 0 else 0
    full_fund_data['rolling_peak'] = full_fund_data['net_value'].cummax()
    current_peak = full_fund_data['rolling_peak'].iloc[-1]
    holdings[code] = {
        'value': value,
        'cost_nav': cost_nav,
        'shares': shares,
        'latest_net_value': latest_nav_value,
        'profit': profit,
        'profit_rate': profit_rate,
        'current_peak': current_peak
    }

# 决策函数（新增布林带突破计数和净值目标计算）
def decide_sell(code, holding, full_fund_data, big_market_latest, big_market_data, big_trend):
    print(f"\n处理基金: {code}")
    profit_rate = holding['profit_rate']
    latest_net_value = holding['latest_net_value']
    cost_nav = holding['cost_nav']
    current_peak = holding['current_peak'] # 当前历史最高净值
    fund_latest = full_fund_data.iloc[-1]
    rsi = fund_latest['rsi']
    macd = fund_latest['macd']
    signal = fund_latest['signal']
    ma50 = fund_latest['ma50']
    adx = fund_latest['adx']
    
    # --- 修复 UnboundLocalError: 在函数开头设置默认值 ---
    bb_pos = '未知'
    macd_signal = '未知'
    macd_zero_dead_cross = False
    
    # --- 新增：止盈/止损净值目标计算 ---
    target_nav = {
        'trailing_stop_nav': round(current_peak * (1 - trailing_stop_loss_pct), 4),
        'abs_stop_20_nav': round(cost_nav * (1 - 0.20), 4),
        'abs_stop_15_nav': round(cost_nav * (1 - 0.15), 4),
        'short_drawdown_nav': np.nan # 后面计算
    }
    # --------------------------------------------------
    
    # 辅助信息
    sell_reasons = []

    # 分级止损（新增）
    if profit_rate < -20:
        sell_reasons.append(f'绝对止损（亏损>20%，触发净值: {target_nav["abs_stop_20_nav"]}）触发，卖出100%')
        return {
            'code': code,
            'profit_rate': round(profit_rate, 2),
            'rsi': round(rsi, 2),
            'macd_signal': macd_signal,
            'bb_pos': bb_pos,
            'big_trend': big_trend,
            'decision': '因绝对止损（亏损>20%）卖出100%',
            'target_nav': target_nav
        }
    elif profit_rate < -15:
        sell_reasons.append(f'亏损>15%（触发净值: {target_nav["abs_stop_15_nav"]}）触发，减仓50%')
        return {
            'code': code,
            'profit_rate': round(profit_rate, 2),
            'rsi': round(rsi, 2),
            'macd_signal': macd_signal,
            'bb_pos': bb_pos,
            'big_trend': big_trend,
            'decision': '因亏损>15%减仓50%',
            'target_nav': target_nav
        }
    elif profit_rate < -10:
        sell_reasons.append('亏损>10%触发，暂停定投')
        return {
            'code': code,
            'profit_rate': round(profit_rate, 2),
            'rsi': round(rsi, 2),
            'macd_signal': macd_signal,
            'bb_pos': bb_pos,
            'big_trend': big_trend,
            'decision': '暂停定投',
            'target_nav': target_nav
        }

    # --- 变量赋值和趋势判断（现在位于止损判断之后） ---
    
    if len(full_fund_data) >= 2:
        recent_data = full_fund_data.tail(2)
        if (recent_data['net_value'] > recent_data['bb_upper']).all():
            bb_pos = '上轨'
        elif (recent_data['net_value'] < recent_data['bb_lower']).all():
             bb_pos = '下轨'
        else:
            bb_pos = '中轨'
            
    macd_signal = '金叉'
    if len(full_fund_data) >= 2:
        recent_macd = full_fund_data.tail(2)
        if (recent_macd['macd'] < recent_macd['signal']).all():
            macd_signal = '死叉'
            if (recent_macd.iloc[-1]['macd'] < 0 and recent_macd.iloc[-1]['signal'] < 0):
                macd_zero_dead_cross = True
    
    # --- 高优先级规则 ---

    # 移动止盈
    drawdown = (current_peak - latest_net_value) / current_peak
    if drawdown > trailing_stop_loss_pct:
        sell_reasons.append(f'移动止盈（高点 {round(current_peak, 4)}，触发净值: {target_nav["trailing_stop_nav"]}）触发')
        return {
            'code': code,
            'profit_rate': round(profit_rate, 2),
            'rsi': round(rsi, 2),
            'macd_signal': macd_signal,
            'bb_pos': bb_pos,
            'big_trend': big_trend,
            'decision': '因移动止盈卖出',
            'target_nav': target_nav
        }

    # MACD 顶背离 (逻辑不变)
    if len(full_fund_data) >= macd_divergence_window:
        recent_data = full_fund_data.tail(macd_divergence_window)
        if not recent_data.empty:
            is_nav_peak = (full_fund_data['net_value'].iloc[-1] == current_peak)
            is_macd_divergence = is_nav_peak and (recent_data['macd'].iloc[-1] < recent_data['macd'].max())
            
            if is_macd_divergence:
                sell_reasons.append('MACD顶背离触发')
                return {
                    'code': code,
                    'profit_rate': round(profit_rate, 2),
                    'rsi': round(rsi, 2),
                    'macd_signal': macd_signal,
                    'bb_pos': bb_pos,
                    'big_trend': big_trend,
                    'decision': '因MACD顶背离减仓70%',
                    'target_nav': target_nav
                }

    # ADX 趋势转弱 (逻辑不变)
    if not np.isnan(adx) and adx >= adx_threshold and macd_zero_dead_cross:
        sell_reasons.append('ADX趋势转弱触发')
        return {
            'code': code,
            'profit_rate': round(profit_rate, 2),
            'rsi': round(rsi, 2),
            'macd_signal': macd_signal,
            'bb_pos': bb_pos,
            'big_trend': big_trend,
            'decision': '因ADX趋势转弱减仓50%',
            'target_nav': target_nav
        }

    # 布林带上轨突破计数 (逻辑不变)
    if len(full_fund_data) >= profit_lock_days:
        recent_data = full_fund_data.tail(profit_lock_days)
        break_count = recent_data['bb_break_upper'].sum()
        if break_count >= 2 and rsi > 75:
            sell_reasons.append('布林带上轨突破≥2次且RSI>75，减仓50%锁定利润')
            return {
                'code': code,
                'profit_rate': round(profit_rate, 2),
                'rsi': round(rsi, 2),
                'macd_signal': macd_signal,
                'bb_pos': bb_pos,
                'big_trend': big_trend,
                'decision': '因布林带上轨突破≥2次且RSI>75减仓50%',
                'target_nav': target_nav
            }

    # 最大回撤止损
    if len(full_fund_data) >= profit_lock_days:
        recent_data = full_fund_data.tail(profit_lock_days)
        if not recent_data.empty:
            peak_nav = recent_data['net_value'].max()
            current_nav = recent_data['net_value'].iloc[-1]
            drawdown = (peak_nav - current_nav) / peak_nav
            
            # 计算并记录短期回撤止损目标价
            target_nav['short_drawdown_nav'] = round(peak_nav * (1 - 0.10), 4)

            if drawdown > 0.10:
                sell_reasons.append(f'14天内最大回撤>10%（短期高点 {round(peak_nav, 4)}，触发净值: {target_nav["short_drawdown_nav"]}）触发，止损20%')
                return {
                    'code': code,
                    'profit_rate': round(profit_rate, 2),
                    'rsi': round(rsi, 2),
                    'macd_signal': macd_signal,
                    'bb_pos': bb_pos,
                    'big_trend': big_trend,
                    'decision': '因最大回撤止损20%',
                    'target_nav': target_nav
                }
    
    # 连续回吐日 (逻辑不变)
    if len(full_fund_data) >= decline_days_threshold:
        recent_returns = full_fund_data['daily_return'].tail(decline_days_threshold)
        if not recent_returns.empty and (recent_returns < 0).all():
            sell_reasons.append(f'连续{decline_days_threshold}天回吐')
            
    # 波动率卖出 (逻辑不变)
    if len(full_fund_data) >= volatility_window_adjusted and fund_latest['volatility'] is not np.nan and fund_latest['volatility'] > volatility_threshold:
        if rsi > 80 and macd_signal == '死叉' and latest_net_value < ma50:
            sell_reasons.append('波动率过高且指标过热')
            return {
                'code': code,
                'profit_rate': round(profit_rate, 2),
                'rsi': round(rsi, 2),
                'macd_signal': macd_signal,
                'bb_pos': bb_pos,
                'big_trend': big_trend,
                'decision': '因波动率过高卖出',
                'target_nav': target_nav
            }

    # 超规则（指标钝化）- 位于次级决策之前，暂停卖出信号 (逻辑不变)
    is_overbought_consecutive = False
    if len(full_fund_data) >= consecutive_days_threshold:
        recent_rsi = full_fund_data.tail(consecutive_days_threshold)['rsi']
        if (recent_rsi > rsi_overbought_threshold).all():
             is_overbought_consecutive = True
             
    big_market_recent = big_market_data.iloc[-2:]
    big_macd_dead_cross_today = False
    if len(big_market_recent) == 2:
        if big_market_latest['macd'] < big_market_latest['signal'] and \
           big_market_recent.iloc[0]['macd'] >= big_market_recent.iloc[0]['signal']:
            big_macd_dead_cross_today = True

    if is_overbought_consecutive:
        if (big_market_latest['macd'] > big_market_latest['signal']) and not big_macd_dead_cross_today:
             sell_reasons.append(f'持续强势，RSI>{rsi_overbought_threshold}，暂停卖出')
             return {
                'code': code,
                'profit_rate': round(profit_rate, 2),
                'rsi': round(rsi, 2),
                'macd_signal': macd_signal,
                'bb_pos': bb_pos,
                'big_trend': big_trend,
                'decision': '持续强势，暂停卖出',
                'target_nav': target_nav
            }

    # RSI和布林带锁定利润（保留原规则，作为次级保障）(逻辑不变)
    if len(full_fund_data) >= profit_lock_days:
        recent_data = full_fund_data.tail(profit_lock_days)
        recent_rsi = recent_data['rsi']
        bb_break = False
        if len(recent_data) >= 2:
            bb_break = (recent_data.tail(2)['net_value'] > recent_data.tail(2)['bb_upper']).all()

        if (recent_rsi > 75).any() and bb_break:
            sell_reasons.append('RSI>75且连续突破布林带上轨，减仓50%锁定利润')
            return {
                'code': code,
                'profit_rate': round(profit_rate, 2),
                'rsi': round(rsi, 2),
                'macd_signal': macd_signal,
                'bb_pos': bb_pos,
                'big_trend': big_trend,
                'decision': '减仓50%锁定利润',
                'target_nav': target_nav
            }
    
    # --- 三要素综合决策（最低优先级）---

    # 收益率要素
    if profit_rate > 50:
        sell_profit = '卖50%'
    elif profit_rate > 40:
        sell_profit = '卖30%'
    elif profit_rate > 30:
        sell_profit = '卖20%'
    elif profit_rate > 20:
        sell_profit = '卖10%'
    elif profit_rate < -10:
        sell_profit = '暂停定投'
    else:
        sell_profit = '持仓'

    # 指标要素
    indicator_sell = '持仓'
    if rsi > 85 or bb_pos == '上轨' :
        indicator_sell = '卖30%'
    elif rsi > 75 or macd_signal == '死叉':
        indicator_sell = '卖20%'

    # 大盘要素
    if big_trend == '弱势':
        market_sell = '卖10%'
    else:
        market_sell = '持仓'

    # 综合决策
    if '卖' in sell_profit and '卖' in indicator_sell and '卖' in market_sell:
        decision = '卖30%'
    elif '卖' in sell_profit and '卖' in indicator_sell:
        decision = '卖20%'
    elif '卖' in sell_profit and '卖' in market_sell:
        decision = '卖10%'
    elif '卖' in indicator_sell and '卖' in market_sell:
        decision = '卖10%'
    elif '暂停' in sell_profit:
        decision = '暂停定投'
    else:
        decision = '持仓'
        
    if sell_reasons:
        print(f"辅助信息: 触发了次级信号 {sell_reasons}")
    print(f"收益率: {holding['profit_rate']}%")
    print(f"RSI: {rsi}, MACD信号: {macd_signal}, 布林带位置: {bb_pos}, 50天均线: {ma50}, ADX: {adx}")

    return {
        'code': code,
        'profit_rate': round(profit_rate, 2),
        'rsi': round(rsi, 2),
        'macd_signal': macd_signal,
        'bb_pos': bb_pos,
        'big_trend': big_trend,
        'decision': decision,
        'target_nav': target_nav
    }

# 生成决策
decisions = []
for code, holding in holdings.items():
    # 传入优化后的 big_trend
    decisions.append(decide_sell(code, holding, fund_nav_data[code], big_market_latest, big_market_data, big_trend))

# 报告生成（时间设置为当前）
current_time = datetime.now()
md_content = f"""
# 基金卖出决策报告 (已优化 - 强化止盈止损)

## 报告总览
生成时间: {current_time.strftime('%Y-%m-%d %H:%M:%S')} (上海时间)
**注意:** 该报告基于**单位份额**的成本净值进行分析，不涉及总持仓金额。

## 大盘趋势
沪深300 RSI: {round(big_market_latest['rsi'], 2)} | MACD: {round(big_market_latest['macd'], 4)} | 趋势: **{big_trend}** (RSI和MA50综合判断)
**配置参数:** RSI天数: {rsi_window}，MA天数: {ma_window}，布林带天数: {bb_window}，**周期天数(利润锁定/回撤): {profit_lock_days}**，ADX阈值: {adx_threshold}。

## 卖出决策

| 基金代码 | 收益率 (%) | 成本净值 | 最新净值 | RSI | MACD信号 | 决策 |
|----------|------------|----------|----------|-----|----------|------|
"""

for d in decisions:
    latest_nav = holdings[d['code']]['latest_net_value']
    cost_nav = holdings[d['code']]['cost_nav']
    md_content += f"| {d['code']} | {d['profit_rate']} | {round(cost_nav, 4)} | {round(latest_nav, 4)} | {d['rsi']} | {d['macd_signal']} | **{d['decision']}** |\n"

md_content += f"""
## 策略建议 (优化及新增规则说明)
- **【优先级最高】绝对止损:** - 亏损超过 **20%** 触发**卖出100%**。
    - 亏损超过 **15%** 触发**减仓50%**。
- **【优先级高】移动止盈已启用:** - 净值从**历史最高点**回撤超过 **{int(trailing_stop_loss_pct * 100)}%** 则触发卖出。
- **【优先级高】最大回撤止损:** - **{profit_lock_days}天内**若净值从**短期最高点**回撤超过**10%**，触发**因最大回撤止损20%**。

## 止盈止损目标净值详情

| 基金代码 | 成本净值 | 历史峰值 | 移动止盈价 ({int(trailing_stop_loss_pct * 100)}%回撤) | 绝对止损价 (亏损 20%) | 短期回撤止损价 (10%回撤) |
|----------|----------|----------|----------------------------------------|-------------------------|--------------------------------|
"""

for d in decisions:
    cost_nav = holdings[d['code']]['cost_nav']
    current_peak = holdings[d['code']]['current_peak']
    target_nav = d['target_nav']
    
    # 格式化净值，如果为 np.nan 则显示 '-'
    trailing_stop_nav_str = f"{target_nav['trailing_stop_nav']:.4f}"
    abs_stop_20_nav_str = f"{target_nav['abs_stop_20_nav']:.4f}"
    short_drawdown_nav_str = f"{target_nav['short_drawdown_nav']:.4f}" if not np.isnan(target_nav['short_drawdown_nav']) else '-'
    
    md_content += f"| {d['code']} | {round(cost_nav, 4)} | {round(current_peak, 4)} | {trailing_stop_nav_str} | {abs_stop_20_nav_str} | {short_drawdown_nav_str} |\n"


md_content += f"""
## 策略细则（不变）
- **【强化】MACD 顶背离:** 净值创新高但MACD未能创新高时，触发**减仓70%**。
- **【强化】ADX 趋势转弱:** 若 ADX ≥ {adx_threshold} 且 MACD 在 0 轴附近或下方死叉，触发**减仓50%**。
- **【保留】布林带上轨突破计数:** 若 {profit_lock_days}天内净值突破布林带上轨≥2次且RSI>75，触发**减仓50%**。
- **【优化】大盘趋势:** 趋势判断（强势/中性/弱势）加入了**MA50均线**，使得大盘判断更准确。
- **超规则已启用:** 当大盘和基金都处于持续强势上涨时，会暂停卖出信号，避免过早离场。
"""

with open('sell_decision_optimized_full.md', 'w', encoding='utf-8') as f:
    f.write(md_content)

print("报告已生成: sell_decision_optimized_full.md")
