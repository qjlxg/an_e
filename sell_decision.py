import pandas as pd
import numpy as np
import os
import json
from datetime import datetime, timedelta

# 加载配置文件
with open('holdings_config.json', 'r', encoding='utf-8') as f:
    holdings_config = json.load(f)

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
adx_threshold = params.get('adx_threshold', 30)  # 新增：ADX阈值

# 数据路径
big_market_path = 'index_data/000300.csv'
fund_data_dir = 'fund_data/'

# 加载大盘数据
if os.path.exists(big_market_path):
    big_market = pd.read_csv(big_market_path, parse_dates=['date'])
    big_market = big_market.sort_values('date').reset_index(drop=True)
    if big_market['net_value'].max() > 10 or big_market['net_value'].min() < 0:
        print("警告: 大盘数据净值异常，请检查 index_data/000300.csv")
else:
    raise FileNotFoundError("大盘数据文件 index_data/000300.csv 未找到")

# 计算ADX指标
def calculate_adx(df, window):
    df = df.copy()
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
    pdi = (pdm / atr) * 100
    mdi = (mdm / atr) * 100
    dx = (abs(pdi - mdi) / (pdi + mdi)) * 100
    df['adx'] = dx.ewm(span=window, adjust=False, min_periods=window).mean()
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
    df['rsi'] = 100 - (100 / (1 + rs.replace([np.inf, -np.inf], np.nan).fillna(0)))
    
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
    fund_file = os.path.join(fund_data_dir, f"{code}.csv")
    if os.path.exists(fund_file):
        fund_df = pd.read_csv(fund_file, parse_dates=['date'])
        if fund_df['net_value'].max() > 10 or fund_df['net_value'].min() < 0:
            print(f"警告: 基金 {code} 数据净值异常，请检查 {fund_file}")
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
    else:
        print(f"警告: 基金数据文件 {fund_file} 未找到，跳过 {code}")

# 决策函数（新增布林带突破计数）
def decide_sell(code, holding, full_fund_data, big_market_latest, big_market_data, big_trend):
    print(f"\n处理基金: {code}")
    profit_rate = holding['profit_rate']
    latest_net_value = holding['latest_net_value']
    cost_nav = holding['cost_nav']
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
    # --------------------------------------------------
    
    # 辅助信息
    sell_reasons = []

    # 分级止损（新增）
    if profit_rate < -20:
        sell_reasons.append('绝对止损（亏损>20%）触发，卖出100%')
        return {
            'code': code,
            'profit_rate': round(profit_rate, 2),
            'rsi': round(rsi, 2),
            'macd_signal': macd_signal,
            'bb_pos': bb_pos,
            'big_trend': big_trend,
            'decision': '因绝对止损（亏损>20%）卖出100%'
        }
    elif profit_rate < -15:
        sell_reasons.append('亏损>15%触发，减仓50%')
        return {
            'code': code,
            'profit_rate': round(profit_rate, 2),
            'rsi': round(rsi, 2),
            'macd_signal': macd_signal,
            'bb_pos': bb_pos,
            'big_trend': big_trend,
            'decision': '因亏损>15%减仓50%'
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
            'decision': '暂停定投'
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
    drawdown = (holding['current_peak'] - latest_net_value) / holding['current_peak']
    if drawdown > trailing_stop_loss_pct:
        sell_reasons.append('移动止盈触发')
        return {
            'code': code,
            'profit_rate': round(profit_rate, 2),
            'rsi': round(rsi, 2),
            'macd_signal': macd_signal,
            'bb_pos': bb_pos,
            'big_trend': big_trend,
            'decision': '因移动止盈卖出'
        }

    # MACD 顶背离
    if len(full_fund_data) >= macd_divergence_window:
        recent_data = full_fund_data.tail(macd_divergence_window)
        if not recent_data.empty:
            peak_nav_idx = recent_data['net_value'].idxmax()
            peak_macd_idx = recent_data['macd'].idxmax()
            # 判断净值是否创新高，且MACD未能创新高
            is_nav_peak = (full_fund_data['net_value'].iloc[-1] == holding['current_peak'])
            is_macd_divergence = is_nav_peak and (recent_data['macd'].iloc[-1] < recent_data['macd'].max())
            
            # 使用更严格的判断：价格创新高，但MACD的峰值在更早出现
            if is_macd_divergence:
                sell_reasons.append('MACD顶背离触发')
                return {
                    'code': code,
                    'profit_rate': round(profit_rate, 2),
                    'rsi': round(rsi, 2),
                    'macd_signal': macd_signal,
                    'bb_pos': bb_pos,
                    'big_trend': big_trend,
                    'decision': '因MACD顶背离减仓70%'
                }

    # ADX 趋势转弱
    if not np.isnan(adx) and adx >= adx_threshold and macd_zero_dead_cross:
        sell_reasons.append('ADX趋势转弱触发')
        return {
            'code': code,
            'profit_rate': round(profit_rate, 2),
            'rsi': round(rsi, 2),
            'macd_signal': macd_signal,
            'bb_pos': bb_pos,
            'big_trend': big_trend,
            'decision': '因ADX趋势转弱减仓50%'
        }

    # 布林带上轨突破计数
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
                'decision': '因布林带上轨突破≥2次且RSI>75减仓50%'
            }

    # 最大回撤止损（保留原规则，作为次级保障）
    if len(full_fund_data) >= profit_lock_days:
        recent_data = full_fund_data.tail(profit_lock_days)
        if not recent_data.empty:
            peak_nav = recent_data['net_value'].max()
            current_nav = recent_data['net_value'].iloc[-1]
            drawdown = (peak_nav - current_nav) / peak_nav
            if drawdown > 0.10:
                sell_reasons.append('14天内最大回撤>10%触发，止损20%')
                return {
                    'code': code,
                    'profit_rate': round(profit_rate, 2),
                    'rsi': round(rsi, 2),
                    'macd_signal': macd_signal,
                    'bb_pos': bb_pos,
                    'big_trend': big_trend,
                    'decision': '因最大回撤止损20%'
                }
    
    # 连续回吐日（保留原规则）
    if len(full_fund_data) >= decline_days_threshold:
        recent_returns = full_fund_data['daily_return'].tail(decline_days_threshold)
        if not recent_returns.empty and (recent_returns < 0).all():
            sell_reasons.append(f'连续{decline_days_threshold}天回吐')
            
    # 波动率卖出（保留原规则）
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
                'decision': '因波动率过高卖出'
            }

    # 超规则（指标钝化） - 位于次级决策之前，暂停卖出信号
    # 优化：判断是否触发超规则的条件
    is_overbought_consecutive = False
    if len(full_fund_data) >= consecutive_days_threshold:
        recent_rsi = full_fund_data.tail(consecutive_days_threshold)['rsi']
        if (recent_rsi > rsi_overbought_threshold).all():
             is_overbought_consecutive = True
             
    big_market_recent = big_market_data.iloc[-2:]
    big_macd_dead_cross_today = False
    if len(big_market_recent) == 2:
        # 大盘今天是否死叉
        if big_market_latest['macd'] < big_market_latest['signal'] and \
           big_market_recent.iloc[0]['macd'] >= big_market_recent.iloc[0]['signal']:
            big_macd_dead_cross_today = True

    if is_overbought_consecutive:
        # 只有在大盘趋势非常强势（MACD金叉，且今天未形成死叉）时，才触发暂停卖出
        if (big_market_latest['macd'] > big_market_latest['signal']) and not big_macd_dead_cross_today:
             sell_reasons.append(f'持续强势，RSI>{rsi_overbought_threshold}，暂停卖出')
             return {
                'code': code,
                'profit_rate': round(profit_rate, 2),
                'rsi': round(rsi, 2),
                'macd_signal': macd_signal,
                'bb_pos': bb_pos,
                'big_trend': big_trend,
                'decision': '持续强势，暂停卖出'
            }

    # RSI和布林带锁定利润（保留原规则，作为次级保障）
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
                'decision': '减仓50%锁定利润'
            }
    
    # --- 三要素综合决策（最低优先级） ---

    # 收益率要素
    if profit_rate > 50:
        sell_profit = '卖50%'
    elif profit_rate > 40:
        sell_profit = '卖30%'
    elif profit_rate > 30:
        sell_profit = '卖20%'
    elif profit_rate > 20:
        sell_profit = '卖10%'
    # 优化：-15% 止损已前置，这里保留 -10% 暂停定投
    elif profit_rate < -10:
        sell_profit = '暂停定投'
    else:
        sell_profit = '持仓'

    # 指标要素
    # 优化：高优先级 ADX 规则已独立，这里主要关注超买和死叉
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
        'decision': decision
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

| 基金代码 | 收益率 (%) | RSI | MACD信号 | 布林位置 | 大盘趋势 | 决策 |
|----------|------------|-----|----------|----------|----------|------|
"""

for d in decisions:
    md_content += f"| {d['code']} | {d['profit_rate']} | {d['rsi']} | {d['macd_signal']} | {d['bb_pos']} | {d['big_trend']} | **{d['decision']}** |\n"

md_content += f"""
## 策略建议 (优化及新增规则说明)
- **【优先级最高】绝对止损:** 亏损超过 **15%** 时，触发**减仓50%**，亏损超过 **20%** 触发**卖出100%**。
- **【优先级高】移动止盈已启用:** 净值从最高点回撤超过 **{int(trailing_stop_loss_pct * 100)}%** 则触发卖出。
- **【强化】MACD 顶背离:** 当净值创新高而MACD指标未能创新高时，视为重要顶部信号，触发**减仓70%**（原50%）。
- **【强化】ADX 趋势转弱:** 若 ADX ≥ {adx_threshold} 且 MACD 在 0 轴附近或下方死叉，视为趋势转弱，触发**减仓50%**。
- **【保留】布林带上轨突破计数:** 若 {profit_lock_days}天内净值突破布林带上轨≥2次且RSI>75，触发**减仓50%**。
- **【保留】最大回撤止损:** **{profit_lock_days}天内**若净值从最高点回撤超过**10%**，触发**因最大回撤止损20%**。
- **【优化】大盘趋势:** 趋势判断（强势/中性/弱势）加入了**MA50均线**，使得大盘判断更准确。
- **超规则已启用:** 当大盘和基金都处于持续强势上涨（MACD金叉，RSI钝化）时，会暂停卖出信号，避免过早离场。
- **决策逻辑:** 独立的高优先级规则（移动止盈、顶背离等）优先于原有的“收益率、指标和大盘”三要素综合判断。
"""

with open('sell_decision_optimized.md', 'w', encoding='utf-8') as f:
    f.write(md_content)

print("报告已生成: sell_decision_optimized.md")
