import pandas as pd
import numpy as np
import os
import yaml
from datetime import datetime
# import pandas_ta as ta # ⚠️ 注意：实际运行环境需要安装 pandas_ta，并取消本行注释

# --- 配置部分 ---
def load_config(config_path='holdings_config.yaml'):
    """加载配置文件并返回配置参数和持仓数据。"""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            holdings_config = yaml.safe_load(f)
    except FileNotFoundError:
        print(f"错误：{config_path} 文件未找到。")
        exit()
        
    params = holdings_config.get('parameters', {})
    holdings = {k: v for k, v in holdings_config.items() if k != 'parameters'}
    return params, holdings

# --- 计算指标函数 (保持不变) ---
def calculate_indicators(df, rsi_win, ma_win, bb_win, adx_win):
    """
    计算基金净值的RSI(14)、MACD、MA50、布林带位置和ADX。
    """
    df = df.copy()
    
    # 1. RSI (14)
    delta = df['net_value'].diff()
    up = delta.where(delta > 0, 0)
    down = -delta.where(delta < 0, 0)
    avg_up = up.ewm(com=rsi_win - 1, adjust=False, min_periods=rsi_win).mean()
    avg_down = down.ewm(com=rsi_win - 1, adjust=False, min_periods=rsi_win).mean()
    rs = avg_up / avg_down
    rs.replace([np.inf, -np.inf], np.nan, inplace=True)
    rs.fillna(0, inplace=True)
    df['rsi'] = 100 - (100 / (1 + rs))
    
    # 2. MA50 / MACD
    df['ma50'] = df['net_value'].rolling(window=ma_win, min_periods=1).mean()
    exp12 = df['net_value'].ewm(span=12, adjust=False).mean()
    exp26 = df['net_value'].ewm(span=26, adjust=False).mean()
    df['macd'] = 2 * (exp12 - exp26)
    df['signal'] = df['macd'].ewm(span=9, adjust=False).mean()
    
    # 3. Bollinger Bands
    df['bb_mid'] = df['net_value'].rolling(window=bb_win, min_periods=1).mean()
    df['bb_std'] = df['net_value'].rolling(window=bb_win, min_periods=1).std()
    df['bb_upper'] = df['bb_mid'] + (df['bb_std'] * 2)
    df['bb_lower'] = df['bb_mid'] - (df['bb_std'] * 2)
    
    # 4. Volatility / Daily Return
    df['daily_return'] = df['net_value'].pct_change()
    df['volatility'] = df['daily_return'].rolling(window=bb_win).std()
    
    # 5. ADX 占位符
    df['adx'] = df['daily_return'].rolling(window=adx_win).std() * 1000
        
    return df

# --- 大盘数据加载和指标计算 (保持不变) ---
def get_big_market_status(params, big_market_path='index_data/000300.csv'):
    """加载大盘数据并返回最新状态和趋势。"""
    try:
        if os.path.exists(big_market_path):
            big_market = pd.read_csv(big_market_path, parse_dates=['date'])
            big_market = big_market.sort_values('date').reset_index(drop=True)
        else:
            # 模拟数据
            dates = pd.date_range(end=datetime.now(), periods=100, freq='D')
            net_values = np.linspace(1.0, 1.2, 50)
            net_values = np.append(net_values, np.linspace(1.2, 0.9, 50))
            net_values = net_values + np.random.randn(100) * 0.005
            big_market = pd.DataFrame({'date': dates, 'net_value': net_values})
            big_market = big_market.sort_values('date').reset_index(drop=True)
            print("警告: 大盘数据文件未找到，使用熊市模拟数据。")
            
        rsi_window = params.get('rsi_window', 14)
        ma_window = params.get('ma_window', 50)
        bb_window = params.get('bb_window', 20)
        adx_window = params.get('adx_window', 14)
        
        big_market_data = calculate_indicators(big_market, rsi_window, ma_window, bb_window, adx_window)
        big_market_latest = big_market_data.iloc[-1]
        
        # 动态趋势判断 (简化)
        big_nav = big_market_latest['net_value']
        big_ma50 = big_market_latest['ma50']
        if big_market_latest['rsi'] > 50 and big_nav > big_ma50:
            big_trend = '强势'
        elif big_market_latest['rsi'] < 50 and big_nav < big_ma50:
            big_trend = '弱势'
        else:
            big_trend = '中性'
            
        return big_market_data, big_market_latest, big_trend
        
    except Exception as e:
        print(f"加载大盘数据时发生错误: {e}")
        return pd.DataFrame(), {}, '错误'
        
# --- 决策函数 (关键修改在此函数内) ---
def decide_sell(code, holding, full_fund_data, params, big_market_latest, big_market_data, big_trend):
    """
    根据最新数据和参数做出卖出决策。
    """
    # 获取参数 (优化: 移动止盈回撤比例改为 0.06)
    trailing_stop_loss_pct = params.get('trailing_stop_loss_pct', 0.06) 
    macd_divergence_window = params.get('macd_divergence_window', 60)
    adx_threshold = params.get('adx_threshold', 25) 
    profit_lock_days = params.get('profit_lock_days', 14)
    rsi_overbought_threshold = params.get('rsi_overbought_threshold', 80)
    consecutive_days_threshold = params.get('consecutive_days_threshold', 3)
    
    profit_rate = holding['profit_rate']
    latest_net_value = holding['latest_net_value']
    cost_nav = holding['cost_nav']
    current_peak = holding['current_peak']
    fund_latest = full_fund_data.iloc[-1]
    rsi = fund_latest['rsi']
    macd = fund_latest['macd']
    signal = fund_latest['signal']
    adx = fund_latest['adx']
    bb_pos = '未知'
    macd_signal = '未知'
    macd_zero_dead_cross = False
    
    # --- 关键：止盈/止损净值目标计算 ---
    target_nav = {
        'trailing_stop_nav': round(current_peak * (1 - trailing_stop_loss_pct), 4),
        'abs_stop_20_nav': round(cost_nav * (1 - 0.20), 4),
        'abs_stop_15_nav': round(cost_nav * (1 - 0.15), 4),
        'abs_stop_10_nav': round(cost_nav * (1 - 0.10), 4),
        'short_drawdown_nav': np.nan
    }
    # -----------------------------------
    
    sell_reasons = []
    decision = '持仓'
    
    # 指标计算
    if len(full_fund_data) >= 2:
        recent_data = full_fund_data.tail(2)
        if (recent_data['net_value'] > recent_data['bb_upper']).all(): bb_pos = '上轨'
        elif (recent_data['net_value'] < recent_data['bb_lower']).all(): bb_pos = '下轨'
        else: bb_pos = '中轨'
            
    macd_signal = '金叉'
    if len(full_fund_data) >= 2:
        recent_macd = full_fund_data.tail(2)
        if (recent_macd['macd'] < recent_macd['signal']).all():
            macd_signal = '死叉'
            if (recent_macd.iloc[-1]['macd'] < 0 and recent_macd.iloc[-1]['signal'] < 0): macd_zero_dead_cross = True

    # --- 高优先级规则（P1-P10 保持不变） ---
    # P1, P2, P3 绝对止损/暂停定投
    if profit_rate < -20:
        decision = '因绝对止损（亏损>20%）卖出100%'
        sell_reasons.append(f'绝对止损（亏损>20%，触发净值: {target_nav["abs_stop_20_nav"]}）触发')
        return { 'code': code, 'latest_nav': latest_net_value, 'cost_nav': cost_nav, 'profit_rate': round(profit_rate, 2), 'rsi': round(rsi, 2), 'macd_signal': macd_signal, 'bb_pos': bb_pos, 'big_trend': big_trend, 'decision': decision, 'target_nav': target_nav }
    elif profit_rate < -15:
        decision = '因亏损>15%减仓50%'
        sell_reasons.append(f'亏损>15%（触发净值: {target_nav["abs_stop_15_nav"]}）触发')
        return { 'code': code, 'latest_nav': latest_net_value, 'cost_nav': cost_nav, 'profit_rate': round(profit_rate, 2), 'rsi': round(rsi, 2), 'macd_signal': macd_signal, 'bb_pos': bb_pos, 'big_trend': big_trend, 'decision': decision, 'target_nav': target_nav }
    elif profit_rate < -10:
        decision = '暂停定投'
        sell_reasons.append(f'亏损>10%（触发净值: {target_nav["abs_stop_10_nav"]}）触发')
        return { 'code': code, 'latest_nav': latest_net_value, 'cost_nav': cost_nav, 'profit_rate': round(profit_rate, 2), 'rsi': round(rsi, 2), 'macd_signal': macd_signal, 'bb_pos': bb_pos, 'big_trend': big_trend, 'decision': decision, 'target_nav': target_nav }
    
    # P4 T1 止盈规则（布林带中轨）
    if profit_rate > 5 and bb_pos == '中轨': 
        decision = '【T1止盈】布林中轨已达，建议卖出 30% - 50% 仓位'
        sell_reasons.append('布林中轨已达（T1），锁定部分利润')
        return { 'code': code, 'latest_nav': latest_net_value, 'cost_nav': cost_nav, 'profit_rate': round(profit_rate, 2), 'rsi': round(rsi, 2), 'macd_signal': macd_signal, 'bb_pos': bb_pos, 'big_trend': big_trend, 'decision': decision, 'target_nav': target_nav }
    
    # P5 移动止盈 (新增成本保护)
    drawdown = (current_peak - latest_net_value) / current_peak
    trailing_stop_nav = target_nav['trailing_stop_nav']
    if trailing_stop_nav > cost_nav:
        if drawdown > trailing_stop_loss_pct:
            decision = '因移动止盈卖出'
            sell_reasons.append(f'移动止盈触发 (止盈价: {trailing_stop_nav})')
            return { 'code': code, 'latest_nav': latest_net_value, 'cost_nav': cost_nav, 'profit_rate': round(profit_rate, 2), 'rsi': round(rsi, 2), 'macd_signal': macd_signal, 'bb_pos': bb_pos, 'big_trend': big_trend, 'decision': decision, 'target_nav': target_nav }
            
    # P6 MACD 顶背离
    if len(full_fund_data) >= macd_divergence_window:
        recent_data = full_fund_data.tail(macd_divergence_window)
        if not recent_data.empty:
            is_nav_peak = (full_fund_data['net_value'].iloc[-1] == current_peak)
            is_macd_divergence = is_nav_peak and (recent_data['macd'].iloc[-1] < recent_data['macd'].max())
            if is_macd_divergence:
                decision = '因MACD顶背离减仓70%'
                sell_reasons.append('MACD顶背离触发')
                return { 'code': code, 'latest_nav': latest_net_value, 'cost_nav': cost_nav, 'profit_rate': round(profit_rate, 2), 'rsi': round(rsi, 2), 'macd_signal': macd_signal, 'bb_pos': bb_pos, 'big_trend': big_trend, 'decision': decision, 'target_nav': target_nav }
                
    # P7 ADX 趋势转弱
    if not np.isnan(adx) and adx >= adx_threshold and macd_zero_dead_cross:
        decision = '因ADX趋势转弱减仓50%'
        sell_reasons.append('ADX趋势转弱触发')
        return { 'code': code, 'latest_nav': latest_net_value, 'cost_nav': cost_nav, 'profit_rate': round(profit_rate, 2), 'rsi': round(rsi, 2), 'macd_signal': macd_signal, 'bb_pos': bb_pos, 'big_trend': big_trend, 'decision': decision, 'target_nav': target_nav }
        
    # P8 最大回撤止损
    if len(full_fund_data) >= profit_lock_days:
        recent_data = full_fund_data.tail(profit_lock_days)
        if not recent_data.empty:
            peak_nav = recent_data['net_value'].max()
            current_nav = recent_data['net_value'].iloc[-1]
            drawdown_short = (peak_nav - current_nav) / peak_nav
            
            target_nav['short_drawdown_nav'] = round(peak_nav * (1 - 0.10), 4)
            if drawdown_short > 0.10:
                decision = '因最大回撤止损20%'
                sell_reasons.append('14天内最大回撤>10%触发')
                return { 'code': code, 'latest_nav': latest_net_value, 'cost_nav': cost_nav, 'profit_rate': round(profit_rate, 2), 'rsi': round(rsi, 2), 'macd_signal': macd_signal, 'bb_pos': bb_pos, 'big_trend': big_trend, 'decision': decision, 'target_nav': target_nav }
    
    # P9 RSI和布林带锁定利润
    if len(full_fund_data) >= profit_lock_days:
        recent_data = full_fund_data.tail(profit_lock_days)
        recent_rsi = recent_data['rsi']
        bb_break = False
        if len(recent_data) >= 2:
            bb_break = (recent_data.tail(2)['net_value'] > recent_data.tail(2)['bb_upper']).all()
        if (recent_rsi > 75).any() and bb_break:
            decision = '减仓50%锁定利润'
            sell_reasons.append('RSI>75且连续突破布林带上轨，减仓50%锁定利润')
            return { 'code': code, 'latest_nav': latest_net_value, 'cost_nav': cost_nav, 'profit_rate': round(profit_rate, 2), 'rsi': round(rsi, 2), 'macd_signal': macd_signal, 'bb_pos': bb_pos, 'big_trend': big_trend, 'decision': decision, 'target_nav': target_nav }

    # P10 超规则（指标钝化）
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
            decision = '持续强势，暂停卖出'
            sell_reasons.append(f'持续强势，RSI>{rsi_overbought_threshold}，暂停卖出')
            return { 'code': code, 'latest_nav': latest_net_value, 'cost_nav': cost_nav, 'profit_rate': round(profit_rate, 2), 'rsi': round(rsi, 2), 'macd_signal': macd_signal, 'bb_pos': bb_pos, 'big_trend': big_trend, 'decision': decision, 'target_nav': target_nav }
            
    # --- P11 三要素综合决策（最低优先级 - 优化低频减仓） ---
    
    # 1. 盈利幅度卖出
    sell_profit = '持仓'
    if profit_rate > 50: sell_profit = '卖50%'
    elif profit_rate > 40: sell_profit = '卖30%'
    elif profit_rate > 30: sell_profit = '卖20%'
    elif profit_rate > 20: sell_profit = '卖10%' # 只有盈利超过20%才触发低优先级卖出
    elif profit_rate < -10: sell_profit = '暂停定投'
    
    # 2. 指标卖出（仅考虑极度超买，不考虑MACD死叉）
    indicator_sell = '持仓'
    if rsi > 85 or bb_pos == '上轨' : indicator_sell = '卖30%'
    elif rsi > 75: indicator_sell = '卖20%' # 极度超买，强制减仓
    
    # 3. 市场和MACD弱势时的操作 (转为暂停定投)
    is_weak_signal = (macd_signal == '死叉') and (big_trend == '弱势')
    
    # 决策优先级：高幅度盈利 > 极度超买 > 弱势暂停
    if '卖' in sell_profit and '卖' in indicator_sell: decision = '卖20%' # 盈利+超买 叠加
    elif '卖' in sell_profit: decision = sell_profit # 仅盈利幅度高
    elif '卖' in indicator_sell: decision = indicator_sell # 仅极度超买
    elif '暂停' in sell_profit: decision = '暂停定投' # 亏损>10%
    elif is_weak_signal: decision = '暂停定投' # <--- 新增: 弱势信号同时出现，则暂停定投代替减仓
    else: decision = '持仓'
        
    return {
        'code': code,
        'latest_nav': latest_net_value,
        'cost_nav': cost_nav,
        'profit_rate': round(profit_rate, 2),
        'rsi': round(rsi, 2),
        'macd_signal': macd_signal,
        'bb_pos': bb_pos,
        'big_trend': big_trend,
        'decision': decision,
        'target_nav': target_nav
    }

# --- 脚本运行的主逻辑 (保持不变) ---
if __name__ == '__main__':
    # 加载配置和参数
    params, holdings_config = load_config()
    
    # 获取大盘状态
    big_market_data, big_market_latest, big_trend = get_big_market_status(params)
    
    # 获取参数
    rsi_window = params.get('rsi_window', 14)
    ma_window = params.get('ma_window', 50)
    bb_window = params.get('bb_window', 20)
    adx_window = params.get('adx_window', 14)
    trailing_stop_loss_pct = params.get('trailing_stop_loss_pct', 0.06) 
    fund_data_dir = 'fund_data/'

    # 加载基金数据并生成 holdings 结构
    fund_nav_data = {}
    holdings = {}
    
    for code, cost_nav in holdings_config.items():
        fund_file = os.path.join(fund_data_dir, f"{code}.csv")
        if os.path.exists(fund_file):
            fund_df = pd.read_csv(fund_file, parse_dates=['date'])
        else:
            # 模拟数据
            dates = pd.date_range(end=datetime.now(), periods=100, freq='D')
            net_values = np.linspace(cost_nav * 1.05, cost_nav * 0.95, 100)
            net_values = net_values + np.random.randn(100) * 0.01
            fund_df = pd.DataFrame({'date': dates, 'net_value': net_values})
            fund_df = fund_df.sort_values('date').reset_index(drop=True)
            print(f"警告: 基金数据文件 {fund_file} 未找到，使用震荡下跌模拟数据。")

        if not fund_df.empty:
            full_fund_data = calculate_indicators(fund_df, rsi_window, ma_window, bb_window, adx_window)
            fund_nav_data[code] = full_fund_data
            latest_nav_data = full_fund_data.iloc[-1]
            latest_nav_value = float(latest_nav_data['net_value'])
            
            shares = 1 # 假设
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
            print(f"警告: 基金 {code} 数据为空，跳过。")
            
    # 生成决策
    decisions = []
    for code, holding in holdings.items():
        if code in fund_nav_data:
            decisions.append(decide_sell(code, holding, fund_nav_data[code], params, big_market_latest, big_market_data, big_trend))

    # --- 将结果转换为 CSV ---
    results_list = []
    for d in decisions:
        row = {
            '基金代码': d['code'],
            '最新净值': round(d['latest_nav'], 4),
            '成本净值': round(d['cost_nav'], 4),
            '收益率(%)': d['profit_rate'],
            'RSI': d['rsi'],
            'MACD信号': d['macd_signal'],
            '布林位置': d['bb_pos'],
            '大盘趋势': d['big_trend'],
            '**最终决策**': d['decision'],
            
            # 目标净值输出
            f'移动止盈价({int(trailing_stop_loss_pct * 100)}%回撤)': d['target_nav']['trailing_stop_nav'],
            '绝对止损价(-20%)': d['target_nav']['abs_stop_20_nav'],
            '绝对止损价(-15%)': d['target_nav']['abs_stop_15_nav'],
            '绝对止损价(-10%)': d['target_nav']['abs_stop_10_nav'],
            '短期回撤止损价(10%回撤)': d['target_nav']['short_drawdown_nav'] if not np.isnan(d['target_nav']['short_drawdown_nav']) else '-'
        }
        results_list.append(row)

    results_df = pd.DataFrame(results_list)
    output_filename = 'sell_decision_results.csv'
    results_df.to_csv(output_filename, index=False, encoding='utf_8_sig')
    print(f"决策结果已生成 CSV 文件: {output_filename}")
