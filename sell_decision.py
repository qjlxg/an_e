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
        # 为确保回测能运行，如果没有配置文件，返回默认空字典
        return {'trailing_stop_loss_pct': 0.08, 'lookback_days': 30}, {}
        
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
    df['rsi'] = 100 - (100 / (1 + rs))
    
    # 2. MA50
    df['ma50'] = df['net_value'].rolling(window=ma_win, min_periods=1).mean()
    
    # 3. MACD (使用EMA 12/26/9)
    exp12 = df['net_value'].ewm(span=12, adjust=False).mean()
    exp26 = df['net_value'].ewm(span=26, adjust=False).mean()
    df['macd'] = exp12 - exp26
    df['signal'] = df['macd'].ewm(span=9, adjust=False).mean()
    df['hist'] = df['macd'] - df['signal']
    
    # 4. 布林带 (BBands, 20)
    df['bb_mid'] = df['net_value'].rolling(window=bb_win).mean()
    std = df['net_value'].rolling(window=bb_win).std()
    df['bb_upper'] = df['bb_mid'] + (std * 2)
    df['bb_lower'] = df['bb_mid'] - (std * 2)
    
    # 5. ADX (简化版，仅用于示例)
    # ADX计算较为复杂，这里仅添加ADX列以兼容，实际回测时可能需要完整的ta库
    df['adx'] = 20.0 # 默认值，待补充
    
    return df

# --- 获取大盘状态函数 (保持不变) ---
def get_big_market_status(params):
    """加载大盘数据 (000300.csv) 并计算指标。"""
    big_market_code = params.get('big_market_index', '000300')
    file_path = f"fund_data/{big_market_code}.csv"
    
    try:
        big_market_df = pd.read_csv(file_path, parse_dates=['date']).sort_values('date').reset_index(drop=True)
        
        # 使用配置中的窗口参数计算大盘指标
        big_market_df = calculate_indicators(
            big_market_df, 
            params.get('rsi_window', 14), 
            params.get('ma_window', 50), 
            params.get('bb_window', 20), 
            params.get('adx_window', 14)
        )
        # 获取最新大盘数据
        big_market_latest = big_market_df.iloc[-1]
        
        # 判断大盘趋势 (简化逻辑)
        big_trend = '中性'
        if big_market_latest['rsi'] > 50 and big_market_latest['net_value'] > big_market_latest['ma50']:
            big_trend = '强势'
        elif big_market_latest['rsi'] < 50 and big_market_latest['net_value'] < big_market_latest['ma50']:
            big_trend = '弱势'
            
        return big_market_df, big_market_latest, big_trend
    
    except FileNotFoundError:
        print(f"警告: 大盘指数文件 {file_path} 未找到。")
        return pd.DataFrame({'date': [], 'net_value': []}), pd.Series(), '中性'


# --- 获取技术指标快照 (保持不变) ---
def get_fund_metrics(current_data_slice):
    """从数据快照中提取最新的技术指标和布林带位置。"""
    if current_data_slice.empty:
        return {'rsi': np.nan, 'macd_signal': '未知', 'bb_pos': '未知'}
    
    latest = current_data_slice.iloc[-1]
    
    # MACD 信号
    macd_signal = '未知'
    if latest['hist'] > 0 and current_data_slice.iloc[-2]['hist'] <= 0:
        macd_signal = '金叉'
    elif latest['hist'] < 0 and current_data_slice.iloc[-2]['hist'] >= 0:
        macd_signal = '死叉'
    
    # 布林带位置
    bb_pos = '中轨'
    if latest['net_value'] > latest['bb_upper']:
        bb_pos = '上轨'
    elif latest['net_value'] < latest['bb_lower']:
        bb_pos = '下轨'
        
    return {
        'rsi': round(latest['rsi'], 2) if not np.isnan(latest['rsi']) else np.nan,
        'macd_signal': macd_signal,
        'bb_pos': bb_pos
    }

# --- 卖出决策逻辑 (保持不变) ---
def decide_sell(fund_code, holding, current_data_slice, params, big_market_latest, big_market_data, big_trend):
    """根据持仓数据和指标快照，决定卖出策略。"""
    
    # 获取参数
    trailing_stop_loss_pct = params.get('trailing_stop_loss_pct', 0.08)
    abs_stop_loss_pcts = params.get('absolute_stop_loss_pcts', [-0.20, -0.15, -0.10])
    
    # 提取持仓信息
    cost_nav = holding['cost_nav']
    latest_nav = holding['latest_net_value']
    profit_rate = holding['profit_rate']
    current_peak = holding['current_peak']
    
    # 计算目标净值
    target_nav = {
        'trailing_stop_nav': round(current_peak * (1 - trailing_stop_loss_pct), 4),
        'abs_stop_20_nav': round(cost_nav * (1 - 0.20), 4),
        'abs_stop_15_nav': round(cost_nav * (1 - 0.15), 4),
        'abs_stop_10_nav': round(cost_nav * (1 - 0.10), 4),
    }

    # 获取技术指标
    metrics = get_fund_metrics(current_data_slice)
    
    # --- 决策逻辑 ---
    
    decision = "Hold"
    reason = "保持"
    
    # 1. 绝对止损 (亏损达到阈值时必须清仓)
    if profit_rate <= abs_stop_loss_pcts[0] * 100: # 假设最低止损线为 -20%
        decision = "卖出100%"
        reason = f"因绝对止损（亏损>{abs_stop_loss_pcts[0]*100}%）卖出100%"
        
    # 2. 移动止盈 (价格从峰值回撤达到阈值)
    elif latest_nav <= target_nav['trailing_stop_nav']:
        decision = "卖出100%"
        reason = f"因移动止盈（从净值 {current_peak:.4f} 回撤 {int(trailing_stop_loss_pct*100)}%）卖出100%"
        
    # 3. 交易信号止盈/止损 (例如 MACD 死叉)
    elif metrics['macd_signal'] == '死叉' and big_trend == '弱势':
        # 在大盘弱势配合死叉时清仓 (主动避险)
        decision = "卖出100%"
        reason = "MACD死叉配合大盘弱势，清仓避险"
        
    # 4. T1止盈 (布林中轨已达 - 假设用于短期波段交易者的减仓)
    elif metrics['bb_pos'] == '中轨' and profit_rate > 3.0: # 确保至少有一定盈利
        decision = "卖出50%"
        reason = "【T1止盈】布林中轨已达，建议卖出 50% 仓位"
        
    # 5. 技术指标警告
    elif metrics['rsi'] >= 80:
        decision = "卖出30%"
        reason = "RSI严重超买（>=80），建议减仓 30%"
        
    return {
        'code': fund_code,
        'decision': decision,
        'reason': reason,
        'latest_nav': latest_nav,
        'cost_nav': cost_nav,
        'profit_rate': round(profit_rate, 2),
        'rsi': metrics['rsi'],
        'macd_signal': metrics['macd_signal'],
        'bb_pos': metrics['bb_pos'],
        'big_trend': big_trend,
        'target_nav': target_nav
    }

# --- 辅助函数：计算买入指标 ---
def _calculate_buy_metrics(df, lookback_days):
    """
    计算买入决策所需的回撤和连续下跌天数。
    :param df: 基金净值数据 (必须按日期升序排列)
    :param lookback_days: 回溯的天数（例如 30 天）
    :return: 包含 'month_drawdown' 和 'consecutive_drop' 的字典
    """
    if len(df) < lookback_days:
        return {'month_drawdown': 0.0, 'consecutive_drop': 0}

    # 截取近 lookback_days 的数据 (iloc[-1] 是最新数据)
    recent_df = df.iloc[-lookback_days:]
    
    # 1. 计算短期回撤 (Month Drawdown)
    # 找到最近 lookback_days 内的最高点净值
    peak_nav = recent_df['net_value'].max()
    latest_nav = recent_df['net_value'].iloc[-1]
    
    # 回撤率 = (最高点 - 当前净值) / 最高点
    month_drawdown = (peak_nav - latest_nav) / peak_nav
    
    # 2. 计算连续下跌天数 (Consecutive Drop Days)
    # 倒序检查每日净值变化
    df_reversed = df['net_value'].iloc[::-1]
    
    consecutive_drop = 0
    # 检查当前净值是否低于前一日
    for i in range(1, len(df_reversed)):
        if df_reversed.iloc[i-1] < df_reversed.iloc[i]:
            consecutive_drop += 1
        else:
            break # 遇到上涨或持平则中断
            
    return {
        'month_drawdown': month_drawdown,
        'consecutive_drop': consecutive_drop
    }

# --- 【新增】买入决策逻辑 ---
def decide_buy(fund_code, holding, current_data_slice, params, big_market_latest, big_market_data, big_trend):
    """
    根据筛选逻辑（大跌/回撤）决定买入策略。
    此函数仅在 shares == 0 (空仓) 且 cash > 0 (有现金) 时被 backtest_module.py 调用。
    """
    # 获取买入参数
    # 默认值参考 analyzer.py 的逻辑
    MIN_MONTH_DRAWDOWN = params.get('buy_min_month_drawdown', 0.06) # 6% 短期回撤
    MIN_CONSECUTIVE_DROP_DAYS = params.get('buy_min_consecutive_drop', 3) # 3天连续下跌
    LOOKBACK_DAYS = params.get('buy_lookback_days', 30) # 回溯天数
    
    # 1. 计算买入指标
    buy_metrics = _calculate_buy_metrics(current_data_slice, LOOKBACK_DAYS)
    month_drawdown = buy_metrics['month_drawdown']
    consecutive_drop = buy_metrics['consecutive_drop']
    
    # 2. 获取技术指标快照（用于辅助判断，例如 RSI）
    metrics = get_fund_metrics(current_data_slice)

    # 3. 决策逻辑
    
    decision = "Hold"
    reason = "空仓观望"
    
    # 策略 1: 大盘趋势过滤 (避免在整体市场极度弱势时抄底)
    # 仅在中性或强势市场，或者大盘虽弱但指标极佳时考虑买入
    if big_trend == '弱势':
        # 即使大盘弱势，如果回撤或连续下跌极度符合条件，仍然可以买入（抄底）
        if month_drawdown >= MIN_MONTH_DRAWDOWN * 1.5 or consecutive_drop >= MIN_CONSECUTIVE_DROP_DAYS + 2:
            pass # 允许继续执行下面的买入判断
        else:
            return {'decision': decision, 'reason': f"大盘趋势为【{big_trend}】，不执行买入"}

    # 策略 2: 核心买入信号 (达到回撤或连续下跌阈值)
    is_big_drawdown = month_drawdown >= MIN_MONTH_DRAWDOWN
    is_consecutive_drop = consecutive_drop >= MIN_CONSECUTIVE_DROP_DAYS
    is_oversold = metrics['rsi'] is not np.nan and metrics['rsi'] <= 35 # RSI超卖
    
    if (is_big_drawdown or is_consecutive_drop) and is_oversold:
        # 满足回撤/连跌条件 AND RSI超卖 (双重确认，增强买入信号)
        decision = "买入100%"
        reason = f"【抄底买入】短期回撤{round(month_drawdown*100, 2)}%满足 ({MIN_MONTH_DRAWDOWN*100}%) 或连跌{consecutive_drop}天满足 ({MIN_CONSECUTIVE_DROP_DAYS}天)，且RSI超卖 ({metrics['rsi']})"
    elif is_big_drawdown or is_consecutive_drop:
        # 仅满足回撤/连跌条件
        decision = "买入100%"
        reason = f"【抄底买入】短期回撤{round(month_drawdown*100, 2)}%满足 ({MIN_MONTH_DRAWDOWN*100}%) 或连跌{consecutive_drop}天满足 ({MIN_CONSECUTIVE_DROP_DAYS}天)"
        
    return {
        'decision': decision,
        'reason': reason,
        'latest_nav': holding['latest_net_value'],
        'cost_nav': 0.0, # 此时为空仓
        'profit_rate': 0.0, 
        'rsi': metrics['rsi'],
        'macd_signal': metrics['macd_signal'],
        'bb_pos': metrics['bb_pos'],
        'big_trend': big_trend,
        'month_drawdown': round(month_drawdown * 100, 2),
        'consecutive_drop': consecutive_drop,
    }

# --- 主函数 (保持不变，但增加买入指标的输出) ---
def main():
    """读取数据，运行卖出/买入决策，并输出结果。"""
    
    # 1. 加载配置和参数
    params, holdings_config = load_config()
    
    # 2. 预加载大盘数据
    big_market_data, big_market_latest, big_trend = get_big_market_status(params)
    
    # 3. 加载基金数据并计算指标
    fund_nav_data = {}
    holdings = {} # 包含基金代码、净值、成本、份额等信息
    
    # 默认值
    trailing_stop_loss_pct = params.get('trailing_stop_loss_pct', 0.08)
    
    fund_data_dir = 'fund_data/'
    for code, cost_nav_str in holdings_config.items():
        fund_file = os.path.join(fund_data_dir, f"{code}.csv")
        
        try:
            df = pd.read_csv(fund_file, parse_dates=['date']).sort_values('date').reset_index(drop=True)
            # 计算指标
            df = calculate_indicators(
                df, 
                params.get('rsi_window', 14), 
                params.get('ma_window', 50), 
                params.get('bb_window', 20), 
                params.get('adx_window', 14)
            )
            fund_nav_data[code] = df
            
            # 模拟持仓状态 (仅用于当日卖出决策的静态模拟，不用于回测)
            latest_nav = df.iloc[-1]['net_value']
            cost_nav = float(cost_nav_str)
            current_peak = df['net_value'].max() # 假设历史最高点
            
            profit_rate = (latest_nav / cost_nav - 1) * 100
            
            holdings[code] = {
                'cost_nav': cost_nav,
                'latest_net_value': latest_nav,
                'profit_rate': profit_rate,
                'current_peak': current_peak
            }
        except Exception as e:
            print(f"处理文件 {fund_file} 时发生错误: {e}")
            continue
            
    # 生成决策
    decisions = []
    for code, holding in holdings.items():
        if code in fund_nav_data:
            # 理论上这里只运行 decide_sell，因为 holdings_config 是指当前持仓
            # 如果要运行 decide_buy，需要有一个“待买入池”
            
            # 这里保持只运行 decide_sell，因为这是卖出决策模块的主要功能
            decision_result = decide_sell(code, holding, fund_nav_data[code], params, big_market_latest, big_market_data, big_trend)
            
            # ⚠️ 另外，如果需要生成一份“今日可买入基金”的报告，需要单独的逻辑
            # 这里为兼容性，先让 decide_sell的结果通过
            decisions.append(decision_result)

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
        }
        results_list.append(row)

    if results_list:
        results_df = pd.DataFrame(results_list)
        
        # 确保目录存在
        output_dir = 'sell_reports'
        os.makedirs(output_dir, exist_ok=True)
        
        # 生成带时间戳的文件名
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = os.path.join(output_dir, f'sell_decision_results_{timestamp}.csv')
        
        results_df.to_csv(output_file, index=False, encoding='utf_8_sig')
        print(f"✅ 卖出决策报告已生成: {output_file}")
    else:
        print("⚠️ 未找到有效基金数据或配置，未生成报告。")

if __name__ == '__main__':
    main()
