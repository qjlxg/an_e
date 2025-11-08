import pandas as pd
import numpy as np
import os
import re
import concurrent.futures
import datetime
import requests
import random
import time
from bs4 import BeautifulSoup
import warnings

# 忽略 pandas 的 SettingWithCopyWarning
try:
    warnings.filterwarnings('ignore', category=pd.errors.SettingWithCopyWarning)
except AttributeError:
    warnings.filterwarnings('ignore', category=pd.core.common.SettingWithCopyWarning)

# --- 配置参数 ---
FUND_DATA_DIR = 'fund_data'
OUTPUT_FILE = 'fund_analysis_summary_common_period.csv' 
MAX_THREADS = 10
TRADING_DAYS_PER_YEAR = 250  # 每年平均交易日数量
RISK_FREE_RATE = 0.02  # 无风险利率 2%
EPSILON = 1e-10 # 用于几何平均计算，防止 log(<=0) 导致的 RuntimeWarning

ROLLING_PERIODS = {
    '1月': 20,
    '1季度': 60,
    '半年': 120,
    '1年': 250
}
FUND_INFO_CACHE = {}  # 缓存基金基本信息
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.3 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:99.0) Gecko/20100101 Firefox/99.0'
]

# --- 辅助函数：网络请求 ---
def fetch_fund_info(fund_code):
    """从天天基金网获取基金的基本信息。"""
    if fund_code in FUND_INFO_CACHE:
        return FUND_INFO_CACHE[fund_code]

    url = f'http://fundf10.eastmoney.com/jbgk_{fund_code}.html' 
    headers = {'User-Agent': random.choice(USER_AGENTS)}
    
    time.sleep(random.uniform(2, 4)) 

    defaults = {
        'name': f'名称查找失败({fund_code})', 
        'size': 'N/A', 
        'type': 'N/A', 
        'daily_growth': 'N/A', 
        'net_value': 'N/A', 
        'rate': 'N/A'
    }

    try:
        response = requests.get(url, headers=headers, timeout=20) 
        response.raise_for_status()
        content = response.text
        
        soup = BeautifulSoup(content, 'html.parser')
        
        # --- 1. 提取基金简称 ---
        title_tag = soup.select_one('.basic-new .bs_jz h4.title a')
        if title_tag and 'title' in title_tag.attrs:
            full_name = title_tag['title']
            defaults['name'] = re.sub(r'\(.*?\)$', '', full_name).strip() 

        # --- 2. 提取最新净值和日涨跌幅 ---
        net_value_tag = soup.select_one('.basic-new .bs_jz .col-right .row1 b')
        if net_value_tag:
            text = net_value_tag.text.strip()
            parts = re.split(r'\s*\((.*?)\)\s*', text, 1) 
            if len(parts) >= 3:
                defaults['net_value'] = parts[0].strip()
                defaults['daily_growth'] = f'({parts[1]})'
            else:
                 defaults['net_value'] = parts[0].strip()
                 
        # --- 3. 提取基金类型和资产规模 (从 .bs_gl 块) ---
        bs_gl = soup.select_one('.basic-new .bs_gl')
        if bs_gl:
            type_label = bs_gl.find('label', string=re.compile(r'类型：'))
            if type_label and type_label.find('span'):
                 defaults['type'] = type_label.find('span').text.strip()

            size_label = bs_gl.find('label', string=re.compile(r'资产规模：'))
            if size_label and size_label.find('span'):
                defaults['size'] = size_label.find('span').text.strip()

        # --- 4. 提取管理费率 (从 .info w790 表格) ---
        info_table = soup.select_one('table.info.w790')
        if info_table:
            rate_th = info_table.find('th', string=re.compile(r'管理费率'))
            if rate_th:
                rate_td = rate_th.find_next_sibling('td')
                if rate_td:
                    defaults['rate'] = rate_td.text.strip()
        
    except requests.exceptions.RequestException as e:
        print(f"❌ 基金 {fund_code} 网络请求失败: {e}")
    except Exception as e:
        print(f"❌ 基金 {fund_code} 数据解析失败: {e}")
    
    FUND_INFO_CACHE[fund_code] = defaults
    return defaults


def clean_and_prepare_df(df, fund_code):
    """数据清洗和预处理，返回清理后的DataFrame和其有效起止日期。"""
    df.columns = df.columns.str.lower()
    df = df.rename(columns={'累计净值': 'cumulative_net_value', 'date': 'date'})
    df['cumulative_net_value'] = pd.to_numeric(df['cumulative_net_value'], errors='coerce')
    
    # 极端异常值修正 (针对可能的输入错误，例如单位净值错输为累计净值)
    mask_high_error = df['cumulative_net_value'] > 50 
    if mask_high_error.any():
        print(f"⚠️ 基金 {fund_code} 发现并修正了 {mask_high_error.sum()} 个极端净值异常点（>50）。")
        df.loc[mask_high_error, 'cumulative_net_value'] = df.loc[mask_high_error, 'cumulative_net_value'] / 100 
    
    df = df.dropna(subset=['cumulative_net_value', 'date'])

    # 零或负净值清理
    mask_zero_or_negative = df['cumulative_net_value'] <= 0
    if mask_zero_or_negative.any():
        print(f"💣 基金 {fund_code} 发现 {mask_zero_or_negative.sum()} 个零或负净值，已移除。")
        df.loc[mask_zero_or_negative, 'cumulative_net_value'] = np.nan
        df = df.dropna(subset=['cumulative_net_value'])
    
    try:
        df.loc[:, 'date'] = pd.to_datetime(df['date'])
    except:
        df.loc[:, 'date'] = df['date'].apply(lambda x: pd.to_datetime(x, errors='coerce') if pd.notna(x) else np.nan)
        df = df.dropna(subset=['date'])

    df = df.sort_values(by='date').reset_index(drop=True)
    
    if len(df) < 2:
        return None, None, None
        
    start_date = df['date'].iloc[0]
    end_date = df['date'].iloc[-1]
    
    return df, start_date, end_date


def calculate_metrics(df, fund_code, period_prefix=''):
    """计算基金的各种风险收益指标。"""
    global EPSILON
    
    if df is None or len(df) < 2:
        # 如果数据点不足，返回一个包含NaN值的字典
        metrics = {
            '基金代码': fund_code,
            f'{period_prefix}起始日期': df['date'].iloc[0].strftime('%Y-%m-%d') if len(df) > 0 else 'N/A',
            f'{period_prefix}结束日期': df['date'].iloc[-1].strftime('%Y-%m-%d') if len(df) > 0 else 'N/A',
            f'{period_prefix}年化收益率': np.nan,
            f'{period_prefix}年化标准差': np.nan,
            f'{period_prefix}最大回撤(MDD)': np.nan,
            f'{period_prefix}夏普比率': np.nan,
        }
        for name in ROLLING_PERIODS:
             metrics[f'{period_prefix}平均滚动年化收益率({name})'] = np.nan
        return metrics
        
    cumulative_net_value = df['cumulative_net_value']
    
    # --- 1. 年化收益率 (基于交易日) ---
    if cumulative_net_value.iloc[0] <= 0:
        annual_return = np.nan
    else:
        total_return = (cumulative_net_value.iloc[-1] / cumulative_net_value.iloc[0]) - 1
        num_trading_days = len(cumulative_net_value) - 1
        
        if num_trading_days > 0:
            # 几何平均年化
            annual_return = (1 + total_return) ** (TRADING_DAYS_PER_YEAR / num_trading_days) - 1
        else:
            annual_return = np.nan

    # --- 2. 年化标准差和日收益率 ---
    returns = cumulative_net_value.pct_change().dropna()
    
    annual_volatility = returns.std() * np.sqrt(TRADING_DAYS_PER_YEAR)
    
    # --- 3. 最大回撤 ---
    max_drawdown = (cumulative_net_value / cumulative_net_value.expanding().max() - 1).min()

    # --- 4. 夏普比率 ---
    if annual_volatility > EPSILON: # 避免除以零
        sharpe_ratio = (annual_return - RISK_FREE_RATE) / annual_volatility
    else:
        sharpe_ratio = np.nan
        
    # --- 5. 滚动年化收益率 ---
    rolling_metrics = {}
    
    for name, period_days in ROLLING_PERIODS.items():
        if len(cumulative_net_value) < period_days:
            rolling_metrics[f'{period_prefix}平均滚动年化收益率({name})'] = np.nan
            continue

        rolling_non_ann_returns = cumulative_net_value.pct_change(periods=period_days).dropna()
        compounding_factors = 1 + rolling_non_ann_returns
        compounding_factors = np.maximum(compounding_factors, EPSILON) # 避免 log(<=0)

        log_returns = np.log(compounding_factors)
        mean_log_return = log_returns.mean()
        R_geo = np.exp(mean_log_return) - 1
        
        annualized_R_geo = (1 + R_geo) ** (TRADING_DAYS_PER_YEAR / period_days) - 1
        
        rolling_metrics[f'{period_prefix}平均滚动年化收益率({name})'] = annualized_R_geo

    metrics = {
        '基金代码': fund_code,
        f'{period_prefix}起始日期': df['date'].iloc[0].strftime('%Y-%m-%d'),
        f'{period_prefix}结束日期': df['date'].iloc[-1].strftime('%Y-%m-%d'),
        f'{period_prefix}年化收益率': annual_return,
        f'{period_prefix}年化标准差': annual_volatility,
        f'{period_prefix}最大回撤(MDD)': max_drawdown,
        f'{period_prefix}夏普比率': sharpe_ratio,
        **rolling_metrics
    }
    
    return metrics


def main():
    if not os.path.isdir(FUND_DATA_DIR):
        print(f"❌ 错误：未找到数据目录 '{FUND_DATA_DIR}'。请创建此目录并将CSV文件放入其中。")
        return

    csv_files = [f for f in os.listdir(FUND_DATA_DIR) if f.endswith('.csv')]
    if not csv_files:
        print(f"❌ 错误：'{FUND_DATA_DIR}' 目录中未找到任何CSV文件。")
        return

    fund_codes = [f.split('.')[0] for f in csv_files]
    all_funds_data = {} # 存储清洗后的DF
    valid_start_dates = []
    valid_end_dates = []
    
    # 阶段 1: 清洗数据，记录有效起止期
    print(f"--- 阶段 1/3: 清洗数据并确定共同分析期 ---")
    
    for fund_code in fund_codes:
        file_path = os.path.join(FUND_DATA_DIR, f'{fund_code}.csv')
        try:
            # 尝试不同的编码读取
            try:
                df_raw = pd.read_csv(file_path, encoding='utf-8')
            except UnicodeDecodeError:
                df_raw = pd.read_csv(file_path, encoding='gbk')
            except pd.errors.ParserError:
                 df_raw = pd.read_csv(file_path, encoding='utf-8', sep='\t')
            
            df_clean, start_date, end_date = clean_and_prepare_df(df_raw.copy(), fund_code)
            
            if df_clean is not None:
                all_funds_data[fund_code] = df_clean
                valid_start_dates.append(start_date)
                valid_end_dates.append(end_date)
            else:
                print(f"⚠️ 基金 {fund_code} 数据不足或无效，已跳过。")
        
        except Exception as e:
            print(f"❌ 基金 {fund_code} 数据文件读取失败: {e}")

    if not all_funds_data:
        print("所有基金数据处理均失败。")
        return

    # 确定共同分析期
    latest_start = max(valid_start_dates) if valid_start_dates else None
    earliest_end = min(valid_end_dates) if valid_end_dates else None

    # 检查共同期是否有效（至少需要 2 个数据点）
    min_days_required = 2 
    
    if latest_start is None or earliest_end is None or (earliest_end - latest_start).days < min_days_required:
        print("\n❌ 警告：无法确定有效的共同分析期。将转而计算所有基金的全历史指标。")
        
        # 如果共同期无效，计算全历史指标
        common_metrics_list = []
        for fund_code, df in all_funds_data.items():
            metrics = calculate_metrics(df, fund_code, period_prefix='全历史')
            if metrics:
                common_metrics_list.append(metrics)
        
        common_period_info = "所有基金的全历史指标 (共同分析期无效)"

    else:
        # 共同分析期有效，执行第二阶段计算
        common_period_info = f"所有基金共同分析期：{latest_start.strftime('%Y-%m-%d')} 到 {earliest_end.strftime('%Y-%m-%d')}"
        print(f"\n✅ 共同分析期确定为：{common_period_info}")
        
        # 阶段 2: 过滤数据并计算共同分析期指标
        print("\n--- 阶段 2/3: 计算共同分析期内的指标 ---")
        common_metrics_list = []
        
        for fund_code, df in all_funds_data.items():
            # 过滤数据到共同分析期内
            df_common = df[(df['date'] >= latest_start) & (df['date'] <= earliest_end)].copy()
            
            # 检查共同期内数据点是否足够
            if len(df_common) < min_days_required:
                 print(f"⚠️ 基金 {fund_code} 在共同期内数据点 ({len(df_common)}个) 不足，跳过共同期计算。")
                 metrics = {'基金代码': fund_code}
                 for col in ['起始日期', '结束日期', '年化收益率', '年化标准差', '最大回撤(MDD)', '夏普比率'] + [f'平均滚动年化收益率({p})' for p in ROLLING_PERIODS]:
                    metrics[f'共同期{col}'] = np.nan
                 common_metrics_list.append(metrics)
                 continue

            metrics = calculate_metrics(df_common, fund_code, period_prefix='共同期')
            if metrics:
                common_metrics_list.append(metrics)
        
    final_metrics_df = pd.DataFrame(common_metrics_list)
    
    # 阶段 3: 获取基金基本信息 (多线程)
    fund_codes_to_fetch = final_metrics_df['基金代码'].tolist()
    print(f"\n--- 阶段 3/3: 多线程获取 {len(fund_codes_to_fetch)} 支基金的基本信息 ---")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        future_to_code = {executor.submit(fetch_fund_info, code): code for code in fund_codes_to_fetch}
        _ = [future.result() for future in concurrent.futures.as_completed(future_to_code)]

    # 阶段 4: 整合和输出
    print("\n--- 阶段 4/4: 整合数据并输出结果 ---")

    # 从缓存中获取基金信息并整合
    info_list = [FUND_INFO_CACHE[code] for code in final_metrics_df['基金代码']]
    info_df = pd.DataFrame(info_list).rename(columns={
        'name': '基金简称', 'size': '资产规模', 'type': '基金类型', 
        'daily_growth': '最新日涨跌幅', 'net_value': '最新净值', 'rate': '管理费率'
    })
    
    # 重置索引并拼接
    info_df.index = final_metrics_df.index
    final_df = pd.concat([info_df, final_metrics_df], axis=1)
    
    # 格式化百分比和数字
    sharpe_col_candidates = [col for col in final_df.columns if '夏普比率' in col]
    sharpe_col = sharpe_col_candidates[0] if sharpe_col_candidates else None
    
    if sharpe_col:
        
        # 创建一个用于排序的临时数字列，基于共同期/全历史的夏普比率
        final_df[f'{sharpe_col}_Num'] = final_df[sharpe_col].replace({'N/A': np.nan}).astype(float)
        
        for col in final_df.columns:
            if ('收益率' in col or '标准差' in col or '回撤' in col) and col != sharpe_col:
                def format_pct(x):
                    if pd.isna(x) or np.isinf(x):
                        return 'N/A'
                    return f'{x * 100:.2f}%'
                final_df[col] = final_df[col].apply(format_pct)
            elif sharpe_col in col:
                def format_sharpe(x):
                    if pd.isna(x) or np.isinf(x):
                        return 'N/A'
                    return f'{x:.3f}'
                final_df[col] = final_df[col].apply(format_sharpe)
            
        # 排序（按共同期/全历史夏普比率降序）
        final_df = final_df.sort_values(by=f'{sharpe_col}_Num', ascending=False).drop(columns=[f'{sharpe_col}_Num']).reset_index(drop=True)
    
    # 创建共同分析期信息行
    period_info_row = pd.Series(
        {'基金简称': common_period_info, '基金代码': '分析期信息'},
        index=final_df.columns
    ).to_frame().T
    
    final_output = pd.concat([period_info_row, final_df], ignore_index=True)
    
    # 确保列顺序正确 (将 '基金代码' 和 '基金简称' 提前)
    prefix = '共同期' if '共同期年化收益率' in final_output.columns else '全历史'
    target_columns = [
        '基金代码', '基金简称', '资产规模', '基金类型', '最新日涨跌幅', '最新净值', 
        '管理费率', f'{prefix}起始日期', f'{prefix}结束日期', 
        f'{prefix}年化收益率', f'{prefix}年化标准差', f'{prefix}最大回撤(MDD)', 
        f'{prefix}夏普比率', f'{prefix}平均滚动年化收益率(1月)', 
        f'{prefix}平均滚动年化收益率(1季度)', f'{prefix}平均滚动年化收益率(半年)', 
        f'{prefix}平均滚动年化收益率(1年)'
    ]
    
    # 重新排列列，如果某个列缺失则忽略
    final_output = final_output[[col for col in target_columns if col in final_output.columns]]

    # 使用 utf_8_sig 编码以确保 Excel 中文不乱码
    final_output.to_csv(OUTPUT_FILE, index=False, encoding='utf_8_sig')
    print(f"\n✅ 成功：分析结果已保存至 {os.path.abspath(OUTPUT_FILE)}")
    
if __name__ == '__main__':
    main()
