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

# --- 核心修正：将 SettingWithCopyWarning 导入路径从 core.common 更改为 errors ---
# 忽略 pandas 的 SettingWithCopyWarning
try:
    warnings.filterwarnings('ignore', category=pd.errors.SettingWithCopyWarning)
except AttributeError:
    # 兼容非常旧的 Pandas 版本
    warnings.filterwarnings('ignore', category=pd.core.common.SettingWithCopyWarning)

# --- 配置参数 ---
FUND_DATA_DIR = 'fund_data'
OUTPUT_FILE = 'fund_analysis_summary_optimized.csv' 
MAX_THREADS = 10
TRADING_DAYS_PER_YEAR = 250  # 每年平均交易日数量
RISK_FREE_RATE = 0.02  # 无风险利率 2%

# 【核心修正：取消 1 周周期】
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

# --- 辅助函数：网络请求 (保持不变) ---
def fetch_fund_info(fund_code):
    """从天天基金网获取基金的基本信息，使用 BeautifulSoup 增强解析鲁棒性，并加入反爬机制。"""
    if fund_code in FUND_INFO_CACHE:
        return FUND_INFO_CACHE[fund_code]

    url = f'http://fund.eastmoney.com/{fund_code}.html'
    headers = {'User-Agent': random.choice(USER_AGENTS)}
    time.sleep(1) 

    defaults = {
        'name': f'名称查找失败({fund_code})', 
        'size': 'N/A', 
        'type': 'N/A', 
        'daily_growth': 'N/A', 
        'net_value': 'N/A', 
        'rate': 'N/A'
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        content = response.text
        
        soup = BeautifulSoup(content, 'html.parser')

        # 1. 提取基金简称和代码
        title_tag = soup.find('div', class_='fundDetail-tit')
        if title_tag and title_tag.find('h4'):
            full_name = title_tag.find('h4').text.strip()
            defaults['name'] = re.sub(r'\(.*?\)$', '', full_name).strip()
        
        # 2. 提取资产规模、基金类型等信息
        size_element = soup.find('th', text=re.compile(r'资产规模'))
        if size_element:
            size_td = size_element.find_next_sibling('td')
            if size_td:
                defaults['size'] = size_td.text.strip()
        
        rate_element = soup.find('th', text=re.compile(r'管理费率'))
        if rate_element:
            rate_td = rate_element.find_next_sibling('td')
            if rate_td:
                defaults['rate'] = rate_td.text.strip()

        # 3. 提取最新净值和日涨跌幅
        data_div = soup.find('dl', class_='dataItem02')
        if data_div:
            net_value_tag = data_div.find('span', id='gz_nav')
            if net_value_tag:
                defaults['net_value'] = net_value_tag.text.strip()
            
            daily_growth_tag = data_div.find('span', id='gz_rate')
            if daily_growth_tag:
                defaults['daily_growth'] = daily_growth_tag.text.strip()

    except requests.exceptions.RequestException as e:
        print(f"❌ 基金 {fund_code} 网络请求失败: {e}")
    except Exception as e:
        print(f"❌ 基金 {fund_code} 数据解析失败: {e}")
    
    FUND_INFO_CACHE[fund_code] = defaults
    return defaults


# --- 核心计算函数 (仅修改滚动收益率部分) ---

def calculate_metrics(df, fund_code):
    """计算基金的各种风险收益指标，并进行数据清洗和优化。"""
    
    df.columns = df.columns.str.lower()
    df = df.rename(columns={'累计净值': 'cumulative_net_value', 'date': 'date'})
    df['cumulative_net_value'] = pd.to_numeric(df['cumulative_net_value'], errors='coerce')
    
    # 【数据清洗：极端异常值修正】
    mask_high_error = df['cumulative_net_value'] > 50 
    if mask_high_error.any():
        print(f"⚠️ 基金 {fund_code} 发现并修正了 {mask_high_error.sum()} 个极端净值异常点（>50）。")
        # 使用 .loc 进行赋值以避免 SettingWithCopyWarning
        df.loc[mask_high_error, 'cumulative_net_value'] = df.loc[mask_high_error, 'cumulative_net_value'] / 100 
    
    df = df.dropna(subset=['cumulative_net_value', 'date'])
    
    try:
        df['date'] = pd.to_datetime(df['date'])
    except:
        df['date'] = df['date'].apply(lambda x: pd.to_datetime(x, errors='coerce') if pd.notna(x) else np.nan)
        df = df.dropna(subset=['date'])

    df = df.sort_values(by='date').reset_index(drop=True)
    
    if len(df) < 2:
        return None, None
        
    cumulative_net_value = df['cumulative_net_value']

    # --- 1. 年化收益率 (基于交易日) ---
    total_return = (cumulative_net_value.iloc[-1] / cumulative_net_value.iloc[0]) - 1
    num_trading_days = len(cumulative_net_value) - 1
    
    if num_trading_days > 0:
        annual_return = (1 + total_return) ** (TRADING_DAYS_PER_YEAR / num_trading_days) - 1
    else:
        annual_return = np.nan

    # --- 2. 年化标准差和日收益率 ---
    returns = cumulative_net_value.pct_change().dropna()
    
    # 【新加异常检测和警告】
    mask_extreme_return = returns.abs() > 0.20 # 每日收益率超过 20%
    if mask_extreme_return.any():
        print(f"📢 基金 {fund_code} 警告：发现 {mask_extreme_return.sum()} 个极端日收益率（>20%），可能影响波动率计算。")
    
    annual_volatility = returns.std() * np.sqrt(TRADING_DAYS_PER_YEAR)
    
    # --- 3. 最大回撤 ---
    max_drawdown = (cumulative_net_value / cumulative_net_value.expanding().max() - 1).min()

    # --- 4. 夏普比率 ---
    if annual_volatility > 0:
        sharpe_ratio = (annual_return - RISK_FREE_RATE) / annual_volatility
    else:
        sharpe_ratio = np.nan
        
    # --- 5. 滚动年化收益率 (使用几何平均平滑异常值) ---
    rolling_metrics = {}
    
    for name, period_days in ROLLING_PERIODS.items():
        if len(cumulative_net_value) >= period_days:
            # 1. 计算所有非年化的期间收益率 (R_p)
            rolling_non_ann_returns = cumulative_net_value.pct_change(periods=period_days).dropna()
            
            # 2. 将收益率转换为 (1 + R_p)
            compounding_factors = 1 + rolling_non_ann_returns

            # 3. 计算所有周期收益率的几何平均
            log_returns = np.log(compounding_factors)
            mean_log_return = log_returns.mean()
            R_geo = np.exp(mean_log_return) - 1
            
            # 4. 将平均几何收益率年化
            annualized_R_geo = (1 + R_geo) ** (TRADING_DAYS_PER_YEAR / period_days) - 1
            
            rolling_metrics[f'平均滚动年化收益率({name})'] = annualized_R_geo
        else:
            rolling_metrics[f'平均滚动年化收益率({name})'] = np.nan

    metrics = {
        '基金代码': fund_code,
        '起始日期': df['date'].iloc[0].strftime('%Y-%m-%d'),
        '结束日期': df['date'].iloc[-1].strftime('%Y-%m-%d'),
        '年化收益率(总)': annual_return,
        '年化标准差(总)': annual_volatility,
        '最大回撤(MDD)': max_drawdown,
        '夏普比率(总)': sharpe_ratio,
        **rolling_metrics
    }
    
    return metrics, df['date'].iloc[0], df['date'].iloc[-1]

# --- 主执行函数 (保持不变) ---

def main():
    if not os.path.isdir(FUND_DATA_DIR):
        print(f"❌ 错误：未找到数据目录 '{FUND_DATA_DIR}'。请创建此目录并将CSV文件放入其中。")
        return

    csv_files = [f for f in os.listdir(FUND_DATA_DIR) if f.endswith('.csv')]
    if not csv_files:
        print(f"❌ 错误：'{FUND_DATA_DIR}' 目录中未找到任何CSV文件。")
        return

    fund_codes = [f.split('.')[0] for f in csv_files]
    all_metrics = []
    
    # 阶段 1: 计算指标并确定共同分析期
    print(f"--- 阶段 1/2: 计算 {len(fund_codes)} 支基金的风险收益指标 ---")
    start_dates = []
    end_dates = []
    
    for fund_code in fund_codes:
        file_path = os.path.join(FUND_DATA_DIR, f'{fund_code}.csv')
        try:
            # 尝试多种分隔符读取
            try:
                df = pd.read_csv(file_path, encoding='utf-8')
            except UnicodeDecodeError:
                df = pd.read_csv(file_path, encoding='gbk')
            except pd.errors.ParserError:
                 df = pd.read_csv(file_path, encoding='utf-8', sep='\t')
            
            metrics, start_date, end_date = calculate_metrics(df.copy(), fund_code)
            
            if metrics:
                all_metrics.append(metrics)
                start_dates.append(start_date)
                end_dates.append(end_date)
        
        except Exception as e:
            print(f"❌ 基金 {fund_code} 处理失败: {e}")
            
    if not all_metrics:
        print("所有基金数据处理均失败。")
        return

    # 确定共同分析期
    latest_start = max(start_dates) if start_dates else None
    earliest_end = min(end_dates) if end_dates else None

    # 阶段 2: 获取基金基本信息 (多线程)
    print(f"\n--- 阶段 2/2: 多线程获取 {len(fund_codes)} 支基金的基本信息 ---")
    fund_codes_to_fetch = [m['基金代码'] for m in all_metrics]
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        future_to_code = {executor.submit(fetch_fund_info, code): code for code in fund_codes_to_fetch}
        
        for future in concurrent.futures.as_completed(future_to_code):
            code = future_to_code[future]
            try:
                _ = future.result() 
            except Exception as e:
                print(f"❌ 基金 {code} 信息获取失败: {e}")

    # 阶段 3: 整合和输出
    print("\n--- 阶段 3/3: 整合数据并输出结果 ---")
    final_df = pd.DataFrame(all_metrics)

    info_list = [FUND_INFO_CACHE[code] for code in final_df['基金代码']]
    info_df = pd.DataFrame(info_list).rename(columns={'name': '基金简称', 'size': '资产规模', 'type': '基金类型', 'daily_growth': '最新日涨跌幅', 'net_value': '最新净值', 'rate': '管理费率'})
    
    final_df = pd.concat([info_df, final_df], axis=1)
    
    # 格式化百分比和数字
    for col in final_df.columns:
        if ('收益率' in col or '标准差' in col or '回撤' in col) and col != '夏普比率(总)':
            final_df[col] = final_df[col].apply(lambda x: f'{x * 100:.2f}%' if pd.notna(x) else 'N/A')
        elif '夏普比率(总)' in col:
            final_df[col] = final_df[col].apply(lambda x: f'{x:.3f}' if pd.notna(x) else 'N/A')
            final_df['夏普比率(总)_Num'] = final_df[col].replace({'N/A': np.nan}).astype(float)
            
    # 排序（按夏普比率降序）
    final_df = final_df.sort_values(by='夏普比率(总)_Num', ascending=False).drop(columns=['夏普比率(总)_Num']).reset_index(drop=True)
    
    # 输出共同分析期信息
    common_period = f'所有基金共同分析期：{latest_start.strftime("%Y-%m-%d")} 到 {earliest_end.strftime("%Y-%m-%d")}'
    print(common_period)
    
    # 添加共同分析期信息
    # 修正：直接创建一行描述，避免复杂的append操作
    period_info_row = pd.Series(
        {'基金代码': common_period},
        index=final_df.columns
    ).to_frame().T
    
    final_output = pd.concat([period_info_row, final_df], ignore_index=True)
    
    final_output.to_csv(OUTPUT_FILE, index=False, encoding='utf_8_sig')
    print(f"\n✅ 成功：分析结果已保存至 {os.path.abspath(OUTPUT_FILE)}")
    
if __name__ == '__main__':
    # 必须在主函数外调用，才能在程序启动时生效
    # warnings.filterwarnings('ignore', category=pd.errors.SettingWithCopyWarning) 
    main()
