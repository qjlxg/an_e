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
warnings.filterwarnings('ignore', category=pd.core.common.SettingWithCopyWarning)

# --- 配置参数 ---
FUND_DATA_DIR = 'fund_data'
OUTPUT_FILE = 'fund_analysis_summary_with_info_improved.csv'
MAX_THREADS = 10
TRADING_DAYS_PER_YEAR = 250  # 每年平均交易日数量
RISK_FREE_RATE = 0.02  # 无风险利率 2%
ROLLING_PERIODS = {
    '1周': 5,
    '1月': 20,
    '1季度': 60,
    '半年': 120,
    '1年': 250
}
FUND_INFO_CACHE = {}  # 缓存基金基本信息，避免重复请求
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.3 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:99.0) Gecko/20100101 Firefox/99.0'
]

# --- 辅助函数：网络请求 ---

def fetch_fund_info(fund_code):
    """从天天基金网获取基金的基本信息，使用 BeautifulSoup 增强解析鲁棒性，并加入反爬机制。"""
    if fund_code in FUND_INFO_CACHE:
        return FUND_INFO_CACHE[fund_code]

    url = f'http://fund.eastmoney.com/{fund_code}.html'
    headers = {'User-Agent': random.choice(USER_AGENTS)}

    # 增加请求延时，降低被封禁的风险
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
            # 提取文本，并清除基金代码部分
            full_name = title_tag.find('h4').text.strip()
            defaults['name'] = re.sub(r'\(.*?\)$', '', full_name).strip()
        
        # 2. 提取资产规模、基金类型等信息（更稳健的表格解析）
        fund_info_div = soup.find('div', class_='infoOfFund')
        if fund_info_div:
            # 提取基金类型
            type_match = re.search(r'基金类型：[^<]+<a[^>]+>([\u4e00-\u9fa5]+)</a>', content)
            if type_match:
                defaults['type'] = type_match.group(1).strip()
                
            # 提取资产规模
            size_element = soup.find('th', text=re.compile(r'资产规模'))
            if size_element:
                size_td = size_element.find_next_sibling('td')
                if size_td:
                    defaults['size'] = size_td.text.strip()
        
        # 3. 提取费率
        rate_element = soup.find('th', text=re.compile(r'管理费率'))
        if rate_element:
            rate_td = rate_element.find_next_sibling('td')
            if rate_td:
                defaults['rate'] = rate_td.text.strip()

        # 4. 提取最新净值和日涨跌幅
        data_div = soup.find('dl', class_='dataItem02')
        if data_div:
            # 最新净值
            net_value_tag = data_div.find('span', id='gz_nav')
            if net_value_tag:
                defaults['net_value'] = net_value_tag.text.strip()
            
            # 日涨跌幅
            daily_growth_tag = data_div.find('span', id='gz_rate')
            if daily_growth_tag:
                defaults['daily_growth'] = daily_growth_tag.text.strip()

    except requests.exceptions.RequestException as e:
        print(f"❌ 基金 {fund_code} 网络请求失败: {e}")
    except Exception as e:
        print(f"❌ 基金 {fund_code} 数据解析失败: {e}")
    
    FUND_INFO_CACHE[fund_code] = defaults
    return defaults

# --- 核心计算函数 ---

def calculate_metrics(df, fund_code):
    """计算基金的各种风险收益指标，并进行数据清洗。"""
    
    # 统一列名为小写
    df.columns = df.columns.str.lower()
    
    # 日期和累计净值预处理
    df = df.rename(columns={'累计净值': 'cumulative_net_value', 'date': 'date'})
    
    # 转换为数值类型，无法转换的设为NaN
    df['cumulative_net_value'] = pd.to_numeric(df['cumulative_net_value'], errors='coerce')
    
    # 🌟 关键修正 1: 异常值修正 (解决天文数字收益率)
    # 将累计净值大于 50 的异常值视为小数点错位，并除以 100 修正
    mask_high_error = df['cumulative_net_value'] > 50 
    if mask_high_error.any():
        print(f"⚠️ 基金 {fund_code} 发现并修正了 {mask_high_error.sum()} 个极端净值异常点。")
        # 假设是小数点移动两位，进行修正
        df.loc[mask_high_error, 'cumulative_net_value'] = df.loc[mask_high_error, 'cumulative_net_value'] / 100 
    
    # 清除 NaN 值
    df = df.dropna(subset=['cumulative_net_value', 'date'])
    
    # 确保日期格式正确
    try:
        df['date'] = pd.to_datetime(df['date'])
    except:
        # 如果日期格式混乱，尝试更通用的解析
        df['date'] = df['date'].apply(lambda x: pd.to_datetime(x, errors='coerce') if pd.notna(x) else np.nan)
        df = df.dropna(subset=['date'])

    # 按日期排序
    df = df.sort_values(by='date').reset_index(drop=True)
    
    if len(df) < 2:
        return None, None
        
    cumulative_net_value = df['cumulative_net_value']

    # --- 1. 年化收益率 (修正：使用实际交易日数量) ---
    total_return = (cumulative_net_value.iloc[-1] / cumulative_net_value.iloc[0]) - 1
    num_trading_days = len(cumulative_net_value) - 1
    
    if num_trading_days > 0:
        annual_return = (1 + total_return) ** (TRADING_DAYS_PER_YEAR / num_trading_days) - 1
    else:
        annual_return = np.nan

    # --- 2. 年化标准差和日收益率 ---
    returns = cumulative_net_value.pct_change().dropna()
    annual_volatility = returns.std() * np.sqrt(TRADING_DAYS_PER_YEAR)
    
    # --- 3. 最大回撤 ---
    max_drawdown = (cumulative_net_value / cumulative_net_value.expanding().max() - 1).min()

    # --- 4. 夏普比率 ---
    if annual_volatility > 0:
        sharpe_ratio = (annual_return - RISK_FREE_RATE) / annual_volatility
    else:
        sharpe_ratio = np.nan
        
    # --- 5. 滚动年化收益率 ---
    rolling_metrics = {}
    for name, period_days in ROLLING_PERIODS.items():
        if len(returns) >= period_days:
            # 计算滚动收益率，并年化 (period_days 为交易日)
            rolling_ann_returns = (cumulative_net_value.pct_change(periods=period_days) + 1).pow(TRADING_DAYS_PER_YEAR / period_days) - 1
            # 取平均值
            rolling_metrics[f'平均滚动年化收益率({name})'] = rolling_ann_returns.mean()
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

# --- 主执行函数 ---

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
        # 提交所有网络请求任务
        future_to_code = {executor.submit(fetch_fund_info, code): code for code in fund_codes_to_fetch}
        
        # 收集结果
        for future in concurrent.futures.as_completed(future_to_code):
            code = future_to_code[future]
            try:
                # 结果已存入全局缓存 FUND_INFO_CACHE
                _ = future.result() 
            except Exception as e:
                print(f"❌ 基金 {code} 信息获取失败: {e}")

    # 阶段 3: 整合和输出
    print("\n--- 阶段 3/3: 整合数据并输出结果 ---")
    final_df = pd.DataFrame(all_metrics)

    # 合并基本信息
    info_list = [FUND_INFO_CACHE[code] for code in final_df['基金代码']]
    info_df = pd.DataFrame(info_list).rename(columns={'name': '基金简称', 'size': '资产规模', 'type': '基金类型', 'daily_growth': '最新日涨跌幅', 'net_value': '最新净值', 'rate': '管理费率'})
    
    # 插入信息列到 DataFrame 头部
    final_df = pd.concat([info_df, final_df], axis=1)
    
    # 格式化百分比和数字
    for col in final_df.columns:
        if ('收益率' in col or '标准差' in col or '回撤' in col) and col != '夏普比率(总)':
            # 转换为百分比字符串
            final_df[col] = final_df[col].apply(lambda x: f'{x * 100:.2f}%' if pd.notna(x) else 'N/A')
        elif '夏普比率(总)' in col:
            final_df[col] = final_df[col].apply(lambda x: f'{x:.3f}' if pd.notna(x) else 'N/A')
            # 添加临时数字列用于排序
            final_df['夏普比率(总)_Num'] = final_df[col].replace({'N/A': np.nan}).astype(float)
            
    # 排序（按夏普比率降序）
    final_df = final_df.sort_values(by='夏普比率(总)_Num', ascending=False).drop(columns=['夏普比率(总)_Num']).reset_index(drop=True)
    
    # 输出共同分析期信息
    common_period = f'所有基金共同分析期：{latest_start.strftime("%Y-%m-%d")} 到 {earliest_end.strftime("%Y-%m-%d")}'
    print(common_period)
    
    # 将共同分析期信息添加到输出文件的第一行
    header = pd.DataFrame([{'基金代码': common_period}]).append(final_df.columns.to_series().T, ignore_index=True)
    header.columns = final_df.columns
    final_output = pd.concat([header.iloc[0:1], final_df], ignore_index=True)
    
    final_output.to_csv(OUTPUT_FILE, index=False, encoding='utf_8_sig')
    print(f"\n✅ 成功：分析结果已保存至 {os.path.abspath(OUTPUT_FILE)}")
    
if __name__ == '__main__':
    main()
