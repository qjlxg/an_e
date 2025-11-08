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
OUTPUT_FILE = 'fund_analysis_summary_optimized.csv' 
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

# --- 辅助函数：网络请求 (增强鲁棒性 & 精确解析) ---
def fetch_fund_info(fund_code):
    """从天天基金网获取基金的基本信息，增强反爬机制和解析精度。"""
    if fund_code in FUND_INFO_CACHE:
        return FUND_INFO_CACHE[fund_code]

    # 直接请求 F10 基本概况页面，数据更集中
    url = f'http://fundf10.eastmoney.com/jbgk_{fund_code}.html' 
    headers = {'User-Agent': random.choice(USER_AGENTS)}
    
    # 【优化 1：增加随机等待时间，提高成功率】
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
        # 【优化 2：增加超时时间，应对网络延迟】
        response = requests.get(url, headers=headers, timeout=20) 
        response.raise_for_status()
        content = response.text
        
        # 【优化 3：使用更精确的 BeautifulSoup 解析】
        soup = BeautifulSoup(content, 'html.parser')
        
        # --- 1. 提取基金简称 ---
        # 查找 .bs_jz 下的 h4.title a 标签，并从 title 属性中提取
        title_tag = soup.select_one('.basic-new .bs_jz h4.title a')
        if title_tag and 'title' in title_tag.attrs:
            full_name = title_tag['title']
            # 剥离代码，保留简称
            defaults['name'] = re.sub(r'\(.*?\)$', '', full_name).strip() 

        # --- 2. 提取最新净值和日涨跌幅 ---
        # 查找 .bs_jz .col-right .row1 b，这个标签包含 '净值 (涨跌幅)'
        net_value_tag = soup.select_one('.basic-new .bs_jz .col-right .row1 b')
        if net_value_tag:
            text = net_value_tag.text.strip()
            # 使用正则表达式分割净值和涨跌幅
            parts = re.split(r'\s*\((.*?)\)\s*', text, 1) 
            if len(parts) >= 3:
                defaults['net_value'] = parts[0].strip()
                defaults['daily_growth'] = f'({parts[1]})'
            else:
                 defaults['net_value'] = parts[0].strip()
                 
        # --- 3. 提取基金类型和资产规模 (从 .bs_gl 块) ---
        bs_gl = soup.select_one('.basic-new .bs_gl')
        if bs_gl:
            # 提取类型
            type_label = bs_gl.find('label', string=re.compile(r'类型：'))
            if type_label and type_label.find('span'):
                 defaults['type'] = type_label.find('span').text.strip()

            # 提取资产规模
            size_label = bs_gl.find('label', string=re.compile(r'资产规模：'))
            if size_label and size_label.find('span'):
                defaults['size'] = size_label.find('span').text.strip()


        # --- 4. 提取管理费率 (从 .info w790 表格) ---
        info_table = soup.select_one('table.info.w790')
        if info_table:
            # 查找<th>包含'管理费率'的行，并获取其下一个<td>
            rate_th = info_table.find('th', string=re.compile(r'管理费率'))
            if rate_th:
                # 定位到管理费率所在的 td 标签
                rate_td = rate_th.find_next_sibling('td')
                if rate_td:
                    defaults['rate'] = rate_td.text.strip()
        
    except requests.exceptions.RequestException as e:
        print(f"❌ 基金 {fund_code} 网络请求失败: {e}")
    except Exception as e:
        print(f"❌ 基金 {fund_code} 数据解析失败: {e}")
    
    FUND_INFO_CACHE[fund_code] = defaults
    return defaults


# --- 核心计算函数 (增强数据清洗) ---

def calculate_metrics(df, fund_code):
    """计算基金的各种风险收益指标，并进行数据清洗和优化。"""
    global EPSILON
    
    df.columns = df.columns.str.lower()
    df = df.rename(columns={'累计净值': 'cumulative_net_value', 'date': 'date'})
    df['cumulative_net_value'] = pd.to_numeric(df['cumulative_net_value'], errors='coerce')
    
    # 【数据清洗 1：极端异常值修正 (针对可能的输入错误)】
    mask_high_error = df['cumulative_net_value'] > 50 
    if mask_high_error.any():
        print(f"⚠️ 基金 {fund_code} 发现并修正了 {mask_high_error.sum()} 个极端净值异常点（>50）。")
        df.loc[mask_high_error, 'cumulative_net_value'] = df.loc[mask_high_error, 'cumulative_net_value'] / 100 
    
    df = df.dropna(subset=['cumulative_net_value', 'date'])

    # 【数据清洗 2：解决 -100% MDD 和 inf% 收益率的关键修正】
    # 将净值中小于或等于0的值设为 NaN，然后删除该行
    mask_zero_or_negative = df['cumulative_net_value'] <= 0
    if mask_zero_or_negative.any():
        print(f"💣 基金 {fund_code} 发现 {mask_zero_or_negative.sum()} 个零或负净值，已移除以确保指标计算有效。")
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
        
    # --- 5. 滚动年化收益率 (使用几何平均平滑异常值) ---
    rolling_metrics = {}
    
    for name, period_days in ROLLING_PERIODS.items():
        if len(cumulative_net_value) >= period_days:
            # 1. 计算所有非年化的期间收益率 (R_p)
            rolling_non_ann_returns = cumulative_net_value.pct_change(periods=period_days).dropna()
            
            # 2. 将收益率转换为 (1 + R_p)
            compounding_factors = 1 + rolling_non_ann_returns
            
            # 【优化：使用 EPSILON 避免 log(<=0) 导致的 RuntimeWarning / inf】
            compounding_factors = np.maximum(compounding_factors, EPSILON)

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
            try:
                df = pd.read_csv(file_path, encoding='utf-8')
            except UnicodeDecodeError:
                df = pd.read_csv(file_path, encoding='gbk')
            except pd.errors.ParserError:
                 df = pd.read_csv(file_path, encoding='utf-8', sep='\t')
            
            metrics, start_date, end_date = calculate_metrics(df.copy(), fund_code)
            
            if metrics:
                all_metrics.append(metrics)
                if start_date:
                    start_dates.append(start_date)
                if end_date:
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
    # 确保只对成功计算指标的基金获取信息
    fund_codes_to_fetch = [m['基金代码'] for m in all_metrics]
    print(f"\n--- 阶段 2/2: 多线程获取 {len(fund_codes_to_fetch)} 支基金的基本信息 ---")
    
    # 使用多线程加速信息抓取
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        future_to_code = {executor.submit(fetch_fund_info, code): code for code in fund_codes_to_fetch}
        
        # 实时打印信息，并等待所有抓取任务完成
        _ = [future.result() for future in concurrent.futures.as_completed(future_to_code)]

    # 阶段 3: 整合和输出
    print("\n--- 阶段 3/3: 整合数据并输出结果 ---")
    final_df = pd.DataFrame(all_metrics)

    # 从缓存中获取基金信息并整合
    info_list = [FUND_INFO_CACHE[code] for code in final_df['基金代码']]
    info_df = pd.DataFrame(info_list).rename(columns={'name': '基金简称', 'size': '资产规模', 'type': '基金类型', 'daily_growth': '最新日涨跌幅', 'net_value': '最新净值', 'rate': '管理费率'})
    
    # 重置索引以确保拼接对齐
    info_df.index = final_df.index
    final_df = pd.concat([info_df, final_df], axis=1)
    
    # 格式化百分比和数字
    for col in final_df.columns:
        if ('收益率' in col or '标准差' in col or '回撤' in col) and col != '夏普比率(总)':
            # 使用 try/except 捕获 inf 异常并处理为 N/A
            def format_pct(x):
                if pd.isna(x) or np.isinf(x):
                    return 'N/A'
                return f'{x * 100:.2f}%'
            final_df[col] = final_df[col].apply(format_pct)
        elif '夏普比率(总)' in col:
            def format_sharpe(x):
                if pd.isna(x) or np.isinf(x):
                    return 'N/A'
                return f'{x:.3f}'
            final_df[col] = final_df[col].apply(format_sharpe)
            # 创建一个用于排序的临时数字列
            final_df['夏普比率(总)_Num'] = final_df['夏普比率(总)'].replace({'N/A': np.nan}).astype(float)
            
    # 排序（按夏普比率降序）
    final_df = final_df.sort_values(by='夏普比率(总)_Num', ascending=False).drop(columns=['夏普比率(总)_Num']).reset_index(drop=True)
    
    # 输出共同分析期信息
    if latest_start and earliest_end:
        common_period = f'所有基金共同分析期：{latest_start.strftime("%Y-%m-%d")} 到 {earliest_end.strftime("%Y-%m-%d")}'
        print(common_period)
        
        # 创建一个包含共同分析期信息的新行
        period_info_row = pd.Series(
            {'基金简称': common_period, '基金代码': '所有基金共同分析期'},
            index=final_df.columns
        ).to_frame().T
        
        final_output = pd.concat([period_info_row, final_df], ignore_index=True)
    else:
        final_output = final_df
        print("未确定有效的共同分析期。")

    
    # 确保列顺序正确
    target_columns = [
        '基金简称', '资产规模', '基金类型', '最新日涨跌幅', '最新净值', 
        '管理费率', '基金代码', '起始日期', '结束日期', '年化收益率(总)', 
        '年化标准差(总)', '最大回撤(MDD)', '夏普比率(总)', 
        '平均滚动年化收益率(1月)', '平均滚动年化收益率(1季度)', 
        '平均滚动年化收益率(半年)', '平均滚动年化收益率(1年)'
    ]
    
    # 重新排列列，如果某个列缺失则忽略
    final_output = final_output[[col for col in target_columns if col in final_output.columns]]


    # 使用 utf_8_sig 编码以确保 Excel 中文不乱码
    final_output.to_csv(OUTPUT_FILE, index=False, encoding='utf_8_sig')
    print(f"\n✅ 成功：分析结果已保存至 {os.path.abspath(OUTPUT_FILE)}")
    
if __name__ == '__main__':
    main()
