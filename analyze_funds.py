import pandas as pd
import numpy as np
import os
import re
# 导入 concurrent.futures 用于并行处理
import concurrent.futures 
from datetime import datetime, timedelta
import requests 

# --- 配置 ---
DATA_DIR = 'fund_data'
OUTPUT_FILE = 'fund_analysis_summary_with_info.csv'
RISK_FREE_RATE = 0.02 
TRADING_DAYS_PER_YEAR = 250
# 设定最大线程数，用于控制同时进行的网络请求数量，例如 10 个线程并行查询
MAX_THREADS = 10 
# --- 配置结束 ---

# 定义滚动分析周期（以交易日近似）
ROLLING_PERIODS = {
    '1周': 5,
    '1月': 20,
    '1季度': 60,
    '半年': 125,
    '1年': 250
}

# 用于缓存已查询到的基金信息，避免重复网络请求
FUND_INFO_CACHE = {}


def fetch_fund_info_from_internet(fund_code):
    """
    根据基金代码从网络查找基金简称、最新净值和日涨跌幅。
    此函数现在由线程池并行调用。
    """
    if fund_code in FUND_INFO_CACHE:
        # 如果缓存命中，则直接返回，避免不必要的网络请求
        return fund_code, FUND_INFO_CACHE[fund_code]

    # 1. 构造目标 URL
    search_url = f"http://fundf10.eastmoney.com/jbgk_{fund_code}.html"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Referer': 'http://fund.eastmoney.com/'
    }
    
    # 默认值
    fund_name = f"名称查找失败({fund_code})"
    latest_nav = 'N/A'
    daily_return = 'N/A'

    try:
        response = requests.get(search_url, headers=headers, timeout=10)
        response.raise_for_status() 
        response.encoding = 'utf-8'
        content = response.text
        
        # 2. 基金简称提取
        match_name = re.search(r'基金简称\s*([\u4e00-\u9fa5A-Za-z0-9()]+)基金代码', content)
        if match_name:
            fund_name = match_name.group(1).strip()
            
        # 3. 最新净值和日涨跌幅提取
        match_nav = re.search(r'单位净值.*?\s*([\d\.]+)\s*\(\s*([-\+\d\.]+%)\s*\)', content)
        
        if match_nav:
            latest_nav = match_nav.group(1)
            daily_return = match_nav.group(2)
            
    except requests.exceptions.RequestException as e:
        # print(f"网络请求错误，无法获取 {fund_code} 信息: {e}") # 在多线程环境中，打印会影响性能，可以省略或写入日志
        pass
    except Exception as e:
        # print(f"数据解析错误，无法获取 {fund_code} 信息: {e}") 
        pass
        
    # 4. 整合结果
    result = {
        'name': fund_name,
        'latest_nav': latest_nav,
        'daily_return': daily_return
    }
    
    # 存储到缓存，并返回代码和信息（以便在主线程中对应结果）
    FUND_INFO_CACHE[fund_code] = result
    return fund_code, result


# 以下辅助函数保持不变：
def calculate_rolling_returns(cumulative_net_value, period_days):
    """计算指定周期（交易日）的平均滚动年化收益率"""
    
    rolling_returns = (cumulative_net_value.pct_change(periods=period_days) + 1).pow(TRADING_DAYS_PER_YEAR / period_days) - 1
    
    return rolling_returns.mean()

def calculate_metrics(df, start_date, end_date):
    """计算基金的关键指标：年化收益、年化标准差、最大回撤、夏普比率和滚动收益"""
    
    df = df[(df['date'] >= start_date) & (df['date'] <= end_date)].sort_values(by='date')
    
    if df.empty or len(df) < 2:
        return None

    cumulative_net_value = pd.to_numeric(df['cumulative_net_value'], errors='coerce').replace(0, np.nan).dropna()
    
    if len(cumulative_net_value) < 2:
          return None

    returns = cumulative_net_value.pct_change().dropna()
    
    total_days = (df['date'].iloc[-1] - df['date'].iloc[0]).days
    if total_days <= 0:
         return None
         
    total_return = (cumulative_net_value.iloc[-1] / cumulative_net_value.iloc[0]) - 1
    annual_return = (1 + total_return) ** (365 / total_days) - 1
    
    annual_volatility = returns.std() * np.sqrt(TRADING_DAYS_PER_YEAR)
    
    sharpe_ratio = (annual_return - RISK_FREE_RATE) / annual_volatility if annual_volatility != 0 else np.nan
    
    peak = cumulative_net_value.expanding(min_periods=1).max()
    drawdown = (cumulative_net_value / peak) - 1
    max_drawdown = drawdown.min()

    rolling_results = {}
    for name, days in ROLLING_PERIODS.items():
        if len(cumulative_net_value) >= days:
            rolling_return = calculate_rolling_returns(cumulative_net_value, days)
            rolling_results[f'平均滚动年化收益率({name})'] = rolling_return
        else:
             rolling_results[f'平均滚动年化收益率({name})'] = np.nan
    
    metrics = {
        '年化收益率(总)': annual_return,
        '年化标准差(总)': annual_volatility,
        '最大回撤(MDD)': max_drawdown,
        '夏普比率(总)': sharpe_ratio,
        **rolling_results
    }
    
    return metrics


def main():
    earliest_start_date = pd.to_datetime('1900-01-01')
    latest_end_date = pd.to_datetime('2200-01-01')
    
    if not os.path.isdir(DATA_DIR):
        print(f"错误：目录 {DATA_DIR} 不存在。请创建该目录并放入基金CSV文件。")
        file_list = []
    else:
        file_list = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
        
    
    # 第一步：确定共同分析期
    if not file_list:
        print(f"警告：{DATA_DIR} 目录中没有找到 CSV 文件。将跳过分析。")
        return
        
    temp_dfs = {} 
    fund_codes_to_fetch = [] # 存储所有需要查询的基金代码
    
    for filename in file_list:
        filepath = os.path.join(DATA_DIR, filename)
        try:
            df = pd.read_csv(filepath)
            df.columns = df.columns.str.lower()
            if 'date' in df.columns and 'cumulative_net_value' in df.columns:
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
                df = df.dropna(subset=['date'])
                
                valid_net_values = pd.to_numeric(df['cumulative_net_value'], errors='coerce').dropna()
                valid_dates = df.loc[valid_net_values.index, 'date']
                
                if not valid_dates.empty:
                    earliest_start_date = max(earliest_start_date, valid_dates.min())
                    latest_end_date = min(latest_end_date, valid_dates.max())
                    temp_dfs[filename] = df
                    fund_codes_to_fetch.append(filename.replace('.csv', '')) # 收集代码
            else:
                 print(f"警告：文件 {filename} 缺少必要的 'date' 或 'cumulative_net_value' 列，已跳过。")
        except Exception as e:
            print(f"读取文件 {filename} 时发生错误: {e}")
            
    if latest_end_date <= earliest_start_date:
        print("错误：无法找到有效的共同分析期。请检查文件日期范围。")
        return

    print(f"确定共同分析期：{earliest_start_date.strftime('%Y-%m-%d')} 至 {latest_end_date.strftime('%Y-%m-%d')}")
    
    
    # --- 关键修改：第二步：并行查找基金信息 ---
    fund_info_map = {}
    print(f"正在并行查询 {len(fund_codes_to_fetch)} 个基金的最新信息，最大线程数: {MAX_THREADS}...")
    
    # 创建线程池
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        # 提交所有查询任务
        future_to_code = {executor.submit(fetch_fund_info_from_internet, code): code for code in fund_codes_to_fetch}
        
        # 收集结果
        for future in concurrent.futures.as_completed(future_to_code):
            try:
                # future.result() 返回 fetch_fund_info_from_internet 的返回值 (fund_code, result)
                fund_code, info = future.result()
                fund_info_map[fund_code] = info
            except Exception as exc:
                code = future_to_code[future]
                print(f"基金 {code} 的信息查询发生异常: {exc}")

    # --- 并行查找结束 ---

    
    # 第三步：计算指标并整合结果
    results = []
    
    # 遍历已加载的 DataFrame 进行计算和结果整合
    for filename, df in temp_dfs.items():
        fund_code = filename.replace('.csv', '')
        
        # 从并行查询的结果中获取信息
        fund_info = fund_info_map.get(fund_code, {'name': f"名称({fund_code})", 'latest_nav': 'N/A', 'daily_return': 'N/A'})
        
        fund_name = fund_info['name']
        latest_nav = fund_info['latest_nav']
        daily_return = fund_info['daily_return']
        
        try:
            metrics = calculate_metrics(df, earliest_start_date, latest_end_date)
            
            if metrics:
                results.append({
                    '基金代码': fund_code,
                    '基金简称': fund_name, 
                    '最新单位净值': latest_nav, 
                    '日涨跌幅': daily_return, 
                    '起始日期': earliest_start_date.strftime('%Y-%m-%d'),
                    '结束日期': latest_end_date.strftime('%Y-%m-%d'),
                    **metrics
                })
        except Exception as e:
            print(f"计算文件 {filename} 的指标时发生错误: {e}")
            
    # 第四步：生成统计表和分析总结
    if not results:
        print("没有成功计算出任何基金的指标。")
        return
        
    summary_df = pd.DataFrame(results)
    
    # 调整列的顺序和格式化 (逻辑不变)
    cols = summary_df.columns.tolist()
    new_cols_order = ['基金代码', '基金简称', '最新单位净值', '日涨跌幅', '起始日期', '结束日期', 
                      '年化收益率(总)', '年化标准差(总)', '最大回撤(MDD)', '夏普比率(总)']
    
    remaining_cols = [col for col in cols if col not in new_cols_order and col != '夏普比率(总)_Num']
    final_cols_order = new_cols_order + sorted(remaining_cols)

    summary_df = summary_df.reindex(columns=final_cols_order)
    
    summary_df['夏普比率(总)_Num'] = pd.to_numeric(summary_df['夏普比率(总)'], errors='coerce') 
    
    for col in summary_df.columns:
        if '收益率' in col or '标准差' in col or '回撤' in col:
            summary_df[col] = pd.to_numeric(summary_df[col], errors='coerce').apply(lambda x: f"{x:.2%}" if pd.notna(x) else 'NaN')
        elif '夏普比率' in col and '_Num' not in col:
            summary_df[col] = pd.to_numeric(summary_df[col], errors='coerce').apply(lambda x: f"{x:.3f}" if pd.notna(x) else 'NaN')

    summary_df = summary_df.sort_values(by='夏普比率(总)_Num', ascending=False)
    summary_df = summary_df.drop(columns=['夏普比率(总)_Num'])
    
    summary_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
    print(f"\n--- 分析完成 ---\n结果已保存到 {OUTPUT_FILE}")
    print("\n按夏普比率排名的分析摘要（包含最新净值信息）：")
    
    print(summary_df.to_string(index=False))

if __name__ == '__main__':
    main()