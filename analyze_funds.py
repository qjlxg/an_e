import pandas as pd
import numpy as np
import os
import re
import concurrent.futures 
from datetime import datetime, timedelta
import requests 

# --- 配置 ---
DATA_DIR = 'fund_data'
OUTPUT_FILE = 'fund_analysis_summary_with_info.csv'
RISK_FREE_RATE = 0.02 
TRADING_DAYS_PER_YEAR = 250
# 设定最大线程数，用于控制同时进行的网络请求数量
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
    根据基金代码从网络查找基金简称、最新净值、日涨跌幅、资产规模、基金类型和各项费率。
    """
    if fund_code in FUND_INFO_CACHE:
        return fund_code, FUND_INFO_CACHE[fund_code]

    search_url = f"http://fundf10.eastmoney.com/jbgk_{fund_code}.html"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Referer': 'http://fund.eastmoney.com/'
    }
    
    # 初始化所有结果为默认值
    defaults = {
        'name': f"名称查找失败({fund_code})",
        'latest_nav': 'N/A',
        'daily_return': 'N/A',
        'asset_size': 'N/A',
        'fund_type': 'N/A',
        'management_fee': 'N/A',
        'custody_fee': 'N/A',
        'sales_service_fee': 'N/A'
    }

    try:
        response = requests.get(search_url, headers=headers, timeout=10)
        response.raise_for_status() 
        response.encoding = 'utf-8'
        content = response.text
        
        # 1. 基金简称提取 (从表格中获取)
        match_name = re.search(r'<th>基金简称</th><td>([\u4e00-\u9fa5A-Za-z0-9()]+)</td>', content)
        if match_name:
            defaults['name'] = match_name.group(1).strip()
            
        # 2. 最新净值和日涨跌幅提取 (从快照区域获取)
        # 模式：单位净值（日期）：<b class="grn lar bold"> 净值 ( 涨跌幅 )</b>
        match_nav = re.search(r'单位净值.*?([\d\.]+)\s*\(\s*([-\+\d\.]+%)\s*\)', content, re.DOTALL)
        if match_nav:
            defaults['latest_nav'] = match_nav.group(1).strip()
            defaults['daily_return'] = match_nav.group(2).strip()
            
        # 3. 资产规模提取 (重点优化：尝试从两个区域提取)
        asset_size_value = 'N/A'
        
        # 尝试从 bs_gl 区域提取 (例如: 资产规模：<span> 3.85亿元 （截止至：2025-09-30）</span>)
        match_size_1 = re.search(r'资产规模：\s*<span>\s*(.*?)\s*（截止至', content)
        if match_size_1:
            asset_size_value = match_size_1.group(1).strip()
        else:
            # 尝试从表格区域提取 (例如: <th>资产规模</th><td>3.85亿元（截止至：2025年09月30日）)
            match_size_2 = re.search(r'<th>资产规模</th><td>(.*?)（截止至', content)
            if match_size_2:
                asset_size_value = match_size_2.group(1).strip()
        
        # 最终赋值
        defaults['asset_size'] = asset_size_value
        
        # 4. 基金类型提取 (从 bs_gl 区域获取)
        # 目标文本: 类型：<span>指数型-股票</span>
        match_type = re.search(r'类型：\s*<span>\s*(.*?)\s*</span>', content)
        if match_type:
            defaults['fund_type'] = match_type.group(1).strip()
            
        # 5. 管理费率提取 (从表格中获取)
        # 目标文本: <th>管理费率</th><td>1.20%（每年）</td>
        match_mgmt_fee = re.search(r'<th>管理费率</th><td>(.*?)</td>', content)
        if match_mgmt_fee:
            defaults['management_fee'] = match_mgmt_fee.group(1).strip()
            
        # 6. 托管费率提取 (从表格中获取)
        # 目标文本: <th>托管费率</th><td>0.20%（每年）</td>
        match_custody_fee = re.search(r'<th>托管费率</th><td>(.*?)</td>', content)
        if match_custody_fee:
            defaults['custody_fee'] = match_custody_fee.group(1).strip()

        # 7. 销售服务费率提取 (从表格中获取)
        # 目标文本: <th>销售服务费率</th><td>0.30%（每年）</td>
        match_sales_fee = re.search(r'<th>销售服务费率</th><td>(.*?)</td>', content)
        if match_sales_fee:
            defaults['sales_service_fee'] = match_sales_fee.group(1).strip()
            
    except requests.exceptions.RequestException:
        pass
    except Exception:
        pass
        
    FUND_INFO_CACHE[fund_code] = defaults
    return fund_code, defaults


# 【以下函数保持不变】
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
    fund_codes_to_fetch = [] 
    
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
                    fund_codes_to_fetch.append(filename.replace('.csv', '')) 
            else:
                 print(f"警告：文件 {filename} 缺少必要的 'date' 或 'cumulative_net_value' 列，已跳过。")
        except Exception as e:
            print(f"读取文件 {filename} 时发生错误: {e}")
            
    if latest_end_date <= earliest_start_date:
        print("错误：无法找到有效的共同分析期。请检查文件日期范围。")
        return

    print(f"确定共同分析期：{earliest_start_date.strftime('%Y-%m-%d')} 至 {latest_end_date.strftime('%Y-%m-%d')}")
    
    
    # 第二步：并行查找基金信息
    fund_info_map = {}
    print(f"正在并行查询 {len(fund_codes_to_fetch)} 个基金的最新信息，最大线程数: {MAX_THREADS}...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        future_to_code = {executor.submit(fetch_fund_info_from_internet, code): code for code in fund_codes_to_fetch}
        
        for future in concurrent.futures.as_completed(future_to_code):
            try:
                fund_code, info = future.result()
                fund_info_map[fund_code] = info
            except Exception as exc:
                code = future_to_code[future]
                print(f"基金 {code} 的信息查询发生异常: {exc}")

    # 第三步：计算指标并整合结果
    results = []
    
    for filename, df in temp_dfs.items():
        fund_code = filename.replace('.csv', '')
        
        fund_info = fund_info_map.get(fund_code, {
            'name': f"名称({fund_code})", 
            'latest_nav': 'N/A', 
            'daily_return': 'N/A',
            'asset_size': 'N/A',
            'fund_type': 'N/A',
            'management_fee': 'N/A',
            'custody_fee': 'N/A',
            'sales_service_fee': 'N/A'
        })
        
        # 提取信息
        fund_name = fund_info['name']
        latest_nav = fund_info['latest_nav']
        daily_return = fund_info['daily_return']
        asset_size = fund_info['asset_size']
        fund_type = fund_info['fund_type']
        management_fee = fund_info['management_fee']
        custody_fee = fund_info['custody_fee']
        sales_service_fee = fund_info['sales_service_fee']
        
        try:
            metrics = calculate_metrics(df, earliest_start_date, latest_end_date)
            
            if metrics:
                results.append({
                    '基金代码': fund_code,
                    '基金简称': fund_name, 
                    '最新单位净值': latest_nav, 
                    '日涨跌幅': daily_return,
                    '资产规模': asset_size, 
                    '基金类型': fund_type,
                    '管理费率': management_fee,
                    '销售服务费率': sales_service_fee,
                    '托管费率': custody_fee,
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
    
    # 重新排列列的顺序
    cols = summary_df.columns.tolist()
    new_info_cols = ['资产规模', '基金类型', '管理费率', '销售服务费率', '托管费率']
    core_info_cols = ['基金代码', '基金简称', '最新单位净值', '日涨跌幅']
    date_cols = ['起始日期', '结束日期']
    metric_cols = ['年化收益率(总)', '年化标准差(总)', '最大回撤(MDD)', '夏普比率(总)']
    rolling_cols = sorted([col for col in cols if col.startswith('平均滚动年化收益率')])

    final_cols_order = core_info_cols + new_info_cols + date_cols + metric_cols + rolling_cols

    # 确保所有列都在数据框中，并按最终顺序排列
    summary_df = summary_df.reindex(columns=final_cols_order, fill_value='N/A')
    
    # 处理夏普比率的排序和格式化
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
    print("\n按夏普比率排名的分析摘要（包含最新信息）：")
    
    print(summary_df.to_string(index=False))

if __name__ == '__main__':
    main()
