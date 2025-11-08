import pandas as pd
import numpy as np
import os
import re # 用于正则表达式处理
import random # 用于随机User-Agent和延迟
import time # 用于爬取延迟
import requests # 用于网络请求
import concurrent.futures # 用于多线程加速
from bs4 import BeautifulSoup # 用于HTML解析
from datetime import datetime, timedelta
import warnings

# 忽略 pandas 的 SettingWithCopyWarning
try:
    warnings.filterwarnings('ignore', category=pd.errors.SettingWithCopyWarning)
except AttributeError:
    warnings.filterwarnings('ignore', category=pd.core.common.SettingWithCopyWarning)

# --- 配置 ---
DATA_DIR = 'fund_data'
OUTPUT_FILE = 'fund_analysis_summary.csv'
RISK_FREE_RATE = 0.02 # 假设无风险利率为 2.0%
TRADING_DAYS_PER_YEAR = 250
# --- 新增配置：网络爬取相关 ---
MAX_THREADS = 10
FUND_INFO_CACHE = {}  # 缓存基金基本信息
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.3 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:99.0) Gecko/20100101 Firefox/99.0'
]
# --- 配置结束 ---

# 定义滚动分析周期（以交易日近似）
ROLLING_PERIODS = {
    '1周': 5,
    '1月': 20,
    '1季度': 60,
    '半年': 125,
    '1年': 250
}

# --- 优化函数：获取基金基本信息 (解决N/A问题) ---
def fetch_fund_info(fund_code):
    """
    从天天基金网获取基金的基本信息（基金简称、规模、类型、费率等）。
    优化了数据解析逻辑，以提高对资产规模和基金类型的爬取成功率。
    """
    global FUND_INFO_CACHE, USER_AGENTS 
    
    if fund_code in FUND_INFO_CACHE:
        return FUND_INFO_CACHE[fund_code]

    url = f'http://fundf10.eastmoney.com/jbgk_{fund_code}.html' 
    headers = {'User-Agent': random.choice(USER_AGENTS)}
    
    # 强制增加随机延迟 (4-6秒)，确保爬取成功率
    time.sleep(random.uniform(4, 6)) 

    defaults = {
        'code': fund_code, # 明确存储基金代码
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
        
        # 1. 提取基金简称
        title_tag = soup.select_one('.basic-new .bs_jz h4.title a')
        if title_tag and 'title' in title_tag.attrs:
            full_name = title_tag['title']
            # 移除括号中的代码
            defaults['name'] = re.sub(r'\(.*?\)$', '', full_name).strip() 

        # 2. 提取最新净值和日涨跌幅
        net_value_tag = soup.select_one('.basic-new .bs_jz .col-right .row1 b')
        if net_value_tag:
            text = net_value_tag.text.strip()
            # 分割净值和涨跌幅 (X.XX (Y.YY%))
            parts = re.split(r'\s*\((.*?)\)\s*', text, 1) 
            if len(parts) >= 3:
                defaults['net_value'] = parts[0].strip()
                defaults['daily_growth'] = f'({parts[1]})'
            else:
                 defaults['net_value'] = parts[0].strip()
                 
        # 3. 提取基金类型、资产规模、管理费率 (重点优化区域)
        
        # 3.1 优先从 table.info.w790 表格中查找 (更健壮的方式)
        info_table = soup.select_one('table.info.w790')
        if info_table:
            for th_tag in info_table.find_all('th'):
                th_text = th_tag.text.strip()
                td_tag = th_tag.find_next_sibling('td')
                
                if td_tag:
                    td_text = td_tag.text.strip().replace('\n', '').replace('\t', '')
                    
                    if '基金类型' in th_text:
                        defaults['type'] = td_text
                    
                    elif '资产规模' in th_text:
                        defaults['size'] = td_text
                        
                    elif '管理费率' in th_text:
                        defaults['rate'] = td_text

        # 3.2 补充：从 .bs_gl 区域的 label/span 结构中查找 (兼容旧版或不同页面结构)
        bs_gl = soup.select_one('.basic-new .bs_gl')
        if bs_gl:
            # 查找 基金类型 (如果 table.info.w790 没找到或数据更细致，会覆盖 N/A)
            type_label = bs_gl.find('label', string=re.compile(r'类型'))
            if type_label and type_label.find('span') and defaults['type'] == 'N/A':
                 defaults['type'] = type_label.find('span').text.strip()

            # 查找 资产规模
            size_label = bs_gl.find('label', string=re.compile(r'资产规模'))
            if size_label and size_label.find('span') and defaults['size'] == 'N/A':
                 defaults['size'] = size_label.find('span').text.strip().replace('\n', '').replace('\t', '')

    except requests.exceptions.RequestException as e:
        print(f"❌ 基金 {fund_code} 网络请求失败: {e}")
    except Exception as e:
        print(f"❌ 基金 {fund_code} 数据解析失败: {e}")
    
    # 打印获取到的关键信息，方便调试
    print(f"✅ 基金 {fund_code} 信息: 简称={defaults['name']}, 规模={defaults['size']}, 类型={defaults['type']}")
    
    FUND_INFO_CACHE[fund_code] = defaults
    return defaults
# --- 优化函数结束 ---


def calculate_rolling_returns(cumulative_net_value, period_days):
    """计算指定周期（交易日）的平均滚动年化收益率"""
    
    # 计算周期回报率 (Rolling Return)
    # (Value_t / Value_{t-n} - 1 + 1) ^ (T / n) - 1
    rolling_returns = (cumulative_net_value.pct_change(periods=period_days) + 1).pow(TRADING_DAYS_PER_YEAR / period_days) - 1
    
    # 返回所有滚动回报率的平均值（平均滚动年化收益率）
    return rolling_returns.mean()

def calculate_metrics(df, start_date, end_date):
    """计算基金的关键指标：年化收益、年化标准差、最大回撤、夏普比率和滚动收益"""
    
    # 1. 筛选共同分析期数据
    df = df[(df['date'] >= start_date) & (df['date'] <= end_date)].sort_values(by='date')
    
    if df.empty or len(df) < 2:
        return None

    # 确保累计净值是数值类型，并去除0值和缺失值
    cumulative_net_value = pd.to_numeric(df['cumulative_net_value'], errors='coerce').replace(0, np.nan).dropna()
    
    if len(cumulative_net_value) < 2:
          return None

    # 2. 计算日收益率
    returns = cumulative_net_value.pct_change().dropna()
    
    # 3. 长期指标
    total_days = (df['date'].iloc[-1] - df['date'].iloc[0]).days
    total_return = (cumulative_net_value.iloc[-1] / cumulative_net_value.iloc[0]) - 1
    
    # 使用几何平均年化
    annual_return = (1 + total_return) ** (365 / total_days) - 1
    
    annual_volatility = returns.std() * np.sqrt(TRADING_DAYS_PER_YEAR)
    
    # 避免除以零
    sharpe_ratio = (annual_return - RISK_FREE_RATE) / annual_volatility if annual_volatility != 0 else np.nan
    
    peak = cumulative_net_value.expanding(min_periods=1).max()
    drawdown = (cumulative_net_value / peak) - 1
    max_drawdown = drawdown.min()

    # 4. 短期滚动收益指标
    rolling_results = {}
    for name, days in ROLLING_PERIODS.items():
        # 确保数据长度足够进行滚动计算
        if len(cumulative_net_value) >= days:
            rolling_return = calculate_rolling_returns(cumulative_net_value, days)
            rolling_results[f'平均滚动年化收益率({name})'] = rolling_return
        else:
             rolling_results[f'平均滚动年化收益率({name})'] = np.nan
    
    # 5. 整合结果
    metrics = {
        '共同期年化收益率': annual_return,
        '共同期年化标准差': annual_volatility,
        '共同期最大回撤(MDD)': max_drawdown,
        '共同期夏普比率': sharpe_ratio,
        **rolling_results
    }
    
    return metrics

def main():
    # --- 共同期确定 ---
    earliest_start_date = pd.to_datetime('1900-01-01')
    latest_end_date = pd.to_datetime('2200-01-01')
    
    if not os.path.isdir(DATA_DIR):
        print(f"错误：未找到数据目录 '{DATA_DIR}'。请创建此目录并将CSV文件放入其中。")
        return
        
    file_list = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
    
    # 第一步：确定共同分析期
    print("--- 阶段 1/3: 确定共同分析期 ---")
    for filename in file_list:
        filepath = os.path.join(DATA_DIR, filename)
        try:
            # 兼容多种编码读取
            try:
                df = pd.read_csv(filepath, encoding='utf-8')
            except UnicodeDecodeError:
                 df = pd.read_csv(filepath, encoding='gbk')
            
            # 统一列名为小写，并兼容中文/英文列名
            df.columns = df.columns.astype(str).str.strip().str.lower()
            date_col = next((col for col in df.columns if '日期' in col or 'date' in col), None)
            net_value_col = next((col for col in df.columns if '累计净值' in col or 'cumulative_net_value' in col), None)

            if date_col and net_value_col:
                df = df.rename(columns={net_value_col: 'cumulative_net_value', date_col: 'date'})
                
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
                df = df.dropna(subset=['date'])
                # 只有非空的累计净值对应的日期才算有效
                valid_dates = df.dropna(subset=['cumulative_net_value'])['date']
                
                if not valid_dates.empty:
                    earliest_start_date = max(earliest_start_date, valid_dates.min())
                    latest_end_date = min(latest_end_date, valid_dates.max())
            else:
                print(f"警告：文件 {filename} 缺少必要的 '日期' 或 '累计净值' 列，已跳过。")
        except Exception as e:
            print(f"读取文件 {filename} 时发生错误: {e}")
            
    if latest_end_date <= earliest_start_date:
        print("错误：无法找到有效的共同分析期。请检查文件日期范围。")
        return

    print(f"✅ 确定共同分析期：{earliest_start_date.strftime('%Y-%m-%d')} 至 {latest_end_date.strftime('%Y-%m-%d')}")
    
    # --- 指标计算和数据爬取准备 ---
    results = []
    fund_codes_to_fetch = []

    print("\n--- 阶段 2/3: 计算指标并准备数据爬取列表 ---")
    for filename in file_list:
        fund_code = filename.replace('.csv', '')
        filepath = os.path.join(DATA_DIR, filename)
        
        try:
            # 兼容多种编码读取
            try:
                df = pd.read_csv(filepath, encoding='utf-8')
            except UnicodeDecodeError:
                 df = pd.read_csv(filepath, encoding='gbk')
                 
            # 统一列名为小写，并兼容中文/英文列名
            df.columns = df.columns.astype(str).str.strip().str.lower()
            date_col = next((col for col in df.columns if '日期' in col or 'date' in col), None)
            net_value_col = next((col for col in df.columns if '累计净值' in col or 'cumulative_net_value' in col), None)

            if date_col and net_value_col:
                df = df.rename(columns={net_value_col: 'cumulative_net_value', date_col: 'date'})
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
                df = df.dropna(subset=['date'])
                
                metrics = calculate_metrics(df, earliest_start_date, latest_end_date)
            else:
                metrics = None
            
            if metrics:
                results.append({
                    '基金代码': fund_code,
                    '起始日期': earliest_start_date.strftime('%Y-%m-%d'),
                    '结束日期': latest_end_date.strftime('%Y-%m-%d'),
                    **metrics
                })
                fund_codes_to_fetch.append(fund_code)
        except Exception as e:
            print(f"计算文件 {filename} 的指标时发生错误: {e}")

    summary_df = pd.DataFrame(results)

    # --- 阶段 3/3: 多线程获取基金基本信息并整合 ---
    print(f"\n--- 阶段 3/3: 多线程获取 {len(fund_codes_to_fetch)} 支基金的基本信息 ---")
    
    # 使用多线程加速爬取
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        # 提交爬取任务
        future_to_code = {executor.submit(fetch_fund_info, code): code for code in fund_codes_to_fetch}
        # 等待所有爬取任务完成
        for future in concurrent.futures.as_completed(future_to_code):
            try:
                # 触发结果获取，确保异常被捕获
                future.result() 
            except Exception as exc:
                code = future_to_code[future]
                print(f"爬取基金 {code} 时发生异常: {exc}")

    # 从缓存中获取基金信息并整合
    info_list = [FUND_INFO_CACHE.get(code, {}) for code in summary_df['基金代码']]
    info_df = pd.DataFrame(info_list).rename(columns={
        'code': '基金代码_info',
        'name': '基金简称', 
        'size': '资产规模', 
        'type': '基金类型', 
        'daily_growth': '最新日涨跌幅', 
        'net_value': '最新净值', 
        'rate': '管理费率'
    })
    
    # 合并基金代码列（以分析结果中的为准）
    info_df['基金代码'] = info_df['基金代码_info'].fillna(summary_df['基金代码'])
    info_df = info_df.drop(columns=['基金代码_info'])
    
    # 拼接数据
    summary_df.index = info_df.index
    final_df = pd.concat([info_df, summary_df.drop(columns=['基金代码'])], axis=1)

    # --- 格式化、排序和最终列顺序调整 ---
    
    # 用于排序的数值列
    final_df['共同期夏普比率_Num'] = pd.to_numeric(final_df['共同期夏普比率'], errors='coerce')
    
    for col in final_df.columns:
        if '收益率' in col or '标准差' in col or '回撤' in col:
            # 应用百分比格式
            final_df[col] = pd.to_numeric(final_df[col], errors='coerce').apply(lambda x: f"{x:.2%}" if pd.notna(x) and not np.isinf(x) else 'N/A')
        elif '夏普比率' in col and '_Num' not in col:
            # 应用夏普比率格式
            final_df[col] = pd.to_numeric(final_df[col], errors='coerce').apply(lambda x: f"{x:.3f}" if pd.notna(x) and not np.isinf(x) else 'N/A')

    # 按夏普比率降序排序
    final_df = final_df.sort_values(by='共同期夏普比率_Num', ascending=False)
    final_df = final_df.drop(columns=['共同期夏普比率_Num']).reset_index(drop=True)
    
    # 创建共同分析期信息行
    common_period_info = f"所有基金共同分析期：{earliest_start_date.strftime('%Y-%m-%d')} 到 {latest_end_date.strftime('%Y-%m-%d')}"
    period_info_row = pd.Series(
        {'基金简称': common_period_info, '基金代码': '分析期信息'},
        index=final_df.columns
    ).to_frame().T
    final_output = pd.concat([period_info_row, final_df], ignore_index=True)


    # --- 关键修改：强制将 基金代码 放在第一列 ---
    target_columns_order = [
        '基金代码', '基金简称', '资产规模', '基金类型', '最新日涨跌幅', '最新净值', 
        '管理费率', '起始日期', '结束日期', 
        '共同期年化收益率', '共同期年化标准差', '共同期最大回撤(MDD)', 
        '共同期夏普比率'
    ] + [col for col in final_output.columns if '平均滚动年化收益率' in col]

    # 重新排列列
    final_output = final_output[[col for col in target_columns_order if col in final_output.columns]]


    # 使用 utf_8_sig 编码以确保 Excel 中文不乱码
    final_output.to_csv(OUTPUT_FILE, index=False, encoding='utf_8_sig')
    
    print(f"\n--- 分析完成 ---\n结果已保存到 {os.path.abspath(OUTPUT_FILE)}")
    print("\n按夏普比率排名的分析摘要：")
    
    # 使用 to_string 代替 to_markdown 避免依赖问题
    print(final_output.to_string(index=False))

if __name__ == '__main__':
    main()
