import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
from io import StringIO # 用于处理上传的文件

# --- 配置 ---
# 假设您的文件位于当前工作目录下，而不是一个单独的 'fund_data' 文件夹
# 为了与Colab/Jupyter环境和文件上传兼容，我们修改文件读取逻辑
# DATA_DIR = 'fund_data'
OUTPUT_FILE = 'fund_analysis_summary.csv'
RISK_FREE_RATE = 0.02  # 假设无风险利率为 2.0%
TRADING_DAYS_PER_YEAR = 250
# --- 配置结束 ---

# 定义滚动分析周期（以交易日近似）
# 1周: 5天, 1月: 20天, 1季度: 60天, 半年: 125天, 1年: 250天
ROLLING_PERIODS = {
    '1周': 5,
    '1月': 20,
    '1季度': 60,
    '半年': 125,
    '1年': 250
}

def calculate_rolling_returns(cumulative_net_value, period_days):
    """计算指定周期（交易日）的平均滚动年化收益率"""
    
    # 计算周期回报率 (Rolling Return)
    rolling_returns = (cumulative_net_value.pct_change(periods=period_days) + 1).pow(TRADING_DAYS_PER_YEAR / period_days) - 1
    
    # 返回所有滚动回报率的平均值（平均滚动年化收益率）
    return rolling_returns.mean()

def calculate_metrics(df, start_date, end_date):
    """计算基金的关键指标：年化收益、年化标准差、最大回撤、夏普比率和滚动收益"""
    
    # 1. 筛选共同分析期数据
    df['date'] = pd.to_datetime(df['date'], errors='coerce') # 确保日期类型
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
        '年化收益率(总)': annual_return,
        '年化标准差(总)': annual_volatility,
        '最大回撤(MDD)': max_drawdown,
        '夏普比率(总)': sharpe_ratio,
        **rolling_results
    }
    
    return metrics

def main():
    # 为了在Jupyter/Colab中运行，我们从全局变量访问上传的文件
    uploaded_files = {
        "001593.csv": globals().get("uploaded:001593.csv"),
        "001618.csv": globals().get("uploaded:001618.csv"),
        "004348.csv": globals().get("uploaded:004348.csv"),
        "005919.csv": globals().get("uploaded:005919.csv"),
        "007301.csv": globals().get("uploaded:007301.csv"),
        "008001.csv": globals().get("uploaded:008001.csv"),
        "008888.csv": globals().get("uploaded:008888.csv"),
        "011036.csv": globals().get("uploaded:011036.csv"),
        "011609.csv": globals().get("uploaded:011609.csv"),
        "011833.csv": globals().get("uploaded:011833.csv"),
        "012553.csv": globals().get("uploaded:012553.csv"),
    }
    
    # 过滤掉未上传的文件
    available_files = {k: v for k, v in uploaded_files.items() if v}
    file_list = list(available_files.keys())

    earliest_start_date = pd.to_datetime('1900-01-01')
    latest_end_date = pd.to_datetime('2200-01-01')
    
    if not file_list:
        print("错误：没有找到可用的基金数据 CSV 文件。")
        return
        
    all_dfs = {}
    
    # 第一步：确定共同分析期
    for filename in file_list:
        try:
            # 从内存中的文件内容读取
            file_content = available_files[filename]['snippetFromFront'] + available_files[filename]['snippetFromBack']
            df = pd.read_csv(StringIO(file_content), skipinitialspace=True)
            df.columns = df.columns.str.lower()
            
            if 'date' in df.columns and 'cumulative_net_value' in df.columns:
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
                df = df.dropna(subset=['date'])
                valid_dates = df.dropna(subset=['cumulative_net_value'])['date']
                
                if not valid_dates.empty:
                    earliest_start_date = max(earliest_start_date, valid_dates.min())
                    latest_end_date = min(latest_end_date, valid_dates.max())
                    all_dfs[filename] = df
                else:
                    print(f"警告：文件 {filename} 累计净值数据缺失，已跳过。")
            else:
                print(f"警告：文件 {filename} 缺少必要的 'date' 或 'cumulative_net_value' 列，已跳过。")
        except Exception as e:
            print(f"读取文件 {filename} 时发生错误: {e}")
            
    if latest_end_date <= earliest_start_date:
        print("错误：无法找到有效的共同分析期。请检查文件日期范围。")
        return

    print(f"确定共同分析期：{earliest_start_date.strftime('%Y-%m-%d')} 至 {latest_end_date.strftime('%Y-%m-%d')}")
    
    # 第二步：计算所有基金的指标
    results = []
    for filename, df in all_dfs.items():
        fund_code = filename.replace('.csv', '')
        try:
            metrics = calculate_metrics(df, earliest_start_date, latest_end_date)
            
            if metrics:
                results.append({
                    '基金代码': fund_code,
                    '起始日期': earliest_start_date.strftime('%Y-%m-%d'),
                    '结束日期': latest_end_date.strftime('%Y-%m-%d'),
                    **metrics
                })
        except Exception as e:
            print(f"计算文件 {filename} 的指标时发生错误: {e}")
            
    if not results:
        print("没有成功计算出任何基金的指标。")
        return
        
    summary_df = pd.DataFrame(results)
    
    # 格式化前的数值列
    summary_df['夏普比率(总)_Num'] = pd.to_numeric(summary_df['夏普比率(总)'], errors='coerce')
    summary_df['年化标准差(总)_Num'] = pd.to_numeric(summary_df['年化标准差(总)'], errors='coerce')
    summary_df['平均滚动年化收益率(1周)_Num'] = pd.to_numeric(summary_df['平均滚动年化收益率(1周)'], errors='coerce')
    
    # 格式化百分比和夏普比率
    for col in summary_df.columns:
        if '收益率' in col or '标准差' in col or '回撤' in col:
            summary_df[col] = pd.to_numeric(summary_df[col], errors='coerce').apply(lambda x: f"{x:.2%}" if pd.notna(x) else 'NaN')
        elif '夏普比率' in col and '_Num' not in col:
            summary_df[col] = pd.to_numeric(summary_df[col], errors='coerce').apply(lambda x: f"{x:.3f}" if pd.notna(x) else 'NaN')

    # 按夏普比率降序排序
    summary_df = summary_df.sort_values(by='夏普比率(总)_Num', ascending=False).reset_index(drop=True)
    summary_df_final = summary_df.drop(columns=['夏普比率(总)_Num', '年化标准差(总)_Num', '平均滚动年化收益率(1周)_Num'])
    
    # 将结果保存到 CSV
    summary_df_final.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
    print(f"\n--- 分析完成 ---\n结果已保存到 {OUTPUT_FILE}")
    print("\n按夏普比率排名的分析摘要：")
    
    # 打印表格
    print(summary_df_final.to_string(index=False))

    # --- 第四步：自动生成分析结论 ---
    
    # 提取用于生成结论的原始数值数据
    top_funds = summary_df.head(3).copy() 
    
    # 将格式化前的数值列重命名，方便访问
    top_funds['Sharpe'] = top_funds['夏普比率(总)_Num']
    top_funds['StdDev'] = top_funds['年化标准差(总)_Num']
    top_funds['AvgReturn1W'] = top_funds['平均滚动年化收益率(1周)_Num']
    
    if not top_funds.empty:
        best_sharpe_fund = top_funds.iloc[0]
        
        # 找出短期爆发力（1周平均收益）最高的基金
        best_1w_return_fund = summary_df.loc[summary_df['平均滚动年化收益率(1周)_Num'].idxmax()]
        
        # 找出风险（标准差）最低的基金
        lowest_risk_fund = summary_df.loc[summary_df['年化标准差(总)_Num'].idxmin()]

        print("\n" + "="*50)
        print("⭐ 自动生成的短线交易结论（基于共同分析期）")
        print("="*50)
        
        # 结论 1: 最佳效率标的 (高夏普比率)
        print(f"## 1. 最佳综合效率标的 (短期安全与爆发力平衡)")
        print(f"基金代码：**{best_sharpe_fund['基金代码']}**")
        print(f"夏普比率：{best_sharpe_fund['夏普比率(总)']} | 年化标准差：{best_sharpe_fund['年化标准差(总)']}")
        print("> 结论：该基金在风险控制和回报获取上取得了最佳平衡。在短线操作中，这意味着其**短期上涨的确定性（效率）最高**，是首选的综合工具。")

        # 结论 2: 极致爆发力标的 (最高1周平均收益)
        print("\n## 2. 极致爆发力标的 (追求最高赔率)")
        print(f"基金代码：**{best_1w_return_fund['基金代码']}**")
        print(f"平均滚动年化收益率(1周)：{best_1w_return_fund['平均滚动年化收益率(1周)']} | 年化标准差：{best_1w_return_fund['年化标准差(总)']}")
        print("> 结论：该基金在短期内具有最强的弹性，但通常伴随**极高的波动性**。仅建议在明确的**行业催化剂出现时**，作为严格快进快出的投机工具。")

        # 结论 3: 最安全短线标的 (最低标准差)
        print("\n## 3. 最安全短线标的 (风险控制优先)")
        print(f"基金代码：**{lowest_risk_fund['基金代码']}**")
        print(f"年化标准差：{lowest_risk_fund['年化标准差(总)']} | 最大回撤：{lowest_risk_fund['最大回撤(MDD)']}")
        print("> 结论：该基金的波动性最低，**最适合用于7天免手续费的策略**。即使短期判断失误，**持仓压力和回撤风险也最小**。适合稳健的波段操作。")

        print("\n" + "="*50)

if __name__ == '__main__':
    main()
