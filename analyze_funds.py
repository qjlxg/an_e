import pandas as pd
import numpy as np
import os
from datetime import datetime

# --- 配置 ---
DATA_DIR = 'fund_data'
OUTPUT_FILE = 'fund_analysis_summary.csv'
RISK_FREE_RATE = 0.02  # 假设无风险利率为 2.0%
TRADING_DAYS_PER_YEAR = 250
# --- 配置结束 ---

def calculate_metrics(df, start_date, end_date):
    """计算基金的关键指标：年化收益、年化标准差、最大回撤、夏普比率"""
    
    # 1. 筛选共同分析期数据
    df = df[(df['date'] >= start_date) & (df['date'] <= end_date)].sort_values(by='date')
    
    # 确保有足够的数据点进行计算
    if df.empty or len(df) < 2:
        return None

    # 使用累计净值进行计算
    # 确保累计净值是数值类型，并去除0值和缺失值
    cumulative_net_value = pd.to_numeric(df['cumulative_net_value'], errors='coerce').replace(0, np.nan).dropna()
    
    if len(cumulative_net_value) < 2:
         return None

    # 2. 计算日收益率
    returns = cumulative_net_value.pct_change().dropna()
    
    # 3. 年化收益率 (Annualized Return)
    total_days = (df['date'].iloc[-1] - df['date'].iloc[0]).days
    # 使用几何平均法计算总收益
    total_return = (cumulative_net_value.iloc[-1] / cumulative_net_value.iloc[0]) - 1
    # 年化收益率
    annual_return = (1 + total_return) ** (365 / total_days) - 1

    # 4. 年化标准差 (Annualized Volatility)
    # 日标准差乘以根号250
    annual_volatility = returns.std() * np.sqrt(TRADING_DAYS_PER_YEAR)
    
    # 5. 夏普比率 (Sharpe Ratio)
    # 避免除以零
    if annual_volatility == 0:
        sharpe_ratio = np.nan
    else:
        sharpe_ratio = (annual_return - RISK_FREE_RATE) / annual_volatility
    
    # 6. 最大回撤 (Maximum Drawdown)
    # 计算累计净值序列的最大回撤
    peak = cumulative_net_value.expanding(min_periods=1).max()
    drawdown = (cumulative_net_value / peak) - 1
    max_drawdown = drawdown.min()
    
    return {
        'Annual_Return': annual_return,
        'Annual_Volatility': annual_volatility,
        'Max_Drawdown': max_drawdown,
        'Sharpe_Ratio': sharpe_ratio
    }

def main():
    all_data = []
    # 初始化为极早和极晚日期，以确定共同重叠期
    earliest_start_date = pd.to_datetime('1900-01-01')
    latest_end_date = pd.to_datetime('2200-01-01')
    
    file_list = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
    
    if not file_list:
        print(f"错误：{DATA_DIR} 目录中没有找到 CSV 文件。")
        return

    # 第一步：确定共同重叠期
    for filename in file_list:
        filepath = os.path.join(DATA_DIR, filename)
        try:
            # 鲁棒性增强：先读取，再标准化列名
            df = pd.read_csv(filepath)
            df.columns = df.columns.str.lower()
            
            if 'date' in df.columns and 'cumulative_net_value' in df.columns:
                df['date'] = pd.to_datetime(df['date'], errors='coerce') # 强制转换日期
                df = df.dropna(subset=['date']) # 去除无效日期行

                valid_dates = df.dropna(subset=['cumulative_net_value'])['date']
                if not valid_dates.empty:
                    earliest_start_date = max(earliest_start_date, valid_dates.min())
                    latest_end_date = min(latest_end_date, valid_dates.max())
            else:
                 print(f"警告：文件 {filename} 缺少必要的 'date' 或 'cumulative_net_value' 列，已跳过。")
                 
        except Exception as e:
            print(f"读取文件 {filename} 时发生错误: {e}")
            
    # 检查共同期是否有效
    if latest_end_date <= earliest_start_date:
        print("错误：无法找到有效的共同分析期。请检查文件日期范围。")
        return

    print(f"确定共同分析期：{earliest_start_date.strftime('%Y-%m-%d')} 至 {latest_end_date.strftime('%Y-%m-%d')}")
    
    # 第二步：计算所有基金的指标
    results = []
    for filename in file_list:
        fund_code = filename.replace('.csv', '')
        filepath = os.path.join(DATA_DIR, filename)
        
        try:
            df = pd.read_csv(filepath)
            df.columns = df.columns.str.lower()
            
            # 确保列名一致性，便于后续计算
            if 'date' in df.columns and 'cumulative_net_value' in df.columns:
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
                df = df.dropna(subset=['date'])
                metrics = calculate_metrics(df, earliest_start_date, latest_end_date)
            else:
                metrics = None
            
            if metrics:
                results.append({
                    'Fund_Code': fund_code,
                    'Start_Date': earliest_start_date.strftime('%Y-%m-%d'),
                    'End_Date': latest_end_date.strftime('%Y-%m-%d'),
                    'Annual_Return': metrics['Annual_Return'], # 暂时保留数字格式
                    'Annual_Volatility': metrics['Annual_Volatility'],
                    'Max_Drawdown': metrics['Max_Drawdown'],
                    'Sharpe_Ratio': metrics['Sharpe_Ratio']
                })
        except Exception as e:
            print(f"计算文件 {filename} 的指标时发生错误: {e}")
            
    # 第三步：生成统计表和分析总结
    if not results:
        print("没有成功计算出任何基金的指标。")
        return
        
    summary_df = pd.DataFrame(results)
    
    # 格式化输出
    summary_df['Annual_Return'] = summary_df['Annual_Return'].apply(lambda x: f"{x:.2%}")
    summary_df['Annual_Volatility'] = summary_df['Annual_Volatility'].apply(lambda x: f"{x:.2%}")
    summary_df['Max_Drawdown'] = summary_df['Max_Drawdown'].apply(lambda x: f"{x:.2%}")
    summary_df['Sharpe_Ratio_Num'] = pd.to_numeric(summary_df['Sharpe_Ratio'], errors='coerce') # 用于排序
    summary_df['Sharpe_Ratio'] = summary_df['Sharpe_Ratio'].apply(lambda x: f"{x:.3f}" if pd.notna(x) else 'NaN')
    
    # 按夏普比率降序排序
    summary_df = summary_df.sort_values(by='Sharpe_Ratio_Num', ascending=False)
    summary_df = summary_df.drop(columns=['Sharpe_Ratio_Num'])
    
    # 将结果保存到 CSV
    summary_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
    print(f"\n--- 分析完成 ---\n结果已保存到 {OUTPUT_FILE}")
    print("\n按夏普比率排名的分析摘要：")
    
    # 使用 to_string 代替 to_markdown 避免依赖问题
    print(summary_df.to_string(index=False))

if __name__ == '__main__':
    main()
