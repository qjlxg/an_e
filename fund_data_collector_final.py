import requests
import pandas as pd
import time
import os
from datetime import datetime
import concurrent.futures
import re
import json # 新增：用于解析持仓数据

# 检查 Python 脚本顶部是否导入了 json：如果没导入，请加上。
# import json

class FundDataCollector:
    def __init__(self):
        # ... (__init__ 保持不变)
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Referer": "http://fund.eastmoney.com/"
        }
        self.RATE_LIMIT_DELAY = 0.1 

    # ... (read_fund_codes, get_fund_basic_info 保持不变)

    def parse_fund_data(self, fund_code, raw_data):
        # ... (此方法已包含前面提到的收益率解析修复)
        fund_info = {
            '基金代码': fund_code,
            '基金名称': '未知',
            '单位净值': 0.0,
            '日增长率': 0.0, 
            '近1月收益': 0.0,
            '近3月收益': 0.0,
            '近1年收益': 0.0,
            '更新时间': datetime.now().strftime('%Y-%m-%d')
        }
        
        try:
            # 辅助函数：使用正则表达式提取变量值
            def extract_var(pattern, default=None):
                # ... (extract_var 函数保持不变)
                match = re.search(pattern, raw_data)
                if match:
                    return match.group(1).strip().replace('"', '')
                return default

            # --- 1. 提取基本信息 (名称, 净值, 日增长率) ---
            fund_info['基金名称'] = extract_var(r'var fS_name\s*=\s*"([^"]*)";', '未知')

            dwjz_str = extract_var(r'var DWJZ\s*=\s*"([^"]*)";')
            if dwjz_str:
                try:
                    fund_info['单位净值'] = float(dwjz_str)
                except ValueError:
                    pass

            rzdf_str = extract_var(r'var RZDF\s*=\s*"([^"]*)";')
            if rzdf_str:
                try:
                    fund_info['日增长率'] = float(rzdf_str)
                except ValueError:
                    pass
            
            # --- 2. 修复后的周期收益率提取 ---
            returns_data = extract_var(r'var Data_fundReturns\s*=\s*(\[.*?\]);')
            
            if returns_data:
                try:
                    returns_list = eval(returns_data)
                    
                    if len(returns_list) > 0 and len(returns_list[0]) >= 6:
                        latest_returns = returns_list[0]
                        
                        # 修复后的安全转换函数
                        get_safe_float = lambda x: float(x) if x is not None and x != '' else 0.0

                        fund_info['近1月收益'] = get_safe_float(latest_returns[2])
                        fund_info['近3月收益'] = get_safe_float(latest_returns[3])
                        fund_info['近1年收益'] = get_safe_float(latest_returns[5])
                        
                except Exception as e:
                    print(f"[{fund_code}] 警告：无法解析收益率数据或列表格式错误: {e}")

        except Exception as e:
            print(f"[{fund_code}] 解析数据时发生整体错误: {e}")
            
        return fund_info


    def get_fund_holdings(self, fund_code):
        """
        尝试从东方财富的 API 获取基金的前十大持仓数据。
        注意：该数据更新频率低，通常为季度更新。
        """
        # 接口 URL 用于获取基金持仓
        # 这里的 Data_holder 接口通常包含持仓数据，但需要验证其精确性
        api_url = f"https://fundmobapi.eastmoney.com/FundMNewApi/FundHolderNewList?FCODE={fund_code}&pageSize=10&pageIndex=1&appID=9999&product=EFund&plat=Iphone&deviceid=918f0a14-46c5-430c-99c5-7f4625b15875&version=6.6.6"
        
        holdings_data = []
        try:
            time.sleep(self.RATE_LIMIT_DELAY)
            response = requests.get(api_url, headers=self.headers, timeout=10)
            response.encoding = 'utf-8'

            if response.status_code == 200:
                data = response.json()
                if data.get('ErrCode') == 0 and data.get('Datas'):
                    
                    # 报告期信息
                    report_date = data['Datas']['Holder']['ReportDate']
                    
                    # 股票持仓列表
                    stock_list = data['Datas']['Holder']['StockList']
                    
                    for stock in stock_list:
                        holdings_data.append({
                            '报告期': report_date,
                            '股票代码': stock.get('StockCode'),
                            '股票名称': stock.get('StockName'),
                            '占净值比例(%)': stock.get('RZ')
                        })
                return holdings_data
            else:
                 # 持仓 API 可能返回 404 或其他错误
                 return holdings_data
        except requests.exceptions.RequestException as e:
            print(f"[{fund_code}] 获取持仓信息失败: {e}")
            return holdings_data
        except json.JSONDecodeError:
            print(f"[{fund_code}] 持仓数据JSON解析失败。")
            return holdings_data
        
    def _process_single_fund(self, fund_code):
        # ... (此方法保持不变)
        # 仅获取基本信息，不包括持仓
        raw_data = self.get_fund_basic_info(fund_code)
        if raw_data:
            fund_info = self.parse_fund_data(fund_code, raw_data)
            if fund_info['基金名称'] != '未知':
                print(f"[{fund_code}] 处理完成: {fund_info['基金名称']}")
            return fund_info
        else:
            print(f"[{fund_code}] 未能获取数据，跳过。")
            return None

    def collect_all_fund_data(self, fund_codes, output_dir=''):
        """
        使用线程池并行遍历所有基金代码，收集基本信息和持仓数据。
        返回 (基本信息列表, 持仓信息字典)
        """
        all_fund_data = []
        all_holdings = {} # {fund_code: [holding1, holding2, ...]}
        
        # ... (目录创建等保持不变)
        
        print(f"开始并行获取 {len(fund_codes)} 个基金的数据...")
        
        MAX_WORKERS = 10 
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # 提交任务到线程池，执行一个组合任务函数
            def combined_task(code):
                basic_info = self._process_single_fund(code)
                holdings = self.get_fund_holdings(code)
                return basic_info, holdings
            
            future_to_code = {executor.submit(combined_task, code): code for code in fund_codes}
            
            for future in concurrent.futures.as_completed(future_to_code):
                code = future_to_code[future]
                try:
                    basic_info, holdings = future.result()
                    if basic_info:
                        all_fund_data.append(basic_info)
                    if holdings:
                        all_holdings[code] = holdings
                except Exception as exc:
                    print(f"基金 {code} 在处理过程中发生异常: {exc}")
        
        print(f"数据收集完毕，共成功获取 {len(all_fund_data)} 条记录，{len(all_holdings)} 个基金的持仓数据。")
        return all_fund_data, all_holdings


    def save_to_csv(self, data, filename_prefix, output_dir=''):
        """
        通用保存函数，支持保存基本信息和持仓信息。
        """
        if not data:
            print(f"没有 {filename_prefix} 数据可保存")
            return None
        
        now = datetime.now()
        timestamp = now.strftime('%Y%m%d_%H%M%S')
        year_month_dir = now.strftime('%Y%m') 
        
        final_output_dir = year_month_dir 
        
        if not os.path.exists(final_output_dir):
            os.makedirs(final_output_dir, exist_ok=True) 
            print(f"已创建新目录: {final_output_dir}")
        
        # 调整文件名以区分基本信息和持仓信息
        filename = f'{filename_prefix}_{timestamp}.csv'
        filepath = os.path.join(final_output_dir, filename)
        
        df = pd.DataFrame(data)
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        
        print("-" * 30)
        print(f"'{filename_prefix}' 数据已保存到: {filepath}")
        print(f"共保存 {len(data)} 条记录")
        print("-" * 30)
        
        return filepath

def main():
    """
    主执行函数。
    """
    collector = FundDataCollector()
    
    fund_codes = collector.read_fund_codes('C类.txt') 
    
    if not fund_codes:
        print("未读取到有效的基金代码，程序退出。")
        return
    
    start_time = time.time()
    # 接收两个返回结果：基本信息和持仓字典
    fund_data, fund_holdings_dict = collector.collect_all_fund_data(fund_codes)
    end_time = time.time()
    
    print(f"总耗时: {end_time - start_time:.2f} 秒")
    
    # 1. 保存基本信息
    if fund_data:
        saved_file_data = collector.save_to_csv(fund_data, 'fund_data') 
        print(f"任务完成！基本基金数据已保存至: {saved_file_data}")
    else:
        print("未能获取到任何基金基本数据")

    # 2. 扁平化并保存持仓信息
    if fund_holdings_dict:
        # 将持仓字典扁平化为列表
        all_holdings_list = []
        for code, holdings in fund_holdings_dict.items():
            for holding in holdings:
                holding['基金代码'] = code # 添加基金代码字段
                all_holdings_list.append(holding)

        if all_holdings_list:
            saved_file_holdings = collector.save_to_csv(all_holdings_list, 'fund_holdings')
            print(f"任务完成！基金持仓数据已保存至: {saved_file_holdings}")
        else:
             print("未能获取到任何基金持仓数据")
    else:
        print("未能获取到任何基金持仓数据")


if __name__ == "__main__": 
    main()
