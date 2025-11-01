import requests
import pandas as pd
import time
import os
from datetime import datetime
import concurrent.futures
import re
import json 
from bs4 import BeautifulSoup 

class FundDataCollector:
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Referer": "http://fund.eastmoney.com/"
        }
        self.RATE_LIMIT_DELAY = 0.1 # 延时，防止触发反爬虫

    def read_fund_codes(self, file_path):
        """
        从文件中读取基金代码列表。
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
            
            lines = content.split('\n')
            codes = []
            for line in lines:
                if line.strip() and line.strip().lower() != 'code':
                    codes.append(line.strip())
            
            print(f"成功读取 {len(codes)} 个基金代码")
            return codes
        except FileNotFoundError:
            print(f"错误：文件未找到 at {file_path}")
            return []
        except Exception as e:
            print(f"读取文件失败: {e}")
            return []

    def get_fund_holdings(self, fund_code):
        """
        使用 FundArchivesDatas.aspx 接口 + HTML 解析获取前十大持仓数据。
        """
        url = f'http://fundf10.eastmoney.com/FundArchivesDatas.aspx'
        params = {
            'type': 'jjcc',
            'code': fund_code,
            'topline': '10',
            'year': '',
            'month': '',
            'rt': time.time() # 使用时间戳防止缓存
        }
        
        holdings_data = []
        try:
            time.sleep(self.RATE_LIMIT_DELAY)
            response = requests.get(url, params=params, headers=self.headers, timeout=10)
            response.encoding = 'utf-8'

            # 接口返回的是一个包含 HTML 字符串的 JSON
            data = response.json()
            
            if 'content' in data:
                soup = BeautifulSoup(data['content'], 'html.parser')
                tables = soup.find_all('table')
                
                for table in tables:
                    rows = table.find_all('tr')
                    
                    # 提取报告期信息
                    report_info_tag = table.find_previous_sibling('p')
                    report_date = '未知报告期'
                    if report_info_tag:
                         date_match = re.search(r'\d{4}-\d{2}-\d{2}', report_info_tag.text)
                         if date_match:
                             report_date = date_match.group(0)

                    for row in rows[1:]:  # 跳过表头
                        cols = row.find_all('td')
                        # 股票代码和名称通常在 cols[1] 和 cols[2]
                        if len(cols) >= 6:
                            holding_info = {
                                '基金代码': fund_code, # 添加基金代码，方便后续数据分析和关联
                                '报告期': report_date,
                                '股票代码': cols[1].text.strip(), 
                                '股票名称': cols[2].text.strip(), 
                                '持仓比例(%)': cols[4].text.strip(), # 占净值比例
                                '持股数': cols[5].text.strip()
                            }
                            holdings_data.append(holding_info)
                
                print(f"[{fund_code}] 成功获取 {len(holdings_data)} 条持仓记录。")
                return holdings_data
            else:
                return holdings_data
                
        except Exception as e:
            print(f"[{fund_code}] 获取基金持仓数据失败: {e}")
            return holdings_data
        
    def _process_single_fund(self, fund_code):
        """组合任务函数，用于线程池，仅处理持仓"""
        holdings = self.get_fund_holdings(fund_code)
        return holdings

    def collect_all_fund_data(self, fund_codes):
        """
        使用线程池并行遍历所有基金代码，仅收集持仓数据。
        返回 (持仓信息列表)
        """
        all_holdings_list = []
        
        print(f"开始并行获取 {len(fund_codes)} 个基金的持仓数据...")
        
        MAX_WORKERS = 10 
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_code = {executor.submit(self._process_single_fund, code): code for code in fund_codes}
            
            for future in concurrent.futures.as_completed(future_to_code):
                code = future_to_code[future]
                try:
                    holdings = future.result()
                    if holdings:
                        all_holdings_list.extend(holdings)
                except Exception as exc:
                    print(f"基金 {code} 在处理持仓过程中发生异常: {exc}")
        
        print(f"数据收集完毕，共成功获取 {len(all_holdings_list)} 条持仓记录。")
        return all_holdings_list

    def save_to_csv(self, data, filename_prefix):
        """
        通用保存函数。路径格式：YYYYMM/filename_prefix_...csv
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
        
        filename = f'{filename_prefix}_{timestamp}.csv'
        filepath = os.path.join(final_output_dir, filename)
        
        # 确保 DataFrame 字段顺序
        df = pd.DataFrame(data)
        
        # 重新排序字段，让基金代码、报告期、股票代码在前
        column_order = [
            '基金代码', 
            '报告期', 
            '股票代码', 
            '股票名称', 
            '持仓比例(%)', 
            '持股数'
        ]
        
        # 确保只有存在的列被选中
        final_columns = [col for col in column_order if col in df.columns]
        df = df[final_columns]
        
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
    
    # 确保从 'C类.txt' 读取基金代码
    fund_codes = collector.read_fund_codes('C类.txt') 
    
    if not fund_codes:
        print("未读取到有效的基金代码，程序退出。")
        return
    
    start_time = time.time()
    # 仅收集持仓数据
    all_holdings_list = collector.collect_all_fund_data(fund_codes)
    end_time = time.time()
    
    print(f"总耗时: {end_time - start_time:.2f} 秒")
    
    # 保存持仓信息
    saved_file_holdings = collector.save_to_csv(all_holdings_list, 'fund_holdings')
    if saved_file_holdings:
        print(f"任务完成！基金持仓数据已保存至: {saved_file_holdings}")


if __name__ == "__main__": 
    main()
