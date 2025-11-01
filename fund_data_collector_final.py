import requests
import pandas as pd
import time
import os
from datetime import datetime
import concurrent.futures
import re
from bs4 import BeautifulSoup 

class FundDataCollector:
    def __init__(self):
        self.headers = {
            # 模拟浏览器访问，非常重要
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
        通过直接请求 F10 持仓页面 (ccmx_{code}.html) 来抓取持仓数据。
        """
        # 直接目标 URL：基金 F10 档案的持仓页面
        url = f'http://fundf10.eastmoney.com/ccmx_{fund_code}.html'
        
        holdings_data = []
        try:
            time.sleep(self.RATE_LIMIT_DELAY)
            response = requests.get(url, headers=self.headers, timeout=10)
            response.encoding = 'utf-8'

            if response.status_code != 200:
                print(f"[{fund_code}] 访问持仓页面失败: HTTP状态码 {response.status_code}")
                return holdings_data
            
            # 响应是完整的 HTML 页面
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 基金持仓表格通常位于 <div id="cctable"> 内
            cctable_div = soup.find('div', id='cctable')
            if not cctable_div:
                print(f"[{fund_code}] 未找到持仓表格容器 div id='cctable'。")
                return holdings_data

            tables = cctable_div.find_all('table')
            
            for table in tables:
                rows = table.find_all('tr')
                
                # 提取报告期信息。它通常是表格前的 sibling
                report_info_tag = table.find_previous_sibling('div', style=lambda value: value and 'font-size' in value)
                report_date = '未知报告期'
                if report_info_tag:
                     # 报告信息格式: "汇添富中证芯片产业指数增强发起式C 2025年3季度股票投资明细"
                     date_match = re.search(r'\d{4}年\d季度|\d{4}-\d{2}-\d{2}', report_info_tag.text)
                     if date_match:
                         report_date = date_match.group(0).replace('年', '-').replace('季度', 'Q')

                for row in rows[1:]:  # 跳过表头
                    cols = row.find_all(['td', 'th'])
                    
                    # 检查列数，确保是持仓数据行
                    # 预期列索引: 0=序号, 1=股票代码, 2=股票名称, 4=占净值比例, 5=持股数
                    if len(cols) >= 6:
                        holding_info = {
                            '基金代码': fund_code, 
                            '报告期': report_date,
                            '股票代码': cols[1].text.strip(), 
                            '股票名称': cols[2].text.strip(), 
                            '持仓比例(%)': cols[4].text.strip(), 
                            '持股数': cols[5].text.strip()
                        }
                        holdings_data.append(holding_info)
            
            print(f"[{fund_code}] 成功获取 {len(holdings_data)} 条持仓记录。")
            return holdings_data
                
        except Exception as e:
            print(f"[{fund_code}] 获取基金持仓数据失败: {e}")
            return holdings_data
        
    def _process_single_fund(self, fund_code):
        """线程池任务函数，仅处理持仓"""
        return self.get_fund_holdings(fund_code)

    def collect_all_fund_data(self, fund_codes):
        """
        使用线程池并行遍历所有基金代码，仅收集持仓数据。
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
        保存函数。
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
            
        filename = f'{filename_prefix}_{timestamp}.csv'
        filepath = os.path.join(final_output_dir, filename)
        
        df = pd.DataFrame(data)
        
        # 确保字段顺序
        column_order = [
            '基金代码', 
            '报告期', 
            '股票代码', 
            '股票名称', 
            '持仓比例(%)', 
            '持股数'
        ]
        
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
    
    # 假设您的基金代码列表在 C类.txt 中
    fund_codes = collector.read_fund_codes('C类.txt') 
    
    if not fund_codes:
        print("未读取到有效的基金代码，程序退出。")
        return
    
    start_time = time.time()
    all_holdings_list = collector.collect_all_fund_data(fund_codes)
    end_time = time.time()
    
    print(f"总耗时: {end_time - start_time:.2f} 秒")
    
    # 保存持仓信息
    saved_file_holdings = collector.save_to_csv(all_holdings_list, 'fund_holdings')
    if saved_file_holdings:
        print(f"任务完成！基金持仓数据已保存至: {saved_file_holdings}")


if __name__ == "__main__": 
    # 在运行前请确保您已安装所需的库: pip install requests pandas beautifulsoup4
    main()
