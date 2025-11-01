import requests
import pandas as pd
import time
import os
from datetime import datetime
import concurrent.futures
import re
import json 
from bs4 import BeautifulSoup # 确保已安装

class FundDataCollector:
    def __init__(self):
        self.headers = {
            # 使用更完整的User-Agent
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Referer": "http://fund.eastmoney.com/"
        }
        self.RATE_LIMIT_DELAY = 0.1 

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

    def get_fund_basic_info(self, fund_code):
        """
        [重写] 通过 F10DataApi 获取包含净值和收益率的 JSON/HTML 数据。
        """
        # API 用于获取基金的净值数据
        url = f'http://fundf10.eastmoney.com/F10DataApi.aspx'
        params = {
            'type': 'lsjz', # 历史净值数据
            'code': fund_code,
            'page': '1',
            'per': '1', # 只取最新一条数据
            'rt': time.time()
        }
        
        try:
            time.sleep(self.RATE_LIMIT_DELAY) 
            response = requests.get(url, params=params, headers=self.headers, timeout=10)
            response.encoding = 'utf-8'
            if response.status_code == 200:
                 return response.text
            else:
                 print(f"[{fund_code}] 获取基本信息失败: HTTP状态码 {response.status_code}")
                 return None
        except requests.exceptions.RequestException as e:
            print(f"[{fund_code}] 获取基本信息失败: {e}")
            return None

    def parse_fund_data(self, fund_code, raw_data):
        """
        [重写] 解析 F10DataApi 返回的 HTML 字符串以提取基本信息。
        """
        fund_info = {
            '基金代码': fund_code,
            '基金名称': '未知',
            '单位净值': 0.0,
            '日增长率': 0.0, 
            '近1月收益': 0.0, # 周期收益无法通过此单点API获取，暂仍为0.0
            '近3月收益': 0.0,
            '近1年收益': 0.0,
            '更新时间': datetime.now().strftime('%Y-%m-%d')
        }
        
        # 尝试从原始的 raw_data 中提取名称（如果可以的话）
        # 否则，可能需要通过另一个接口单独查询名称
        name_match = re.search(r'var fName\s*=\s*"([^"]*)";', raw_data)
        if name_match:
             fund_info['基金名称'] = name_match.group(1).strip()
             
        try:
            # 提取净值表格中的数据
            soup = BeautifulSoup(raw_data, 'html.parser')
            table = soup.find('table', class_='w782')
            
            if table:
                latest_row = table.find_all('tr')[1] # 第一行是表头，第二行是最新数据
                cols = latest_row.find_all('td')
                
                # 检查数据列的长度和内容
                if len(cols) >= 4:
                    # 索引 0: 净值日期
                    fund_info['更新时间'] = cols[0].text.strip()
                    
                    # 索引 1: 单位净值 (最新)
                    fund_info['单位净值'] = float(cols[1].text.strip() or 0.0) 
                    
                    # 索引 3: 日增长率 (需去除百分号)
                    daily_growth_str = cols[3].text.strip().replace('%', '')
                    fund_info['日增长率'] = float(daily_growth_str or 0.0)
                    
                    # 由于这个单点API不直接提供近1/3/1年收益率，我们保持 0.0
                    # 如果需要，需要单独再抓取原始JS文件或其它专门接口
                    
                    return fund_info
                    
        except Exception as e:
            # 如果解析失败，可能是数据格式不同或数据为空
            print(f"[{fund_code}] 解析基本数据失败: {e}")
            
        return fund_info


    def get_fund_holdings(self, fund_code):
        """
        使用 HTML 解析获取前十大持仓数据 (逻辑保持不变)。
        """
        url = f'http://fundf10.eastmoney.com/FundArchivesDatas.aspx'
        params = {
            'type': 'jjcc',
            'code': fund_code,
            'topline': '10',
            'year': '',
            'month': '',
            'rt': time.time()
        }
        
        holdings_data = []
        try:
            time.sleep(self.RATE_LIMIT_DELAY)
            response = requests.get(url, params=params, headers=self.headers, timeout=10)
            response.encoding = 'utf-8'

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
                        if len(cols) >= 6:
                            holding_info = {
                                '报告期': report_date,
                                '股票代码': cols[0].text.strip(),
                                '股票名称': cols[1].text.strip(),
                                '持仓比例(%)': cols[4].text.strip(), 
                                '持股数': cols[5].text.strip()
                            }
                            holdings_data.append(holding_info)
                
                return holdings_data
            else:
                return holdings_data
                
        except Exception as e:
            print(f"[{fund_code}] 获取基金持仓数据失败: {e}")
            return holdings_data
        
    def _process_single_fund(self, fund_code):
        """组合任务函数，用于线程池"""
        # 1. 获取基本信息 (新方法)
        raw_data = self.get_fund_basic_info(fund_code)
        basic_info = None
        if raw_data:
            basic_info = self.parse_fund_data(fund_code, raw_data)
        
        # 2. 获取持仓数据
        holdings = self.get_fund_holdings(fund_code)
        
        # 打印完成信息
        fund_name = basic_info.get('基金名称') if basic_info else '未知'
        print(f"[{fund_code}] 处理完成: {fund_name}")

        return basic_info, holdings

    def collect_all_fund_data(self, fund_codes, output_dir=''):
        """
        使用线程池并行遍历所有基金代码，收集基本信息和持仓数据。
        返回 (基本信息列表, 持仓信息字典)
        """
        all_fund_data = []
        all_holdings = {}
        
        print(f"开始并行获取 {len(fund_codes)} 个基金的数据...")
        
        MAX_WORKERS = 10 
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_code = {executor.submit(self._process_single_fund, code): code for code in fund_codes}
            
            for future in concurrent.futures.as_completed(future_to_code):
                code = future_to_code[future]
                try:
                    basic_info, holdings = future.result()
                    if basic_info and basic_info.get('单位净值') != 0.0:
                        all_fund_data.append(basic_info)
                    if holdings:
                        all_holdings[code] = holdings
                except Exception as exc:
                    print(f"基金 {code} 在处理过程中发生异常: {exc}")
        
        print(f"数据收集完毕，共成功获取 {len(all_fund_data)} 条记录，{len(all_holdings)} 个基金的持仓数据。")
        return all_fund_data, all_holdings

    def save_to_csv(self, data, filename_prefix, output_dir=''):
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
    fund_data, fund_holdings_dict = collector.collect_all_fund_data(fund_codes)
    end_time = time.time()
    
    print(f"总耗时: {end_time - start_time:.2f} 秒")
    
    # 1. 保存基本信息 (fund_data_...csv)
    saved_file_data = collector.save_to_csv(fund_data, 'fund_data') 
    if saved_file_data:
        print(f"任务完成！基本基金数据已保存至: {saved_file_data}")

    # 2. 扁平化并保存持仓信息 (fund_holdings_...csv)
    all_holdings_list = []
    for code, holdings in fund_holdings_dict.items():
        for holding in holdings:
            holding['基金代码'] = code # 添加基金代码字段
            all_holdings_list.append(holding)

    saved_file_holdings = collector.save_to_csv(all_holdings_list, 'fund_holdings')
    if saved_file_holdings:
        print(f"任务完成！基金持仓数据已保存至: {saved_file_holdings}")


if __name__ == "__main__": 
    main()
