import requests
import pandas as pd
import time
import os
from datetime import datetime
import concurrent.futures # 导入并行处理模块

class FundDataCollector:
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Referer": "http://fund.eastmoney.com/"
        }
        # 限制并发时的请求速率
        self.RATE_LIMIT_DELAY = 0.1 # 每个请求间隔至少 100ms

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
        通过基金代码获取包含基本信息的JavaScript数据。
        """
        url = f"http://fund.eastmoney.com/pingzhongdata/{fund_code}.js"
        try:
            # 并行时需要注意速率限制
            time.sleep(self.RATE_LIMIT_DELAY) 
            response = requests.get(url, headers=self.headers, timeout=10)
            response.encoding = 'utf-8'
            if response.status_code == 200:
                 return response.text
            else:
                 print(f"[{fund_code}] 获取信息失败: HTTP状态码 {response.status_code}")
                 return None
        except requests.exceptions.Timeout:
            print(f"[{fund_code}] 获取信息超时 (10秒)")
            return None
        except requests.exceptions.RequestException as e:
            print(f"[{fund_code}] 获取信息失败: {e}")
            return None

    def parse_fund_data(self, fund_code, raw_data):
        """
        从原始JavaScript字符串中解析出基金的基本信息。
        """
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
            # 提取基金名称 fS_name
            if 'fS_name' in raw_data:
                start_key = 'fS_name="'
                start = raw_data.find(start_key) + len(start_key)
                end = raw_data.find('";', start)
                if end > start:
                    fund_info['基金名称'] = raw_data[start:end].strip()

            # 提取单位净值 DWJZ
            if 'DWJZ' in raw_data:
                start_key = 'DWJZ="'
                start = raw_data.find(start_key) + len(start_key)
                end = raw_data.find('";', start)
                if end > start:
                    try:
                        fund_info['单位净值'] = float(raw_data[start:end].strip())
                    except ValueError:
                        pass 

            # 提取日增长率 RZDF
            if 'RZDF' in raw_data:
                start_key = 'RZDF="'
                start = raw_data.find(start_key) + len(start_key)
                end = raw_data.find('";', start)
                if end > start:
                    try:
                        fund_info['日增长率'] = float(raw_data[start:end].strip())
                    except ValueError:
                        pass 
            
        except Exception as e:
            print(f"[{fund_code}] 解析数据时出错: {e}")
            
        return fund_info

    def _process_single_fund(self, fund_code):
        """处理单个基金的逻辑，供线程池调用"""
        raw_data = self.get_fund_basic_info(fund_code)
        if raw_data:
            fund_info = self.parse_fund_data(fund_code, raw_data)
            print(f"[{fund_code}] 处理完成: {fund_info['基金名称']}")
            return fund_info
        else:
            print(f"[{fund_code}] 未能获取数据，跳过。")
            return None

    def collect_all_fund_data(self, fund_codes, output_dir='fund_data'):
        """
        使用线程池并行遍历所有基金代码，收集数据。
        """
        all_fund_data = []
        
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        print(f"开始并行获取 {len(fund_codes)} 个基金的数据...")
        
        # 使用 ThreadPoolExecutor 进行线程并行
        MAX_WORKERS = 10 # 设置最大线程数，避免过度并发导致被封
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # 提交所有任务到线程池
            future_to_code = {executor.submit(self._process_single_fund, code): code for code in fund_codes}
            
            # 迭代已完成的任务结果
            for future in concurrent.futures.as_completed(future_to_code):
                try:
                    fund_info = future.result()
                    if fund_info:
                        all_fund_data.append(fund_info)
                except Exception as exc:
                    code = future_to_code[future]
                    print(f"基金 {code} 在处理过程中发生异常: {exc}")
        
        print(f"数据收集完毕，共成功获取 {len(all_fund_data)} 条记录。")
        return all_fund_data

    def save_to_csv(self, fund_data, output_dir='fund_data'):
        """
        将收集到的数据保存到CSV文件。
        """
        if not fund_data:
            print("没有数据可保存")
            return None
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'fund_data_{timestamp}.csv'
        filepath = os.path.join(output_dir, filename)
        
        df = pd.DataFrame(fund_data)
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        
        print("-" * 30)
        print(f"数据已保存到: {filepath}")
        print(f"共保存 {len(fund_data)} 条基金记录")
        print("-" * 30)
        
        return filepath

def main():
    """
    主执行函数。
    """
    collector = FundDataCollector()
    
    # 请确保 'C类.txt' 文件存在
    fund_codes = collector.read_fund_codes('C类.txt') 
    
    if not fund_codes:
        print("未读取到有效的基金代码，程序退出。")
        return
    
    start_time = time.time()
    fund_data = collector.collect_all_fund_data(fund_codes)
    end_time = time.time()
    
    print(f"总耗时: {end_time - start_time:.2f} 秒")
    
    if fund_data:
        saved_file = collector.save_to_csv(fund_data) 
        print(f"任务完成！基金数据已保存至: {saved_file}")
    else:
        print("未能获取到任何基金数据")

if __name__ == "__main__": 
    main()
