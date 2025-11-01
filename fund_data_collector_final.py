import requests
import pandas as pd
import time
import os
from datetime import datetime
import concurrent.futures
import re # 导入正则表达式模块

class FundDataCollector:
    def __init__(self):
        self.headers = {
            # 使用更完整的User-Agent
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Referer": "http://fund.eastmoney.com/"
        }
        # 限制并发时的请求速率，避免被服务器拒绝
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
        使用正则表达式从原始JavaScript字符串中解析出基金的基本信息和近期收益率。
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
            # 辅助函数：使用正则表达式提取变量值
            def extract_var(pattern, default=None):
                # 匹配 JS 变量定义: var KEY="VALUE"; 或 var KEY = VALUE;
                match = re.search(pattern, raw_data)
                if match:
                    # 返回捕获组 (即括号中的内容)
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
            
            # --- 2. 提取并计算周期收益率 ---
            returns_data = extract_var(r'var Data_fundReturns\s*=\s*(\[.*?\]);')
            
            if returns_data:
                try:
                    returns_list = eval(returns_data)
                    # 索引 2: 近1月, 索引 3: 近3月, 索引 5: 近1年
                    if len(returns_list) > 0 and len(returns_list[0]) >= 6:
                        latest_returns = returns_list[0]
                        
                        fund_info['近1月收益'] = latest_returns[2] if latest_returns[2] is not None else 0.0
                        fund_info['近3月收益'] = latest_returns[3] if latest_returns[3] is not None else 0.0
                        fund_info['近1年收益'] = latest_returns[5] if latest_returns[5] is not None else 0.0
                        
                except Exception as e:
                    print(f"[{fund_code}] 警告：无法解析收益率数据或列表格式错误: {e}")


        except Exception as e:
            print(f"[{fund_code}] 解析数据时发生整体错误: {e}")
            
        return fund_info

    def _process_single_fund(self, fund_code):
        """处理单个基金的逻辑，供线程池调用"""
        raw_data = self.get_fund_basic_info(fund_code)
        if raw_data:
            fund_info = self.parse_fund_data(fund_code, raw_data)
            if fund_info['基金名称'] != '未知':
                print(f"[{fund_code}] 处理完成: {fund_info['基金名称']}")
            return fund_info
        else:
            print(f"[{fund_code}] 未能获取数据，跳过。")
            return None

    def collect_all_fund_data(self, fund_codes, output_dir=''): # output_dir 默认设置为空，但实际不影响此函数
        """
        使用线程池并行遍历所有基金代码，收集数据。
        """
        all_fund_data = []
        
        # 脚本的根目录不需要额外的 fund_data 目录创建，由 save_to_csv 处理
        
        print(f"开始并行获取 {len(fund_codes)} 个基金的数据...")
        
        MAX_WORKERS = 10 # 线程数
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

    def save_to_csv(self, fund_data, output_dir=''):
        """
        将收集到的数据保存到CSV文件，并放入以年月命名的子目录中。
        路径格式：202511/fund_data_...csv
        """
        if not fund_data:
            print("没有数据可保存")
            return None
        
        # --- 核心修改部分：创建年月子目录 (直接作为根目录) ---
        now = datetime.now()
        timestamp = now.strftime('%Y%m%d_%H%M%S')
        year_month_dir = now.strftime('%Y%m') # 例如: '202511'
        
        # 最终输出目录就是年月目录
        final_output_dir = year_month_dir 
        
        # 确保年月目录存在
        if not os.path.exists(final_output_dir):
            os.makedirs(final_output_dir, exist_ok=True) 
            print(f"已创建新目录: {final_output_dir}")
        # --- 核心修改部分结束 ---
        
        filename = f'fund_data_{timestamp}.csv'
        filepath = os.path.join(final_output_dir, filename)
        
        df = pd.DataFrame(fund_data)
        # 使用'utf-8-sig'编码以确保Excel等软件能正确识别中文
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
    
    # 请确保 'C类.txt' 文件存在于仓库根目录
    fund_codes = collector.read_fund_codes('C类.txt') 
    
    if not fund_codes:
        print("未读取到有效的基金代码，程序退出。")
        return
    
    start_time = time.time()
    # output_dir 参数保持默认，即空字符串
    fund_data = collector.collect_all_fund_data(fund_codes)
    end_time = time.time()
    
    print(f"总耗时: {end_time - start_time:.2f} 秒")
    
    if fund_data:
        # output_dir 参数保持默认，即空字符串
        saved_file = collector.save_to_csv(fund_data) 
        print(f"任务完成！基金数据已保存至: {saved_file}")
    else:
        print("未能获取到任何基金数据")

if __name__ == "__main__": 
    main()
