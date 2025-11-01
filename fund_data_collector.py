import requests
import pandas as pd
import time
import os
from datetime import datetime

class FundDataCollector:
    # 1. 修正构造函数名：init -> __init__
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Referer": "http://fund.eastmoney.com/"
        }
        # 增加一个更完整的User-Agent

    def read_fund_codes(self, file_path):
        """
        从文件中读取基金代码列表。文件每行一个代码，可以包含'code'头。
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
            
            lines = content.split('\n')
            codes = []
            for line in lines:
                # 过滤空行和'code'头
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
        # 这个URL返回的是一个JavaScript文件，包含基本信息、净值、收益率等数据
        url = f"http://fund.eastmoney.com/pingzhongdata/{fund_code}.js"
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            response.encoding = 'utf-8'
            # 检查HTTP状态码
            if response.status_code == 200:
                 return response.text
            else:
                 print(f"获取基金{fund_code}信息失败: HTTP状态码 {response.status_code}")
                 return None
        except requests.exceptions.Timeout:
            print(f"获取基金{fund_code}信息超时 (10秒)")
            return None
        except requests.exceptions.RequestException as e:
            print(f"获取基金{fund_code}信息失败: {e}")
            return None

    def parse_fund_data(self, fund_code, raw_data):
        """
        从原始JavaScript字符串中解析出基金的基本信息。
        注意: '近1月/3月/1年收益' 等字段需要更复杂的解析，本方法中它们将保持为0。
        """
        fund_info = {
            '基金代码': fund_code,
            '基金名称': '未知',
            # DWJZ (单位净值) 字段通常为昨日净值或最新估算
            '单位净值': 0.0,
            # RZDF (日增长率) 字段通常为当日估算净值增长率
            '日增长率': 0.0, 
            '近1月收益': 0.0,
            '近3月收益': 0.0,
            '近1年收益': 0.0,
            '更新时间': datetime.now().strftime('%Y-%m-%d')
        }
        
        try:
            # --- 提取基金名称 fS_name ---
            if 'fS_name' in raw_data:
                # 预期格式如：var fS_name="基金名称";
                start_key = 'fS_name="'
                start = raw_data.find(start_key) + len(start_key)
                end = raw_data.find('";', start)
                if end > start:
                    fund_info['基金名称'] = raw_data[start:end].strip()

            # --- 提取单位净值 DWJZ ---
            if 'DWJZ' in raw_data:
                # 预期格式如：var DWJZ="1.2345";
                start_key = 'DWJZ="'
                start = raw_data.find(start_key) + len(start_key)
                end = raw_data.find('";', start)
                if end > start:
                    try:
                        fund_info['单位净值'] = float(raw_data[start:end].strip())
                    except ValueError:
                        pass # 忽略转换错误

            # --- 提取日增长率 RZDF ---
            if 'RZDF' in raw_data:
                # 预期格式如：var RZDF="0.55";
                start_key = 'RZDF="'
                start = raw_data.find(start_key) + len(start_key)
                end = raw_data.find('";', start)
                if end > start:
                    try:
                        fund_info['日增长率'] = float(raw_data[start:end].strip())
                    except ValueError:
                        pass # 忽略转换错误
                        
            # 近1月、近3月、近1年收益率数据需要解析如'Data_fundNetValues'中的历史净值或另一个数据接口
            # 这里保持为默认值0.0
            
        except Exception as e:
            print(f"解析基金{fund_code}数据时出错: {e}")
            
        return fund_info

    def collect_all_fund_data(self, fund_codes, output_dir='fund_data'):
        """
        遍历所有基金代码，收集数据。
        """
        all_fund_data = []
        
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        print(f"开始获取 {len(fund_codes)} 个基金的数据...")
        
        for i, fund_code in enumerate(fund_codes, 1):
            print(f"正在处理第 {i}/{len(fund_codes)} 个基金: {fund_code}")
            
            raw_data = self.get_fund_basic_info(fund_code)
            if raw_data:
                fund_info = self.parse_fund_data(fund_code, raw_data)
                all_fund_data.append(fund_info)
            
            # 设置合理的延迟，避免请求过快被屏蔽
            time.sleep(0.5) 
        
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
    
    # 假设你的基金代码文件名为 'C类.txt' 并且位于脚本同一目录下
    fund_codes = collector.read_fund_codes('C类.txt') 
    
    if not fund_codes:
        print("未读取到有效的基金代码，程序退出。")
        return
    
    fund_data = collector.collect_all_fund_data(fund_codes)
    
    if fund_data:
        # 数据将保存到 fund_data/ 目录下
        saved_file = collector.save_to_csv(fund_data) 
        print(f"任务完成！基金数据已保存至: {saved_file}")
    else:
        print("未能获取到任何基金数据")

# 2. 修正主执行块的条件判断
if __name__ == "__main__": 
    main()
