# In[]:
#!/usr/bin/env python
# coding: utf-8
# encoding=utf-8
import pandas as pd
import requests
from lxml import etree
import re
import time
from datetime import datetime
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys

# --- 1. 读取 C 类基金代码列表 ---
try:
    # 尝试获取文件内容。如果文件不可用，则跳过。
    # 因为您提到文件已上传，我们假设可以直接读取。
    with open('C类.txt', 'r', encoding='utf-8') as f:
        all_lines = [line.strip() for line in f if line.strip()]
    
    # 检查并移除可能的列标题 'code'
    if all_lines and all_lines[0].lower() in ['code', '基金代码']:
        c_class_codes = all_lines[1:]
    else:
        c_class_codes = all_lines
        
except FileNotFoundError:
    print("错误：未找到 'C类.txt' 文件，请确保文件位于脚本运行目录下！")
    sys.exit(1)
except Exception as e:
    print(f"读取文件时发生错误: {e}")
    sys.exit(1)

if not c_class_codes:
    print("基金代码列表为空，脚本将提前退出。")
    sys.exit(1)

# In[]:
# --- 2. 参数设置 ---
# season=1 表示爬取最新一期（即 div[1]）的持仓数据。
season = 1 
MAX_WORKERS = 20 # 并发线程数
total = len(c_class_codes)

# 爬虫 headers
head = {
"Cookie": "EMFUND1=null; EMFUND2=null; EMFUND3=null; EMFUND4=null; EMFUND5=null; EMFUND6=null; EMFUND7=null; EMFUND8=null; EMFUND0=null; st_si=44023331838789; st_asi=delete; EMFUND9=08-16 22:04:25@#$%u4E07%u5BB6%u65B0%u5229%u7075%u6D3B%u914D%u7F6E%u6DF7%u5408@%23%24519191; ASP.NET_SessionId=45qdofapdlm1hlgxapxuxhe1; st_pvi=87492384111747; st_sp=2020-08-16%2000%3A05%3A17; st_inirUrl=http%3A%2F%2Ffund.eastmoney.com%2Fdata%2Ffundranking.html; st_sn=12; st_psi=2020081622103685-0-6169905557"
,
"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.125 Safari/537.36"}


def fetch_fund_holdings(code, season, head):
    """爬取单个基金的持仓数据和简称，并解析"""
    url = "http://fundf10.eastmoney.com/FundArchivesDatas.aspx?type=jjcc&code={}&topline=10&year=&month=&rt=0.5032668912422176".format(code)
    fund_name = '简称缺失'
    
    try:
        response = requests.get(url, headers=head, timeout=10)
        text = response.text
        
        # --- 核心修正：基金简称提取逻辑 ---
        
        # 1. 尝试从 content 变量的头部提取，这是最稳定的来源
        content_match = re.search(r'content:\\"(.*?)\s+\d{4}年\d季度股票投资明细', text)
        if content_match:
            fund_name = content_match.group(1).strip()
        else:
            # 2. 如果方法1失败，尝试从 name:'...' 模式提取 (备份方法)
            name_match = re.search(r'name:\'(.*?)\'', text)
            if name_match:
                 fund_name = name_match.group(1).strip()

        # ----------------------------

        # 提取 HTML 内容块
        div_match = re.findall('content:\\"(.*)\\",arryear', text)
        if not div_match:
            return code, fund_name, []

        div = div_match[0]
        html_body = '<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><title>test</title></head><body>%s</body></html>' % (div)
        html = etree.HTML(html_body)
        
        # 使用 season 变量定位最新的季度表格
        xpath_base = f'//div[{season}]/div/table/tbody/tr'
        rows = html.xpath(xpath_base)
        
        stock_one_fund = []
        for row in rows:
            # 股票名称位于 td[3]/a/text()
            stock_name_list = row.xpath('./td[3]/a/text()')
            if not stock_name_list:
                 continue
            
            stock_name = stock_name_list[0].strip()
            
            # --- 关键修正：重新定位数据列 ---
            # 尝试通过 XPath 提取最后四个 td 元素，通常是 涨跌幅、占净值比例、持股数、持仓市值
            data_tds = row.xpath('./td[position() >= last()-3]') 
            
            money_data = []
            for td in data_tds:
                # 提取 td 内所有文本并清理
                text = "".join(td.xpath('.//text()')).strip()
                text = text.replace('---','0').replace(',','').replace('%','')
                try:
                    # 如果能成功转换成浮点数，就认为是有效数据
                    money_data.append(float(text))
                except ValueError:
                    pass # 转换失败，跳过
            
            # 确保有足够的数字：占净值比例、持股数、持仓市值
            if len(money_data) >= 3:
                # 假设所需数据（占净值比例、持股数、持仓市值）是 money_data 中的最后三个
                stock_one_fund.append([stock_name, 
                                        money_data[-3], # 占净值比例
                                        money_data[-2], # 持股数_万
                                        money_data[-1]]) # 持仓市值_万
        
        return code, fund_name, stock_one_fund

    except requests.exceptions.RequestException:
        return code, fund_name, []
    except Exception:
        # 捕获所有其他解析错误，避免中断整个并发过程
        return code, fund_name, []

# In[]:
# --- 3. 并发爬取持仓数据 ---
print(f"从 C类.txt 中读取到 {total} 支基金，开始并发爬取持仓...")
start_time = time.time()
futures = []
all_holdings = [] # 用于存储所有基金的所有持仓记录

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    # 提交所有爬取任务到线程池
    for code in c_class_codes:
        future = executor.submit(fetch_fund_holdings, code, season, head)
        futures.append(future)

    # 监控并收集结果
    for i, future in enumerate(as_completed(futures)):
        code, fund_name, holdings_list = future.result()
        
        # 更新进度
        if total > 0 and (i + 1) % (total // 10 + 1) == 0:
            print(f"进度: {i+1}/{total} ({((i+1)/total)*100:.1f}%)")

        # 存储所有持仓数据
        for holding in holdings_list:
            # 扩展持仓记录：[基金代码, 基金简称, 股票简称, 占净值比例, 持股数_万, 持仓市值_万]
            all_holdings.append([code, fund_name] + holding)


end_time = time.time()
print("\n" + "=" * 30 + " 并发获取基金持仓数据完成 " + "=" * 30)
print(f"总耗时: {end_time - start_time:.2f} 秒")

# In[]:
# --- 4. 整合结果并保存 ---

# 创建包含所有持仓记录的 DataFrame
df_holdings = pd.DataFrame(
    all_holdings,
    columns=['基金代码', '基金简称', '股票简称', '占净值比例', '持股数_万', '持仓市值_万']
)

# 添加时间戳并保存文件
current_time_shanghai = datetime.now()
timestamp = current_time_shanghai.strftime("%Y%m%d_%H%M%S")
year_month = current_time_shanghai.strftime("%Y%m")
output_dir = os.path.join(year_month)
os.makedirs(output_dir, exist_ok=True)

filename = os.path.join(output_dir, f"C类基金持仓明细_{timestamp}.csv")
df_holdings.to_csv(filename, encoding="utf_8_sig", index=False)
print(f"所有 C 类基金的持仓明细已保存至：{filename}")
