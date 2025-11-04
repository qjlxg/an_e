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
    with open('C类.txt', 'r', encoding='utf-8') as f:
        all_lines = [line.strip() for line in f if line.strip()]
    
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

# --- 2. 参数设置 ---
season = 1 
MAX_WORKERS = 20 
total = len(c_class_codes)

# 爬虫 headers
head = {
    "Cookie": "EMFUND1=null; EMFUND2=null; EMFUND3=null; EMFUND4=null; EMFUND5=null; EMFUND6=null; EMFUND7=null; EMFUND8=null; EMFUND0=null; st_si=44023331838789; st_asi=delete; EMFUND9=08-16 22:04:25@#$%u4E07%u5BB6%u65B0%u5229%u7075%u6DFB%u5408@%23%24519191; ASP.NET_SessionId=45qdofapdlm1hlgxapxuxhe1; st_pvi=87492384111747; st_sp=2020-08-16%2000%3A05%3A17; st_inirUrl=http%3A%2F%2Ffund.eastmoney.com%2Fdata%2Ffundranking.html; st_sn=12; st_psi=2020081622103685-0-6169905557",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.125 Safari/537.36"
}


def fetch_fund_holdings(code, season, head):
    """爬取单个基金的持仓数据和简称，并解析"""
    url = f"http://fundf10.eastmoney.com/FundArchivesDatas.aspx?type=jjcc&code={code}&topline=10&year=&month=&rt=0.5032668912422176"
    fund_name = '简称缺失'
    
    try:
        response = requests.get(url, headers=head, timeout=10)
        text = response.text
        
        # 1. 提取 HTML 内容块 (包含基金简称和持仓表格)
        div_match = re.findall('content:\\"(.*)\\",arryear', text)
        if not div_match:
            # 备用简称提取：从 name:'...' 模式提取
            name_match = re.search(r"name:'(.*?)'", text)
            if name_match:
                fund_name = name_match.group(1).strip()
            return code, fund_name, []

        div = div_match[0]
        html_body = '<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><title>test</title></head><body>%s</body></html>' % (div)
        html = etree.HTML(html_body)
        
        # --- 最终简称提取：使用 XPath 定位 h4 标签内的链接文本 ---
        # XPath: //h4/label[@class='left']/a[1] 
        name_list = html.xpath('//h4/label/a[1]/text()')
        if name_list:
            fund_name = name_list[0].strip()
        # -----------------------------------------------------
        
        # 2. 提取持仓数据
        xpath_base = f'//div[{season}]/div/table/tbody/tr'
        rows = html.xpath(xpath_base)
        
        stock_one_fund = []
        for row in rows:
            # 股票名称位于 td[3]/a/text()
            stock_name_list = row.xpath('./td[3]/a/text()')
            if not stock_name_list:
                continue
            
            stock_name = stock_name_list[0].strip()
            
            # --- 股票数据提取逻辑 (已验证稳定) ---
            data_tds = row.xpath('./td[position() >= last()-3]') 
            
            money_data = []
            for td in data_tds:
                text = "".join(td.xpath('.//text()')).strip()
                text = text.replace('---','0').replace(',','').replace('%','')
                try:
                    money_data.append(float(text))
                except ValueError:
                    pass
            
            # 确保有足够的数字：占净值比例、持股数、持仓市值
            if len(money_data) >= 3:
                stock_one_fund.append([stock_name, 
                                       money_data[-3], # 占净值比例
                                       money_data[-2], # 持股数_万
                                       money_data[-1]]) # 持仓市值_万
        
        return code, fund_name, stock_one_fund

    except requests.exceptions.RequestException:
        return code, fund_name, []
    except Exception:
        # 捕获所有其他解析错误
        return code, fund_name, []

# --- 3. 并发爬取持仓数据 ---
print(f"从 C类.txt 中读取到 {total} 支基金，开始并发爬取持仓...")
start_time = time.time()
futures = []
all_holdings = [] 

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    for code in c_class_codes:
        future = executor.submit(fetch_fund_holdings, code, season, head)
        futures.append(future)

    for i, future in enumerate(as_completed(futures)):
        code, fund_name, holdings_list = future.result()
        
        if total > 0 and (i + 1) % (total // 10 + 1) == 0:
            print(f"进度: {i+1}/{total} ({((i+1)/total)*100:.1f}%)")
            
        # 强制打印简称，方便您在 Actions 日志中查看是否成功
        if fund_name != '简称缺失':
             print(f"基金 {code} 简称提取成功: {fund_name}")

        for holding in holdings_list:
            all_holdings.append([code, fund_name] + holding)


end_time = time.time()
print("\n" + "=" * 30 + " 并发获取基金持仓数据完成 " + "=" * 30)
print(f"总耗时: {end_time - start_time:.2f} 秒")

# --- 4. 整合结果并保存 ---

df_holdings = pd.DataFrame(
    all_holdings,
    columns=['基金代码', '基金简称', '股票简称', '占净值比例', '持股数_万', '持仓市值_万']
)

current_time_shanghai = datetime.now()
timestamp = current_time_shanghai.strftime("%Y%m%d_%H%M%S")
year_month = current_time_shanghai.strftime("%Y%m")
output_dir = os.path.join(year_month)
os.makedirs(output_dir, exist_ok=True)

filename = os.path.join(output_dir, f"C类基金持仓明细_{timestamp}.csv")
df_holdings.to_csv(filename, encoding="utf_8_sig", index=False)
print(f"所有 C 类基金的持仓明细已保存至：{filename}")
