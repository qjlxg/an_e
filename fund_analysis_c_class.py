# In[]:
#!/usr/bin/env python
# coding: utf-8
# encoding=utf-8
import pandas as pd
import requests
from lxml import etree
import re
import numpy as np
import time
from datetime import datetime
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- 新增：读取C类基金代码列表 (并移除可能的头部 'code') ---
try:
    with open('C类.txt', 'r', encoding='utf-8') as f:
        # 读取所有行，去除首尾空白
        all_lines = [line.strip() for line in f if line.strip()]
    
    # 检查并移除可能的列标题 'code'
    if all_lines and all_lines[0].lower().startswith('code'):
        c_class_codes = all_lines[1:]
    else:
        c_class_codes = all_lines
        
except FileNotFoundError:
    print("错误：未找到 'C类.txt' 文件，请确保文件位于脚本运行目录下！")
    c_class_codes = []
except Exception as e:
    print(f"读取文件时发生错误: {e}")
    c_class_codes = []

if not c_class_codes:
    print("基金代码列表为空，脚本将提前退出。")
    import sys
    sys.exit(1) # 发现列表为空时退出

# In[]:
# --- 参数设置 (为保证脚本连续性，PyQt5 交互部分已被注释或假设为硬编码) ---
# 假设 season 参数为硬编码的 1
season = 1

# In[]:
# --- 替换基金排名爬虫逻辑，使用 C 类基金代码构建初始 DataFrame ---

# 1. 创建一个只包含基金代码的 DataFrame
df = pd.DataFrame(c_class_codes, columns=["基金代码"])

# 2. 基金简称和业绩数据列，因为没有爬取排名数据，填充 NaN
df["基金简称"] = np.nan 
header_performance = ["日增长率", "近1周", "近1月", "近3月", "近半年", "近1年", "近2年", "近3年", "今年来", "成立来"]
for col in header_performance:
    df[col] = np.nan

total = df.count()[0]
print("从 C类.txt 中读取到 {} 支基金！".format(total))

df_part = df[["基金代码", "基金简称"] + header_performance]
df.to_csv("./基金增长率_C类基金.csv", encoding="utf_8_sig")

# In[]:
# --- 修复 NameError：跳过筛选，直接定义 df_picked_part ---
# 由于 df_part 中的业绩数据为 NaN，此筛选已失效。
# 我们直接使用 df_part 中的所有基金代码进行后续持仓分析。
df_picked_part = df_part # <--- 修正错误的关键行！

# In[]:
# --- 基金持仓数据获取 (使用并发加速) ---

rank_codes = df_picked_part['基金代码'].values.tolist()
stocks_array = []
stock_funds = []
total = len(rank_codes)
fund_names_map = {}
# 设置线程数
MAX_WORKERS = 20 

head = {
"Cookie": "EMFUND1=null; EMFUND2=null; EMFUND3=null; EMFUND4=null; EMFUND5=null; EMFUND6=null; EMFUND7=null; EMFUND8=null; EMFUND0=null; st_si=44023331838789; st_asi=delete; EMFUND9=08-16 22:04:25@#$%u4E07%u5BB6%u65B0%u5229%u7075%u6D3B%u914D%u7F6E%u6DF7%u5408@%23%24519191; ASP.NET_SessionId=45qdofapdlm1hlgxapxuxhe1; st_pvi=87492384111747; st_sp=2020-08-16%2000%3A05%3A17; st_inirUrl=http%3A%2F%2Ffund.eastmoney.com%2Fdata%2Ffundranking.html; st_sn=12; st_psi=2020081622103685-0-6169905557"
,
"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.125 Safari/537.36"}

def fetch_fund_holdings(code, season, head):
    """单独的爬取函数，用于线程池"""
    url = "http://fundf10.eastmoney.com/FundArchivesDatas.aspx?type=jjcc&code={}&topline=10&year=&month=&rt=0.5032668912422176".format(code)
    try:
        response = requests.get(url, headers=head, timeout=10)
        text = response.text
        
        # 提取基金简称
        fund_name_match = re.search(r'name:\'(.*?)\'', text)
        fund_name = fund_name_match.group(1) if fund_name_match else '简称缺失'

        div_match = re.findall('content:\\"(.*)\\",arryear', text)
        if not div_match:
            return code, fund_name, []

        div = div_match[0]
        html_body = '<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><title>test</title></head><body>%s</body></html>' % (div)
        html = etree.HTML(html_body)
        
        # 解析逻辑与原脚本保持一致
        stock_info = html.xpath('//div[{}]/div/table/tbody/tr/td/a'.format(season))
        stock_money = html.xpath('//div[{}]/div/table/tbody/tr/td[@class="tor"]'.format(season))
        if stock_money == []:
            stock_money = html.xpath('//div[{}]/div/table/tbody/tr/td[@class="toc"]'.format(season))
        
        stock_money_text = []
        for ii in stock_money:
            ii_text = ii.text
            if ii_text is not None:
                ii_text = ii_text.replace('---','0')
                stock_money_text.append(float(ii_text.replace(',','').replace('%','')))
        
        stock_one_fund = []
        if len(stock_info) != 0 and len(stock_money_text) != 0:
            count = -1
            for i in range(0, len(stock_info)):
                stock = stock_info[i]
                stock_text = stock.text if stock.text is not None else '缺失'
                tmp0 = stock_text.split('.')
                tmp = tmp0[0]
                
                if stock_text and (tmp.isdigit() or (tmp.isupper() and tmp.isalnum() and len(tmp0)>1)):
                    count += 1
                    if len(stock_money_text) > 3*count + 2:
                        stock_one_fund.append([stock_info[i+1].text,
                                                stock_money_text[3*count+0],
                                                stock_money_text[3*count+1],
                                                stock_money_text[3*count+2]])
        
        return code, fund_name, stock_one_fund

    except requests.exceptions.RequestException:
        return code, '爬取失败', []
    except Exception:
        return code, '处理失败', []


print("<" * 30 + "开始并发获取基金持仓数据 (最大线程数: {})".format(MAX_WORKERS) + ">" * 30)
start_time = time.time()
futures = []
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    # 提交所有爬取任务到线程池
    for code in rank_codes:
        future = executor.submit(fetch_fund_holdings, code, season, head)
        futures.append(future)

    # 监控并收集结果
    for i, future in enumerate(as_completed(futures)):
        code, fund_name, stock_one_fund = future.result()
        
        # 更新进度
        if (i + 1) % (total // 10 + 1) == 0:
            print(f"<" * 30 + "进度: {i+1}/{total} ({((i+1)/total)*100:.1f}%)" + ">" * 30)

        fund_names_map[code] = fund_name
        stock_funds.append([code, stock_one_fund])
        stocks_array.extend(stock_one_fund)


end_time = time.time()
print("<" * 30 + "并发获取基金持仓数据完成!!!"+ ">" * 30)
print(f"总耗时: {end_time - start_time:.2f} 秒")


# 更新 df_part 中的基金简称
df_part.loc[:, '基金简称'] = df_part['基金代码'].map(fund_names_map)

tmp = pd.DataFrame(stock_funds,columns=['基金代码','十大重仓'])
df_funds_info_extend = pd.merge(df_picked_part,tmp,how='inner',on='基金代码')
df_funds_info_extend.set_index('基金代码', inplace=True)
df_funds_info_extend.to_csv("./基金持仓_C类基金.csv", encoding="utf_8_sig")


# In[]:
# --- 股票被持有信息提取 (保持不变) ---
stock_info_list = []
for row in df_funds_info_extend.iterrows():
    tenpos = row[1]['十大重仓']
    fund_jc = row[1]['基金简称']
    if tenpos and len(tenpos) != 0:
        # Note: 仅取第一重仓股 [0]
        # 确保 tenpos 至少有 4 个元素
        if len(tenpos[0]) >= 4:
            tmp = [tenpos[0][0],fund_jc,tenpos[0][1],tenpos[0][2],tenpos[0][3]]
            stock_info_list.append(tmp)
df_stock_info = pd.DataFrame(stock_info_list,columns=['股票简称','所属基金','占净值比例','持股数_万','持仓市值_万'])
df_stock_info.to_csv("./股票被持有信息_C类基金.csv", encoding="utf_8_sig")

# In[]
# --- 股票被持有信息统计 (保持不变) ---
df_stock_info_cp = df_stock_info
df_stock_info_cp['所属基金cp'] = df_stock_info['所属基金']
df_stock_info_gb = df_stock_info_cp.groupby('股票简称')
stock_agg_result = df_stock_info_gb.agg({'持股数_万':np.sum,'持仓市值_万':np.sum,'占净值比例':np.mean,'所属基金':len,'所属基金cp':list})
stock_agg_result.columns = ['被持股数_万','被持仓市值_万','平均占比','所属基金数目','所属基金集合']
stock_agg_result.to_csv("./股票被持有信息统计_C类基金.csv", encoding="utf_8_sig")

# In[]
# --- 基金持受欢迎股数目统计 (保持不变) ---
rank = 10
stock_agg_result = stock_agg_result.sort_values(by="所属基金数目",ascending=False)
stock_agg_result_head0 = stock_agg_result.head(rank)
stock_agg_result = stock_agg_result.sort_values(by="被持仓市值_万",ascending=False)
stock_agg_result_head1 = stock_agg_result.head(rank)
stock_agg_result = stock_agg_result.sort_values(by="平均占比",ascending=False)
stock_agg_result_head2 = stock_agg_result.head(rank)

funds_stocks_count = []
for st_funds_ in stock_funds:
    st_funds = st_funds_[1]
    tmp = [i[0] for i in st_funds]
    df_stock_funds = pd.DataFrame(tmp,columns=['股票简称'])
    
    count0 = pd.merge(stock_agg_result_head0.reset_index(),df_stock_funds,how='inner',on='股票简称').shape[0]
    count1 = pd.merge(stock_agg_result_head1.reset_index(),df_stock_funds,how='inner',on='股票简称').shape[0]
    count2 = pd.merge(stock_agg_result_head2.reset_index(),df_stock_funds,how='inner',on='股票简称').shape[0]

    code = st_funds_[0]
    jc_tmp = fund_names_map.get(code, '简称缺失')

    funds_stocks_count.append([jc_tmp,count0,count1,count2])

df_funds_stock_count = pd.DataFrame(funds_stocks_count,columns = ['基金简称','优仓数目_所属基金数','优仓数目_被持仓市值','平均占比'])

df_funds_stock_count = pd.merge(df_funds_stock_count,df_part,how='inner',on='基金简称')
df_funds_stock_count = df_funds_stock_count.sort_values(by=["优仓数目_所属基金数"], ascending=False, axis=0)

# 添加时间戳并保存文件
current_time_shanghai = datetime.now()
timestamp = current_time_shanghai.strftime("%Y%m%d_%H%M%S")
year_month = current_time_shanghai.strftime("%Y%m")
output_dir = os.path.join(year_month)
os.makedirs(output_dir, exist_ok=True)

filename = os.path.join(output_dir, f"基金持受欢迎股数目统计_C类基金_{timestamp}.csv")
df_funds_stock_count.to_csv(filename, encoding="utf_8_sig")
print(f"结果已保存至：{filename}")
