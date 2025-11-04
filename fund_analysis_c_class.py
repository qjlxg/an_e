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

# --- 新增：读取C类基金代码列表 ---
try:
    # 假设 C类.txt 与脚本在同一目录下
    with open('C类.txt', 'r', encoding='utf-8') as f:
        # 读取每一行，去除首尾空白，并过滤掉空行
        c_class_codes = [line.strip() for line in f if line.strip()]
except FileNotFoundError:
    print("错误：未找到 'C类.txt' 文件，请确保文件位于脚本运行目录下！")
    c_class_codes = [] # 如果文件不存在，则代码列表为空
except Exception as e:
    print(f"读取文件时发生错误: {e}")
    c_class_codes = []

if not c_class_codes:
    print("基金代码列表为空，脚本将提前退出。")
    # sys.exit() # 实际运行时可取消注释

# In[]: 
# 参数部分保持不变，但部分参数（如 sample, sc, st, ft, dx）将因不再爬取排名数据而失效
sample = '150000'#样本数量
sc = '6yzf'#排序键值
st = 'desc'#排序方式
ft = 'gp'#基金类型
dx = '1'#是否可购
season = 1#季度选择


r1r = 1#日增长率
r1z = 1#近1周
r1y = 1#近1月
r3y = 0.3333#近3月
r6y = 0.3333#近6月
r1n = 0.25#近1年
r2n = 0.25#近2年
r3n = 0.25#近3年
rjnl = 0.25#今年来
rcll = 1#成立来


sd = '2021-01-07'
ed = '2021-02-07'

# In[] 在参数文书写单元后加上这么一段就可以了
# 保留用户交互界面，以便修改 season 等其他参数
from PyQt5.QtWidgets import QDialog
import sys
from PyQt5.QtWidgets import QApplication
# 假设 dialog 模块存在
# import dialog 
# class TestDialog1(QDialog,dialog.Ui_XMtool):
#     def __init__(self,parent=None):
#         super(TestDialog1,self).__init__(parent)
#         self.setupUi(self)

# app=QApplication(sys.argv)
# dlg=TestDialog1()
# dlg.show()
# app.exec_()

# # -------------------------------------------
# # 由于没有 dialog.py 无法运行，此处将使用硬编码参数
# # 实际运行时，请确保 PyQt5 环境和 dialog.py 存在
# # -------------------------------------------
# # 模拟从对话框读取参数
# # sample = dlg.sample.text() #样本数量
# # sc = dlg.sc.currentText() #排序键值
# # st = dlg.st.currentText() #排序方式
# # ft = dlg.ft.currentText() #基金类型
# # dx = dlg.dx.currentText() #是否可购
# # season = int(dlg.season.currentText()) #季度选择

# # r1r = float(dlg.r1r.text()) #日增长率
# # # ... 其他 rates 参数 ...
# # -------------------------------------------


# In[]:
# --- 移除基金排名爬虫逻辑，替换为 C 类基金代码构建初始 DataFrame ---

# 创建一个只包含基金代码的 DataFrame
df = pd.DataFrame(c_class_codes, columns=["基金代码"])

# 基金简称需要通过另一个接口获取，此处暂时为空，后续在爬取持仓时获取
df["基金简称"] = np.nan 

# 业绩数据列，因为没有爬取排名数据，这些列将填充 NaN 或 0
header_performance = ["日增长率", "近1周", "近1月", "近3月", "近半年", "近1年", "近2年", "近3年", "今年来", "成立来"]
for col in header_performance:
    df[col] = np.nan

total = df.count()[0]
print("从 C类.txt 中读取到 {} 支基金！".format(total))

df_part = df[["基金代码", "基金简称"] + header_performance] # 挑选部分感兴趣的条目
# 初始 df_part 保存到 CSV，此时业绩列为空
df.to_csv("./基金增长率_C类基金.csv", encoding="utf_8_sig")

# In[]:
# --- 4433 法则筛选逻辑 (现在将失效并被注释) ---

# # 由于 df_part 中的业绩数据为 NaN，此筛选将不产生任何结果。
# # 如果需要此筛选，必须先找到一个能批量查询基金业绩的 API 或网站进行爬取。
# # 目前为了继续运行脚本的持仓分析部分，我们直接跳过筛选，使用 df_part 中的所有基金。

# # print("警告：由于缺乏基金业绩排名数据，'4433法则'筛选将失效。")
# # df_picked_part = df_part # 直接使用所有 C 类基金作为筛选结果
# # df_picked_part.to_csv("./4433法则结果.csv", encoding="utf_8_sig")

# 直接使用 df_part 进行后续操作 (跳过筛选)
df_picked_part = df_part 

# In[]:
# --- 基金持仓数据获取 (保持不变，但 rank_codes 使用 df_part) ---
rank_codes = df_picked_part['基金代码'].values.tolist() # 使用 C 类基金代码列表
stocks_array = []
stock_funds = []
total = len(rank_codes)
total_part = int(total/100)+1 #每百分之一报一次进度

# 创建一个临时字典来存储基金简称，以便后续填充 df_part
fund_names_map = {}

for index, code in enumerate(rank_codes):
    if index % total_part == 0:
        print("<" * 30 + "获得基金持仓数据中："+str(index)+"/"+str(total)+ ">" * 30)
    
    # 爬取持仓数据和基金简称
    url = "http://fundf10.eastmoney.com/FundArchivesDatas.aspx?type=jjcc&code={}&topline=10&year=&month=&rt=0.5032668912422176".format(code)
    head = {
    "Cookie": "EMFUND1=null; EMFUND2=null; EMFUND3=null; EMFUND4=null; EMFUND5=null; EMFUND6=null; EMFUND7=null; EMFUND8=null; EMFUND0=null; st_si=44023331838789; st_asi=delete; EMFUND9=08-16 22:04:25@#$%u4E07%u5BB6%u65B0%u5229%u7075%u6D3B%u914D%u7F6E%u6DF7%u5408@%23%24519191; ASP.NET_SessionId=45qdofapdlm1hlgxapxuxhe1; st_pvi=87492384111747; st_sp=2020-08-16%2000%3A05%3A17; st_inirUrl=http%3A%2F%2Ffund.eastmoney.com%2Fdata%2Ffundranking.html; st_sn=12; st_psi=2020081622103685-0-6169905557"
    ,
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.125 Safari/537.36"}
    response = requests.get(url, headers=head)
    text = response.text

    # 尝试从返回数据中提取基金简称，并记录
    fund_name_match = re.search(r'name:\'(.*?)\'', text)
    if fund_name_match:
        fund_name = fund_name_match.group(1)
        fund_names_map[code] = fund_name
    
    div_match = re.findall('content:\\"(.*)\\",arryear', text)
    if not div_match:
        continue

    div = div_match[0]
    html_body = '<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><title>test</title></head><body>%s</body></html>' % (
        div)#构造网页
    html = etree.HTML(html_body)#将传进去的字符串转变成_Element对象
    stock_info = html.xpath('//div[{}]/div/table/tbody/tr/td/a'.format(season))
    stock_money = html.xpath('//div[{}]/div/table/tbody/tr/td[@class="tor"]'.format(season))
    if stock_money == []:
        stock_money = html.xpath('//div[{}]/div/table/tbody/tr/td[@class="toc"]'.format(season))
    
    stock_money_text = []
    for ii in stock_money:
        ii_text = ii.text
        if ii_text!=None:
            ii.text = ii.text.replace('---','0')
            stock_money_text.append(float(ii.text.replace(',','').replace('%','')))
    
    stock_one_fund = []
    if len(stock_info)!=0 and len(stock_money_text)!=0:
        count = -1
        for i in range(0,len(stock_info)):
            stock = stock_info[i]
            if stock.text==None:
                stock.text = '缺失'
            tmp0 = stock.text.split('.')
            tmp = tmp0[0]
            if stock.text and (tmp.isdigit() or (tmp.isupper() and tmp.isalnum() and len(tmp0)>1)):
                count = count+1
                # 确保索引不越界
                if len(stock_money_text) > 3*count + 2:
                    stock_one_fund.append([stock_info[i+1].text,
                                            stock_money_text[3*count+0],
                                            stock_money_text[3*count+1],
                                            stock_money_text[3*count+2]])
    stock_funds.append([code,stock_one_fund])
    stocks_array.extend(stock_one_fund)

print("<" * 30 + "获得基金持仓数据中：done!!!"+ ">" * 30)

# 更新 df_part 中的基金简称
df_part['基金简称'] = df_part['基金代码'].map(fund_names_map)


tmp = pd.DataFrame(stock_funds,columns=['基金代码','十大重仓'])
# 使用 df_picked_part（即 df_part）进行合并
df_funds_info_extend = pd.merge(df_picked_part,tmp,how='inner',on='基金代码')
df_funds_info_extend.set_index('基金代码', inplace=True) # 使用 inplace=True 避免创建副本
df_funds_info_extend.to_csv("./基金持仓_C类基金.csv", encoding="utf_8_sig")


# In[]:
# --- 股票被持有信息提取 (保持不变) ---
stock_info_list = []
for row in df_funds_info_extend.iterrows():
    tenpos = row[1]['十大重仓']
    fund_jc = row[1]['基金简称']
    if len(tenpos)!=0:
        # Note: 仅取第一重仓股 [0]
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
    tmp = [i[0] for i in st_funds] # 提取十大重仓股的股票简称
    df_stock_funds = pd.DataFrame(tmp,columns=['股票简称'])
    
    # 统计基金持有的优仓股数目
    count0 = pd.merge(stock_agg_result_head0.reset_index(),df_stock_funds,how='inner',on='股票简称').shape[0] # 使用 shape[0] 替代 iloc[:,0].size
    count1 = pd.merge(stock_agg_result_head1.reset_index(),df_stock_funds,how='inner',on='股票简称').shape[0]
    count2 = pd.merge(stock_agg_result_head2.reset_index(),df_stock_funds,how='inner',on='股票简称').shape[0]

    # 获取基金简称（使用之前从爬虫结果中获取的 fund_names_map）
    code = st_funds_[0]
    jc_tmp = fund_names_map.get(code, '简称缺失') # 从 map 中获取，更安全

    funds_stocks_count.append([jc_tmp,count0,count1,count2])

df_funds_stock_count = pd.DataFrame(funds_stocks_count,columns = ['基金简称','优仓数目_所属基金数','优仓数目_被持仓市值','平均占比'])

# 重新与更新后的 df_part 合并（包含简称）
df_funds_stock_count = pd.merge(df_funds_stock_count,df_part,how='inner',on='基金简称')
df_funds_stock_count = df_funds_stock_count.sort_values(by=["优仓数目_所属基金数"], ascending=False, axis=0) # 最后排序

# 添加时间戳并保存文件
current_time_shanghai = datetime.now() # 假设系统时间接近上海时区
timestamp = current_time_shanghai.strftime("%Y%m%d_%H%M%S")
year_month = current_time_shanghai.strftime("%Y%m")
output_dir = os.path.join(year_month)
os.makedirs(output_dir, exist_ok=True) # 创建年月目录

filename = os.path.join(output_dir, f"基金持受欢迎股数目统计_C类基金_{timestamp}.csv")
df_funds_stock_count.to_csv(filename, encoding="utf_8_sig")
print(f"结果已保存至：{filename}")
