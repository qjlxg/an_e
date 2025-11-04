# In[]:
# --- 使用 concurrent.futures 加速基金持仓数据获取 ---
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# 保持 rank_codes 使用 df_picked_part 中的代码列表
rank_codes = df_picked_part['基金代码'].values.tolist()
stocks_array = []
stock_funds = []
total = len(rank_codes)
fund_names_map = {}
# 设置线程数。可以根据网络和目标网站的承受能力调整。
# 通常设置为 10 到 30 比较安全。
MAX_WORKERS = 20 

head = {
"Cookie": "EMFUND1=null; EMFUND2=null; EMFUND3=null; EMFUND4=null; EMFUND5=null; EMFUND6=null; EMFUND7=null; EMFUND8=null; EMFUND0=null; st_si=44023331838789; st_asi=delete; EMFUND9=08-16 22:04:25@#$%u4E07%u5BB6%u65B0%u5229%u7075%u6D3B%u914D%u7F6E%u6DF7%u5408@%23%24519191; ASP.NET_SessionId=45qdofapdlm1hlgxapxuxhe1; st_pvi=87492384111747; st_sp=2020-08-16%2000%3A05%3A17; st_inirUrl=http%3A%2F%2Ffund.eastmoney.com%2Fdata%2Ffundranking.html; st_sn=12; st_psi=2020081622103685-0-6169905557"
,
"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.125 Safari/537.36"}

def fetch_fund_holdings(code, season, head):
    """单独的爬取函数，用于线程池"""
    url = "http://fundf10.eastmoney.com/FundArchivesDatas.aspx?type=jjcc&code={}&topline=10&year=&month=&rt=0.5032668912422176".format(code)
    try:
        response = requests.get(url, headers=head, timeout=10) # 增加超时设置
        text = response.text
        
        # 提取基金简称
        fund_name_match = re.search(r'name:\'(.*?)\'', text)
        fund_name = fund_name_match.group(1) if fund_name_match else '简称缺失'

        div_match = re.findall('content:\\"(.*)\\",arryear', text)
        if not div_match:
            return code, fund_name, [] # 返回空持仓

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
                    # 确保索引不越界
                    if len(stock_money_text) > 3*count + 2:
                        stock_one_fund.append([stock_info[i+1].text,
                                                stock_money_text[3*count+0],
                                                stock_money_text[3*count+1],
                                                stock_money_text[3*count+2]])
        
        return code, fund_name, stock_one_fund

    except requests.exceptions.RequestException as e:
        # print(f"爬取基金 {code} 失败: {e}")
        return code, '爬取失败', []
    except Exception as e:
        # print(f"处理基金 {code} 数据失败: {e}")
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
# 注意：这里假设 df_part 变量仍然在作用域内，它是在上一个 In[]: 块中定义的。
df_part.loc[:, '基金简称'] = df_part['基金代码'].map(fund_names_map)


tmp = pd.DataFrame(stock_funds,columns=['基金代码','十大重仓'])
# 使用 df_picked_part（即 df_part）进行合并
df_funds_info_extend = pd.merge(df_picked_part,tmp,how='inner',on='基金代码')
df_funds_info_extend.set_index('基金代码', inplace=True)
df_funds_info_extend.to_csv("./基金持仓_C类基金.csv", encoding="utf_8_sig")
