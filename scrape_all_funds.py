# -*- coding: utf-8 -*-
import os
import requests
from bs4 import BeautifulSoup
import csv
import datetime
import pytz
import random
import time
from concurrent.futures import ThreadPoolExecutor
import re
import json # 引入 json 库用于存储结构化数据

# subprocess 已移除，因为不再进行 Git 操作
# ==================== 配置 ====================
shanghai_tz = pytz.timezone('Asia/Shanghai')
now = datetime.datetime.now(shanghai_tz)
date_only_str = now.strftime('%Y%m%d')
month_dir = now.strftime('%Y%m') # 结果保存的年月目录
timestamp = now.strftime('%Y%m%d_%H%M%S')

fund_data_dir = 'fund_data'
output_base_dir = month_dir
os.makedirs(output_base_dir, exist_ok=True)

# 防反爬请求头
HEADERS = {
    'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                   'AppleWebKit/537.36 (KHTML, like Gecko) '
                   'Chrome/128.0.0.0 Safari/537.36')
}
# ==============================================

def scrape_and_parse_fund(fund_code):
    """抓取单只基金的概况 + 费率 + 基金经理信息（包括历史任职），返回 dict"""
    # 随机延时防封
    time.sleep(random.uniform(0.15, 0.6))

    result = {'基金代码': fund_code, '状态': '成功'}

    # ---------- 1. 基本概况 (jbgk) ----------
    # (此部分代码保持不变，与您上次提供的版本一致)
    jbgk_url = f"https://fundf10.eastmoney.com/jbgk_{fund_code}.html"
    try:
        r = requests.get(jbgk_url, headers=HEADERS, timeout=12)
        if r.status_code != 200:
            result['状态_概况'] = f"抓取失败: 概况页 {r.status_code}"
        else:
            soup = BeautifulSoup(r.text, 'html.parser')
            tbl = soup.find('table', class_='info w790')
            if tbl:
                for row in tbl.find_all('tr'):
                    cols = row.find_all(['th', 'td'])
                    if len(cols) >= 2:
                        k = cols[0].get_text(strip=True).rstrip('：:')
                        v = cols[1].get_text(strip=True)
                        result[k] = v
                    if len(cols) == 4:
                        k2 = cols[2].get_text(strip=True).rstrip('：:')
                        v2 = cols[3].get_text(strip=True)
                        result[k2] = v2
            else:
                result['状态_概况'] = "抓取警告: 未找到概况表格"
    except requests.RequestException as e:
        result['状态_概况'] = f"抓取失败: 网络错误 ({e})"


    # ---------- 2. 费率 (jjfl) ----------
    # (此部分代码保持不变，与您上次提供的版本一致)
    fee_url = f"https://fundf10.eastmoney.com/jjfl_{fund_code}.html"
    try:
        r = requests.get(fee_url, headers=HEADERS, timeout=12)
        if r.status_code != 200:
            result['状态_费率'] = f"抓取失败: 费率页 {r.status_code}"
        else:
            soup = BeautifulSoup(r.text, 'html.parser')

            # a) 运作费用（管理/托管/销售服务）
            h4 = soup.find('h4', string=lambda t: t and '运作费用' in t)
            if h4:
                tbl = h4.find_next('table', class_='comm jjfl')
                if tbl:
                    tds = tbl.find_all(['td', 'th'])
                    if len(tds) == 6:
                        for i in range(0, 6, 2):
                            k = tds[i].get_text(strip=True).replace('费率', '')
                            v = tds[i+1].get_text(strip=True)
                            result[k] = v

            # b) 赎回费率（多写法统一键名：赎回费率_7D0）
            h4 = soup.find('h4', string=lambda t: t and '赎回费率' in t)
            if h4:
                tbl = h4.find_next('table', class_='comm jjfl')
                if tbl and tbl.find('tbody'):
                    for row in tbl.find('tbody').find_all('tr'):
                        cols = row.find_all(['td', 'th'])
                        if len(cols) < 3:
                            continue
                        period = cols[1].get_text(strip=True)
                        rate   = cols[2].get_text(strip=True)

                        # 【增强逻辑】：统一键名：大于等于7天 → 赎回费率_7D0
                        if any(p in period.replace(' ', '') for p in
                               ['大于等于7天', '7天及以上', '≥7天', '7天以上', '7天-1年', '7天＜持有期限＜1年']):
                            result['赎回费率_7D0'] = rate
                        else:
                            result[f"赎回费率_{period.replace(' ', '')}"] = rate
                else:
                    result['状态_费率'] = "抓取警告: 未找到赎回费率表格"
    except requests.RequestException as e:
        result['状态_费率'] = f"抓取失败: 网络错误 ({e})"


    # ---------- 3. 基金经理信息 (jjjl) ----------
    manager_url = f"https://fundf10.eastmoney.com/jjjl_{fund_code}.html"
    try:
        r = requests.get(manager_url, headers=HEADERS, timeout=12)
        if r.status_code != 200:
            result['状态_经理'] = f"抓取失败: 经理页 {r.status_code}"
        else:
            soup = BeautifulSoup(r.text, 'html.parser')

            # --- 3.1 基金经理变动一览（历史任职信息）---
            manager_history = []
            history_table = soup.find('table', class_='w782 comm jloff')
            
            if history_table and history_table.find('tbody'):
                rows = history_table.find('tbody').find_all('tr')
                for i, row in enumerate(rows):
                    cols = row.find_all('td')
                    if len(cols) >= 5:
                        start_date = cols[0].get_text(strip=True)
                        end_date = cols[1].get_text(strip=True)
                        managers = cols[2].get_text(strip=True).replace('\xa0', ' ')
                        duration = cols[3].get_text(strip=True)
                        return_rate = cols[4].get_text(strip=True)
                        
                        record = {
                            '起始期': start_date,
                            '截止期': end_date,
                            '基金经理': managers,
                            '任职期间': duration,
                            '任职回报': return_rate
                        }
                        manager_history.append(record)
                        
                        # 特别提取：如果这是第一行（即现任经理的记录）
                        if i == 0:
                            result['现任经理任职回报'] = return_rate
                            result['现任经理任职期间'] = duration

            # 将历史记录存储为 JSON 字符串
            result['基金经理历史任职'] = json.dumps(manager_history, ensure_ascii=False)
            
            
            # --- 3.2 现任基金经理简介 ---
            manager_intro_box = soup.find('div', class_='jl_intro')
            
            if manager_intro_box:
                # 提取姓名
                name_tag = manager_intro_box.find('p').find('a')
                result['基金经理姓名'] = name_tag.get_text(strip=True) if name_tag else ''

                # 提取上任日期
                date_tag = manager_intro_box.find('p', string=lambda t: t and '上任日期' in t)
                if date_tag:
                    match = re.search(r'上任日期[:：]\s*(\d{4}-\d{2}-\d{2})', date_tag.get_text(strip=True))
                    result['基金经理上任日期'] = match.group(1) if match else ''

                # 提取简介
                text_div = manager_intro_box.find('div', class_='text')
                if text_div:
                    paragraphs = text_div.find_all('p')
                    
                    manager_intro = []
                    for text in [p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)]:
                        if text.startswith('姓名') or text.startswith('上任日期'):
                            continue
                        if text.startswith('查看更多基金经理详情'):
                            break
                        manager_intro.append(text)
                            
                    result['基金经理简介'] = '\n'.join(manager_intro).strip()
            
            elif not manager_history:
                 # 如果没有简介和历史记录，则给出警告
                result['状态_经理'] = result.get('状态_经理', '') + "抓取警告: 未找到基金经理信息。"


    except requests.RequestException as e:
        result['状态_经理'] = f"抓取失败: 经理页网络错误 ({e})"


    # 统一状态
    if any('失败' in result.get(k, '') for k in ('状态_概况', '状态_费率', '状态_经理')):
        result['状态'] = '失败'
    elif any('警告' in result.get(k, '') for k in ('状态_概况', '状态_费率', '状态_经理')):
        result['状态'] = '警告'
    return result

# ==================== 主逻辑 ====================
fund_codes = []
# 确保 fund_data_dir 存在
if os.path.isdir(fund_data_dir):
    for fn in os.listdir(fund_data_dir):
        if fn.endswith('.csv') and re.match(r'^\d+\.csv$', fn):
            fund_codes.append(fn.split('.')[0])
else:
    print(f"[{now.strftime('%H:%M:%S')}] 错误：未找到基金代码目录: {fund_data_dir}")
    fund_codes = []


print(f"[{now.strftime('%H:%M:%S')}] 共 {len(fund_codes)} 只基金，准备并行抓取...")
MAX_WORKERS = 40
all_data = []
if fund_codes:
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        all_data = list(pool.map(scrape_and_parse_fund, fund_codes))

print(f"[{datetime.datetime.now(shanghai_tz).strftime('%H:%M:%S')}] 抓取完毕")

# ---------- 1. 合并全量 CSV ----------
all_keys = set()
for d in all_data:
    all_keys.update(d.keys())

# 定义键的优先级顺序
preferred_keys = [
    '基金代码', '状态', 
    '基金经理姓名', '基金经理上任日期', 
    '现任经理任职期间', '现任经理任职回报', 
    '基金经理简介', '基金经理历史任职'
]
fee_keys = sorted({k for k in all_keys if '费' in k or '费率' in k})

# 最终键列表：优先键 + 费率键 + 其他键
final_keys_set = set(preferred_keys) | set(fee_keys)
other_keys = sorted({k for k in all_keys if k not in final_keys_set and not k.startswith('状态_')}) # 忽略临时的状态键
final_keys = [k for k in preferred_keys if k in all_keys] + fee_keys + other_keys

out_path = os.path.join(output_base_dir, f"basic_info_and_fees_all_funds_{timestamp}.csv")

try:
    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=final_keys)
        w.writeheader()
        for d in all_data:
            # 过滤掉内部的状态键，并且确保数据字典只包含 final_keys 中的键
            row_data = {k: d.get(k, '') for k in final_keys}
            w.writerow(row_data)
    print(f"[{datetime.datetime.now(shanghai_tz).strftime('%H:%M:%S')}] 全量文件已保存 → {out_path}")
except Exception as e:
    print(f"[{datetime.datetime.now(shanghai_tz).strftime('%H:%M:%S')}] 写入全量文件失败: {e}")

print(f"[{datetime.datetime.now(shanghai_tz).strftime('%H:%M:%S')}] 全部完成")
