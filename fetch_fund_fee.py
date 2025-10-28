#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
fetch_fund_fee.py - 优化版：强制使用脚本所在目录 + 详细日志 + 鲁棒的 HTML 解析
"""

import os
import sys
import time
from pathlib import Path
from typing import List, Dict

import requests
from bs4 import BeautifulSoup
import pandas as pd

# ================== 强制使用脚本所在目录 ==================
SCRIPT_DIR = Path(__file__).parent.resolve()
os.chdir(SCRIPT_DIR)  # 关键！强制切换工作目录
print(f"[INFO] 工作目录已切换到: {SCRIPT_DIR}")

TXT_FILE = SCRIPT_DIR / "C类.txt"
RESULT_CSV = SCRIPT_DIR / "fund_fee_result.csv"

# ================== 配置 ==================
# 目标 URL 保持为费率页，因为费率信息最全
BASE_URL = "https://fundf10.eastmoney.com/jjfl_{code}.html" 
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}
TIMEOUT = 15
RETRY = 3
MAX_FUNDS = 0  # 调试用，0 = 全部
# =========================================


def read_codes() -> List[str]:
    if not TXT_FILE.exists():
        print(f"[ERROR] 未找到 {TXT_FILE}")
        sys.exit(1)
    codes = [
        line.strip() for line in TXT_FILE.read_text(encoding="utf-8").splitlines()
        if line.strip().isdigit()
    ]
    if MAX_FUNDS > 0:
        codes = codes[:MAX_FUNDS]
        print(f"[DEBUG] 限制抓取前 {MAX_FUNDS} 只: {codes}")
    return codes


def fetch_page(code: str) -> str:
    url = BASE_URL.format(code=code)
    for i in range(RETRY):
        try:
            print(f"[HTTP] GET {url}")
            resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if resp.status_code == 200:
                print(f"[OK] {code} 页面获取成功 ({len(resp.text)} 字符)")
                return resp.text
            else:
                print(f"[WARN] {code} 页面返回状态码 {resp.status_code}")
        except Exception as e:
            print(f"[ERROR] 抓取 {code} 失败 (重试 {i+1}/{RETRY}): {e}")
        time.sleep(2)
    return ""


def parse_fee(html: str, code: str) -> Dict[str, str]:
    soup = BeautifulSoup(html, "lxml")

    # 1. 初始化结果字典
    info = {
        "基金代码": code,
        "基金名称": "",
        "申购状态": "",
        "赎回状态": "",
        "定投状态": "",
        "申购起点": "",
        "定投起点": "",
        "日累计申购限额": "",
        "管理费率": "",
        "托管费率": "",
        "销售服务费率": "",
        "赎回费率": "",
        "页面链接": BASE_URL.format(code=code),
    }

    # 2. 基金名称
    title_tag = soup.find("title")
    if title_tag:
        # 提取标题中 "基金费率" 前的部分作为名称
        name_part = title_tag.text.split("基金费率")[0].strip()
        info["基金名称"] = name_part.split('(')[0].strip()
        
    # 3. 申购/赎回状态 (位于页面顶部的基本信息区)
    status_div = soup.find('div', class_='bs_jz')
    if status_div:
        status_row = status_div.find('p', class_='row')
        if status_row:
            spans = status_row.find_all('span')
            if len(spans) >= 2:
                # 假设第一个 span 是申购状态，第二个是赎回状态
                info["申购状态"] = spans[0].text.strip()
                info["赎回状态"] = spans[1].text.strip()
            
            # 尝试从文本中直接提取定投状态 (通常与申购状态一致，但此处尝试更精确)
            if "定投" in info["申购状态"] or "定投" in info["赎回状态"]:
                info["定投状态"] = "开放定投"
            elif "开放申购" in info["申购状态"] or "开放赎回" in info["赎回状态"]:
                 info["定投状态"] = "开放定投"
            
            # 如果能找到定投的按钮/链接，则确认定投状态
            if soup.find('a', href=lambda href: href and 'Investment/add' in href):
                 info["定投状态"] = "开放定投"
            elif not info["定投状态"] and "暂停申购" in info["申购状态"]:
                info["定投状态"] = "暂停定投"


    # 4. 费率（管理费、托管费、服务费） - 位于第一个表格
    fee_table = soup.find('table', class_='w790 comm js-fundfl')
    if fee_table:
        rows = fee_table.find_all('tr')
        for row in rows:
            tds = row.find_all('td')
            if len(tds) >= 2:
                fee_type = tds[0].text.strip()
                fee_rate = tds[1].text.strip()
                
                if "管理费" in fee_type:
                    info["管理费率"] = fee_rate
                elif "托管费" in fee_type:
                    info["托管费率"] = fee_rate
                elif "销售服务费" in fee_type:
                    info["销售服务费率"] = fee_rate


    # 5. 申购/定投起点及限额 - 位于购买信息表格 (第二个表格)
    purchase_table = soup.find('div', class_='boxitem w790').find_next('table') # 尝试查找第二个大表格
    if purchase_table:
        rows = purchase_table.find_all('tr')
        for row in rows:
            tds = row.find_all('td')
            if len(tds) >= 2:
                key = tds[0].text.strip()
                value = tds[1].text.strip()
                
                if "申购起点" in key:
                    info["申购起点"] = value
                elif "定投起点" in key:
                    info["定投起点"] = value
                elif "日累计申购限额" in key:
                    info["日累计申购限额"] = value
                    
    
    # 6. 赎回费率 - 位于第二个表格或单独的表格
    # 尝试再次查找 redemption fee table (通常在第二个大表格之后)
    redemption_table = soup.find('div', class_='boxitem w790').find_next('table').find_next('table') 
    redeem_list = []
    if redemption_table:
        rows = redemption_table.find_all('tr')
        # 跳过表头行
        for row in rows[1:]:
            tds = row.find_all('td')
            if len(tds) >= 3:
                holding_period = tds[0].text.strip().replace('\r\n', ' ')
                rate = tds[1].text.strip()
                redeem_list.append(f"{holding_period}: {rate}")

    info["赎回费率"] = " | ".join(redeem_list) if redeem_list else "0.00%"


    return info


def main():
    print(f"[START] 开始执行，时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    codes = read_codes()
    if not codes:
        print("[INFO] 无基金代码，退出")
        return

    results = []
    for idx, code in enumerate(codes, 1):
        print(f"\n[{idx}/{len(codes)}] 正在处理 {code} ...")
        html = fetch_page(code)
        if not html:
            print(f"[SKIP] {code} 获取失败")
            continue
        
        try:
            info = parse_fee(html, code)
            results.append(info)
            print(f"[OK] {code} 解析完成")
        except Exception as e:
            print(f"[FATAL] {code} 解析过程中发生严重错误: {e}")
            continue
            
        time.sleep(1)

    if not results:
        print("[WARN] 没有成功解析任何基金")
        return

    # 写入 CSV
    df = pd.DataFrame(results)
    columns = [
        "基金代码", "基金名称", "申购状态", "赎回状态", "定投状态",
        "申购起点", "定投起点", "日累计申购限额",
        "管理费率", "托管费率", "销售服务费率",
        "赎回费率", "页面链接"
    ]
    df = df[columns]

    try:
        # 确保目录存在（虽然是根目录，但保险）
        RESULT_CSV.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(RESULT_CSV, index=False, encoding="utf-8-sig")
        print(f"\n[SUCCESS] CSV 已写入: {RESULT_CSV.resolve()}")
        print(f"   → 共 {len(results)} 条记录")
    except Exception as e:
        print(f"[FATAL] 写入 CSV 失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
