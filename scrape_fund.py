#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Selenium 并行抓取 C类.txt（支持 JS 渲染页面）
"""

import os
import re
import csv
import time
import argparse
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from tqdm import tqdm

# ============================= 配置 =============================
CODES_FILE = "C类.txt"
CSV_PATH = "fund_data.csv"
FAILED_PATH = "failed_codes.txt"
BASE_URL = "https://fund.eastmoney.com/{code}.html"
TIMEOUT = 20
# ==================================================================

def get_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(TIMEOUT)
    return driver

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--concurrency", type=int, default=5)  # Selenium 建议 ≤5
    return parser.parse_args()

def load_codes(limit: Optional[int]) -> List[str]:
    if not os.path.exists(CODES_FILE):
        print(f"未找到 {CODES_FILE}")
        return []
    with open(CODES_FILE, "r", encoding="utf-8") as f:
        lines = [l.strip() for l in f if l.strip() and l.lower() != "code"]
    codes = [l.split()[0] for l in lines]
    if limit and limit > 0:
        print(f"调试：前 {limit} 只")
        codes = codes[:limit]
    return codes

def fetch_and_parse(code: str) -> Optional[Dict]:
    driver = None
    try:
        driver = get_driver()
        url = BASE_URL.format(code=code)
        driver.get(url)
        
        # 等待关键元素加载
        WebDriverWait(driver, TIMEOUT).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".infoOfFund td"))
        )
        time.sleep(2)  # 确保 JS 渲染

        data = {
            "date": datetime.datetime.now().strftime("%Y-%m-%d"),
            "code": code,
            "nav": "", "change_pct": "", "status_buy": "", "status_sell": "",
            "mgr_fee": "", "cust_fee": "", "sale_fee": "", "assets": "", "assets_date": ""
        }

        # 单位净值 & 涨跌幅
        try:
            nav_text = driver.find_element(By.XPATH, "//td[contains(text(),'单位净值')]/following-sibling::td").text
            m = re.search(r"([\d.]+)\s*\(\s*([+-]?\d+\.\d+)%\s*\)", nav_text)
            if m:
                data["nav"], data["change_pct"] = m.groups()
        except: pass

        # 交易状态
        try:
            status_text = driver.find_element(By.XPATH, "//td[contains(text(),'交易状态')]/following-sibling::td").text
            data["status_buy"] = "开放申购" if "开放申购" in status_text else "暂停申购"
            data["status_sell"] = "开放赎回" if "开放赎回" in status_text else "暂停赎回"
        except: pass

        # 费用
        try:
            for label in ["管理费率", "托管费率", "销售服务费率"]:
                try:
                    value = driver.find_element(By.XPATH, f"//td[contains(text(),'{label}')]/following-sibling::td").text
                    if label == "管理费率": data["mgr_fee"] = value
                    elif label == "托管费率": data["cust_fee"] = value
                    elif label == "销售服务费率": data["sale_fee"] = value
                except: continue
        except: pass

        # 资产规模
        try:
            asset_text = driver.find_element(By.XPATH, "//td[contains(text(),'资产规模')]").text
            m = re.search(r"([\d.]+)亿元.*?截止至：([\d\-]+)", asset_text)
            if m:
                data["assets"], data["assets_date"] = m.groups()
        except: pass

        return data if data["nav"] else None

    except Exception as e:
        print(f"[{code}] 异常: {e}")
        return None
    finally:
        if driver:
            driver.quit()

# ============================= 主程序 =============================
def main():
    args = parse_args()
    codes = load_codes(args.limit)
    if not codes: return

    print(f"开始抓取 {len(codes)} 只基金，最大并发: {args.concurrency}")

    results, failed = [], []
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        for code, data in tqdm(pool.map(lambda c: (c, fetch_and_parse(c)), codes), total=len(codes), desc="抓取进度"):
            if data:
                results.append(data)
            else:
                failed.append(code)

    # 写入 CSV
    if results:
        exists = os.path.isfile(CSV_PATH)
        with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=[
                "date","code","nav","change_pct","status_buy","status_sell",
                "mgr_fee","cust_fee","sale_fee","assets","assets_date"
            ])
            if not exists: w.writeheader()
            w.writerows(results)
        print(f"成功 {len(results)} 条 → {CSV_PATH}")

    if failed:
        with open(FAILED_PATH, "w", encoding="utf-8") as f:
            f.write("\n".join(failed))
        print(f"失败 {len(failed)} 只 → {FAILED_PATH}")
    else:
        print("全部成功！")

if __name__ == "__main__":
    main()