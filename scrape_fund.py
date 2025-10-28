#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
并行抓取 C类.txt 中的基金（脚本在根目录）
支持：
  --limit N        : 只抓前 N 只（调试用）
  --concurrency N  : 最大并发数（默认 10）
"""

import os
import re
import csv
import time
import argparse
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

# ============================= 配置（根目录路径）=============================
CODES_FILE = "C类.txt"        # 直接在根目录
CSV_PATH = "fund_data.csv"
FAILED_PATH = "failed_codes.txt"
BASE_URL = "https://fund.eastmoney.com/{code}.html"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}
TIMEOUT = 15
RETRY = 2
# ==================================================================

def parse_args():
    parser = argparse.ArgumentParser(description="C类基金并行抓取（根目录脚本）")
    parser.add_argument("--limit", type=int, default=None, help="只抓取前 N 只基金（调试用）")
    parser.add_argument("--concurrency", type=int, default=10, help="最大并发数")
    return parser.parse_args()

def load_codes(limit: Optional[int] = None) -> List[str]:
    if not os.path.exists(CODES_FILE):
        print(f"错误：未找到 {CODES_FILE}，当前工作目录: {os.getcwd()}")
        return []
    with open(CODES_FILE, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip() and line.lower() != "code"]
    codes = [line.split()[0] for line in lines]
    if limit is not None and limit > 0:
        print(f"调试模式：只抓取前 {limit} 只")
        codes = codes[:limit]
    else:
        print(f"共加载 {len(codes)} 只基金")
    return codes

def fetch_html(code: str) -> Optional[str]:
    url = BASE_URL.format(code=code)
    for _ in range(RETRY + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            resp.encoding = "utf-8"
            return resp.text
        except Exception as e:
            if _ == RETRY:
                print(f"[{code}] 抓取失败: {e}")
            time.sleep(1)
    return None

def parse_data(html: str, code: str) -> Optional[Dict]:
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    data = {
        "date": datetime.datetime.now().strftime("%Y-%m-%d"),
        "code": code,
        "nav": "", "change_pct": "",
        "status_buy": "", "status_sell": "",
        "mgr_fee": "", "cust_fee": "", "sale_fee": "",
        "assets": "", "assets_date": ""
    }

    # 单位净值
    nav_p = soup.find(string=re.compile(r"单位净值"))
    if nav_p:
        m = re.search(r"([\d.]+)\s*\(\s*([+-]?\d+\.\d+)%\s*\)", nav_p)
        if m:
            data["nav"] = m.group(1)
            data["change_pct"] = m.group(2)

    # 交易状态
    status_p = soup.find(string=re.compile(r"交易状态"))
    if status_p:
        txt = status_p
        data["status_buy"] = "开放申购" if "开放申购" in txt else "暂停申购"
        data["status_sell"] = "开放赎回" if "开放赎回" in txt else "暂停赎回"

    # 费用
    for row in soup.select("table.comm.jjfl tr"):
        tds = row.find_all("td")
        if len(tds) < 2: continue
        label, value = tds[0].get_text(strip=True), tds[1].get_text(strip=True)
        if "管理费率" in label: data["mgr_fee"] = value
        if "托管费率" in label: data["cust_fee"] = value
        if "销售服务费率" in label: data["sale_fee"] = value

    # 资产规模
    asset = soup.find(string=re.compile(r"资产规模"))
    if asset:
        m = re.search(r"([\d.]+)亿元.*?截止至：([\d\-]+)", asset)
        if m:
            data["assets"] = m.group(1)
            data["assets_date"] = m.group(2)

    return data if data["nav"] else None

def worker(code: str) -> tuple[str, Optional[Dict]]:
    html = fetch_html(code)
    data = parse_data(html, code) if html else None
    return code, data

# ============================= 主程序 =============================
def main():
    args = parse_args()
    codes = load_codes(args.limit)
    if not codes:
        return

    print(f"开始并行抓取，最大并发: {args.concurrency}")

    results = []
    failed = []

    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        future_to_code = {executor.submit(worker, code): code for code in codes}
        for future in tqdm(as_completed(future_to_code), total=len(codes), desc="抓取进度", unit="只"):
            code, data = future.result()
            if data:
                results.append(data)
            else:
                failed.append(code)

    # 写入 CSV
    if results:
        file_exists = os.path.isfile(CSV_PATH)
        with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
            fieldnames = [
                "date", "code", "nav", "change_pct", "status_buy", "status_sell",
                "mgr_fee", "cust_fee", "sale_fee", "assets", "assets_date"
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerows(results)
        print(f"成功 {len(results)} 条 → {CSV_PATH}")

    # 失败记录
    if failed:
        with open(FAILED_PATH, "w", encoding="utf-8") as f:
            f.write("\n".join(failed))
        print(f"失败 {len(failed)} 只 → {FAILED_PATH}")
    else:
        print("全部成功！")

if __name__ == "__main__":
    main()
