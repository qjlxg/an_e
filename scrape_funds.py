#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import time
import random  # 用于随机 sleep
import requests
from bs4 import BeautifulSoup
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------- 配置 ----------
INPUT_TXT = "C类.txt"          # 代码列表文件
OUTPUT_CSV = "fund_data.csv"   # 最终输出的 CSV
BASE_URL = "https://fundf10.eastmoney.com/jbgk_{code}.html"
HEADERS = {
    "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0 Safari/537.36"
}
# 随机 sleep 范围，防封
SLEEP_MIN = 0.5
SLEEP_MAX = 1.5
# 并行线程数（根据网站承受力调整，过高可能被封）
MAX_WORKERS = 10

# ---------- 需要提取的字段（顺序固定，CSV 列顺序） ----------
FIELD_ORDER = [
    "基金代码", "基金全称", "基金简称", "基金类型",
    "发行日期", "成立日期/规模", "资产规模", "份额规模",
    "基金管理人", "基金托管人", "基金经理人", "成立来分红",
    "管理费率", "托管费率", "销售服务费率",
    "最高认购费率", "最高申购费率", "最高赎回费率",
    "业绩比较基准", "跟踪标的"
]

# ---------- 读取代码 ----------
def read_codes() -> list[str]:
    path = Path(INPUT_TXT)
    if not path.is_file():
        raise FileNotFoundError(f"未找到 {INPUT_TXT}")
    codes = [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return codes

# ---------- 抓取并解析单只基金 ----------
def scrape_one(code: str) -> dict:
    url = BASE_URL.format(code=code)
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as e:
        print(f"[{code}] 请求失败: {e}")
        return {"基金代码": code, "错误": str(e)}

    soup = BeautifulSoup(r.text, "html.parser")

    # 页面核心信息在 <table class="info w790"> 中
    table = soup.find("table", class_="info")
    if not table:
        print(f"[{code}] 未找到 info 表格")
        return {"基金代码": code, "错误": "未找到表格"}

    data = {"基金代码": code}

    # 逐行读取 <th> 与 <td>
    rows = table.find_all("tr")
    for tr in rows:
        ths = tr.find_all("th")
        tds = tr.find_all("td")
        for th, td in zip(ths, tds):
            key = th.get_text(strip=True).rstrip("：")
            val = td.get_text(strip=True)
            data[key] = val

    # 统一字段名
    rename_map = {
        "基金全称": "基金全称",
        "基金简称": "基金简称",
        "基金代码": "基金代码",
        "基金类型": "基金类型",
        "发行日期": "发行日期",
        "成立日期/规模": "成立日期/规模",
        "资产规模": "资产规模",
        "份额规模": "份额规模",
        "基金管理人": "基金管理人",
        "基金托管人": "基金托管人",
        "基金经理人": "基金经理人",
        "成立来分红": "成立来分红",
        "管理费率": "管理费率",
        "托管费率": "托管费率",
        "销售服务费率": "销售服务费率",
        "最高认购费率": "最高认购费率",
        "最高申购费率": "最高申购费率",
        "最高赎回费率": "最高赎回费率",
        "业绩比较基准": "业绩比较基准",
        "跟踪标的": "跟踪标的",
    }
    normalized = {}
    for raw_key, raw_val in data.items():
        norm_key = rename_map.get(raw_key, raw_key)
        normalized[norm_key] = raw_val

    # 确保所有列都出现（缺失的填空）
    row = {k: normalized.get(k, "") for k in FIELD_ORDER}
    return row

# ---------- 主流程 ----------
def main():
    codes = read_codes()
    print(f"共读取 {len(codes)} 只基金代码")

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # 提交任务
        futures = {executor.submit(scrape_one, code): code for code in codes}
        
        # 等待完成，按顺序收集（可选：如果不需顺序，用 list(as_completed)）
        for future in as_completed(futures):
            try:
                row = future.result()
                results.append(row)
            except Exception as e:
                code = futures[future]
                print(f"[{code}] 并发错误: {e}")
                results.append({"基金代码": code, "错误": str(e)})
            # 随机 sleep（在并发中，整体已分散）
            time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

    # ---------- 写入 CSV ----------
    out_path = Path(OUTPUT_CSV)
    with out_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FIELD_ORDER)
        writer.writeheader()
        writer.writerows(results)

    print(f"CSV 已生成 → {out_path.resolve()}")

if __name__ == "__main__":
    main()
