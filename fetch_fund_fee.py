#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
fetch_fund_fee.py
- 读取根目录 C类.txt（每行一个基金代码）
- 【调试】只抓取前 N 只（默认 3）
- 抓取天天基金网费率页 → 提取关键信息
- 输出为 fund_fee_result.csv（UTF-8 with BOM，兼容 Excel）
"""

import os
import sys
import time
from pathlib import Path
from typing import List, Dict

import requests
from bs4 import BeautifulSoup
import pandas as pd

# ================== 配置 ==================
TXT_FILE = Path("C类.txt")
RESULT_CSV = Path("fund_fee_result.csv")
BASE_URL = "https://fundf10.eastmoney.com/jjfl_{code}.html"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
    )
}
TIMEOUT = 15
RETRY = 3

# 【调试】限制抓取数量，设为 0 表示全部
MAX_FUNDS = 3
# =========================================


def read_codes() -> List[str]:
    if not TXT_FILE.exists():
        print(f"[ERROR] 未找到 {TXT_FILE}")
        sys.exit(1)
    codes = [
        line.strip() for line in TXT_FILE.read_text(encoding="utf-8").splitlines()
        if line.strip() and line.strip().isdigit()
    ]
    if MAX_FUNDS > 0:
        codes = codes[:MAX_FUNDS]
        print(f"[DEBUG] 限制抓取前 {MAX_FUNDS} 只基金: {codes}")
    return codes


def fetch_page(code: str) -> str:
    url = BASE_URL.format(code=code)
    for i in range(RETRY):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if resp.status_code == 200:
                return resp.text
        except Exception as e:
            print(f"[WARN] 抓取 {code} 第{i+1}次失败: {e}")
            time.sleep(2)
    print(f"[ERROR] 抓取 {code} 失败")
    return ""


def parse_fee(html: str, code: str) -> Dict[str, str]:
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(separator="\n")

    def get_val(key: str, default: str = "") -> str:
        for line in text.splitlines():
            if key in line:
                val = line.replace(key, "").strip()
                return " ".join(val.split())
        return default

    info = {
        "基金代码": code,
        "基金名称": "",
        "申购状态": get_val("申购状态"),
        "赎回状态": get_val("赎回状态"),
        "定投状态": get_val("定投状态"),
        "申购起点": get_val("申购起点"),
        "定投起点": get_val("定投起点"),
        "日累计申购限额": get_val("日累计申购限额"),
        "首次购买": get_val("首次购买"),
        "追加购买": get_val("追加购买"),
        "持仓上限": get_val("持仓上限"),
        "最小赎回份额": get_val("最小赎回份额"),
        "部分赎回最低保留份额": get_val("部分赎回最低保留份额"),
        "管理费率": get_val("管理费率"),
        "托管费率": get_val("托管费率"),
        "销售服务费率": get_val("销售服务费率"),
        "认购费率": get_val("认购费率") or "0.00%",
        "申购费率": get_val("申购费率") or "0.00%",
        "赎回费率": "",
        "页面链接": BASE_URL.format(code=code),
    }

    # 基金名称
    title = soup.find("title")
    if title:
        info["基金名称"] = title.text.split("基金费率")[0].strip()

    # 赎回费率（<7天 / >=7天）
    redeem = []
    for line in text.splitlines():
        if "小于7天" in line:
            val = line.split("小于7天")[-1].strip()
            redeem.append(f"<7天: {val}")
        if "大于等于7天" in line:
            val = line.split("大于等于7天")[-1].strip()
            redeem.append(f">=7天: {val}")
    info["赎回费率"] = " | ".join(redeem) if redeem else "0.00%"

    return info


def main():
    codes = read_codes()
    if not codes:
        print("[INFO] 无基金代码，退出。")
        return

    results = []
    for idx, code in enumerate(codes, 1):
        print(f"[{idx}/{len(codes)}] 正在抓取 {code} ...")
        html = fetch_page(code)
        if not html:
            print(f"[SKIP] {code} 页面获取失败")
            continue
        info = parse_fee(html, code)
        results.append(info)
        time.sleep(1)  # 防反爬

    if not results:
        print("[WARN] 没有成功抓取任何基金")
        return

    # 使用 pandas 输出 CSV（带 BOM，Excel 友好）
    df = pd.DataFrame(results)
    # 统一列顺序
    columns_order = [
        "基金代码", "基金名称", "申购状态", "赎回状态", "定投状态",
        "申购起点", "定投起点", "日累计申购限额",
        "首次购买", "追加购买", "持仓上限",
        "最小赎回份额", "部分赎回最低保留份额",
        "管理费率", "托管费率", "销售服务费率",
        "认购费率", "申购费率", "赎回费率", "页面链接"
    ]
    df = df[columns_order]

    # 写入 CSV（UTF-8 with BOM）
    csv_content = df.to_csv(index=False, encoding="utf-8-sig")
    RESULT_CSV.write_text(csv_content, encoding="utf-8-sig")
    print(f"[SUCCESS] 已保存 {len(results)} 条记录 → {RESULT_CSV}")


if __name__ == "__main__":
    main()
