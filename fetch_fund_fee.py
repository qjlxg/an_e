#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
fetch_fund_fee.py - 强制使用脚本所在目录 + 详细日志 + 安全写入
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
BASE_URL = "https://fundf10.eastmoney.com/jbgk_{code}.html"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}
TIMEOUT = 15
RETRY = 3
MAX_FUNDS = 3  # 调试用，0 = 全部
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
        except Exception as e:
            print(f"[ERROR] 抓取 {code} 失败: {e}")
        time.sleep(2)
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
        "基金名称": soup.find("title").text.split("基金费率")[0].strip() if soup.find("title") else "",
        "申购状态": get_val("申购状态"),
        "赎回状态": get_val("赎回状态"),
        "定投状态": get_val("定投状态"),
        "申购起点": get_val("申购起点"),
        "定投起点": get_val("定投起点"),
        "日累计申购限额": get_val("日累计申购限额"),
        "管理费率": get_val("管理费率"),
        "托管费率": get_val("托管费率"),
        "销售服务费率": get_val("销售服务费率"),
        "赎回费率": "",
        "页面链接": BASE_URL.format(code=code),
    }

    # 赎回费率
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
        info = parse_fee(html, code)
        results.append(info)
        print(f"[OK] {code} 解析完成")
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
