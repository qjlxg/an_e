#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_fund_fee.py - 优化版：强制脚本目录 + 详细日志 + 鲁棒 HTML 解析 + 修复所有潜在崩溃点
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
os.chdir(SCRIPT_DIR)
print(f"[INFO] 工作目录已切换到: {SCRIPT_DIR}")

TXT_FILE = SCRIPT_DIR / "C类.txt"
RESULT_CSV = SCRIPT_DIR / "fund_fee_result.csv"

# ================== 配置 ==================
BASE_URL = "https://fundf10.eastmoney.com/jjfl_{code}.html"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
}
TIMEOUT = 15
RETRY = 3
MAX_FUNDS = 5  # 调试用，0 = 全部


# =========================================
def read_codes() -> List[str]:
    """读取 C类.txt 文件中的基金代码"""
    if not TXT_FILE.exists():
        print(f"[ERROR] 未找到 {TXT_FILE}")
        sys.exit(1)

    codes = []
    content = TXT_FILE.read_text(encoding="utf-8", errors="ignore")
    for line in content.splitlines():
        line = line.strip()
        if line.isdigit() and len(line) == 6:
            codes.append(line)

    if MAX_FUNDS > 0 and codes:
        codes = codes[:MAX_FUNDS]
        print(f"[DEBUG] 限制抓取前 {MAX_FUNDS} 只: {codes}")

    if not codes:
        print("[WARN] C类.txt 中未找到有效6位基金代码")
        sys.exit(1)

    return codes


def fetch_page(code: str) -> str:
    """抓取基金费率页面"""
    url = BASE_URL.format(code=code)
    for i in range(RETRY):
        try:
            print(f"[HTTP] GET {url}")
            resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if resp.status_code == 200:
                print(f"[OK] {code} 页面获取成功 ({len(resp.text)} 字符)")
                return resp.text
            else:
                print(f"[WARN] {code} 状态码 {resp.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"[ERROR] 抓取 {code} 失败 (重试 {i+1}/{RETRY}): {e}")
        time.sleep(2 ** i)  # 指数退避
    return ""


def parse_fee(html: str, code: str) -> Dict[str, str]:
    """解析 HTML 提取费率信息（增强容错）"""
    soup = BeautifulSoup(html, "lxml")

    info = {
        "基金代码": code,
        "基金名称": "未知",
        "申购状态": "未知",
        "赎回状态": "未知",
        "定投状态": "未知",
        "申购起点": "未知",
        "定投起点": "未知",
        "日累计申购限额": "未知",
        "管理费率": "未知",
        "托管费率": "未知",
        "销售服务费率": "未知",
        "赎回费率": "未知",
        "页面链接": BASE_URL.format(code=code),
    }

    try:
        # 1. 基金名称（从 title）
        title_tag = soup.find("title")
        if title_tag and "基金费率" in title_tag.text:
            name_part = title_tag.text.split("基金费率")[0].strip()
            info["基金名称"] = name_part.split('(')[0].strip()
    except:
        pass

    try:
        # 2. 申购/赎回状态（顶部 .bs_jz 区域）
        status_div = soup.find('div', class_='bs_jz')
        if status_div:
            p_tags = status_div.find_all('p', class_='row')
            for p in p_tags:
                spans = p.find_all('span')
                if len(spans) >= 2:
                    info["申购状态"] = spans[0].get_text(strip=True)
                    info["赎回状态"] = spans[1].get_text(strip=True)
                    break
    except:
        pass

    try:
        # 3. 定投状态（尝试多种方式）
        if soup.find('a', href=lambda h: h and 'Investment/add' in h):
            info["定投状态"] = "开放定投"
        elif "开放申购" in info["申购状态"]:
            info["定投状态"] = "开放定投"
        elif "暂停申购" in info["申购状态"]:
            info["定投状态"] = "暂停定投"
        else:
            info["定投状态"] = "未知"
    except:
        pass

    try:
        # 4. 管理/托管/服务费率（第一个 .comm 表格）
        fee_table = soup.find('table', class_='w790 comm js-fundfl')
        if fee_table:
            for row in fee_table.find_all('tr'):
                tds = row.find_all('td')
                if len(tds) >= 2:
                    key = tds[0].get_text(strip=True)
                    val = tds[1].get_text(strip=True)
                    if "管理费" in key:
                        info["管理费率"] = val
                    elif "托管费" in key:
                        info["托管费率"] = val
                    elif "销售服务费" in key:
                        info["销售服务费率"] = val
    except:
        pass

    try:
        # 5. 申购/定投起点 & 限额（第二个 boxitem 下的表格）
        box = soup.find('div', class_='boxitem w790')
        if box:
            table = box.find_next('table')
            if table:
                for row in table.find_all('tr'):
                    tds = row.find_all('td')
                    if len(tds) >= 2:
                        key = tds[0].get_text(strip=True)
                        val = tds[1].get_text(strip=True)
                        if "申购起点" in key:
                            info["申购起点"] = val
                        elif "定投起点" in key:
                            info["定投起点"] = val
                        elif "日累计申购限额" in key:
                            info["日累计申购限额"] = val
    except:
        pass

    try:
        # 6. 赎回费率（第三个表格）
        redeem_table = None
        tables = soup.find_all('table', class_='w790 comm')
        if len(tables) >= 3:
            redeem_table = tables[2]  # 通常是第三个
        elif len(tables) >= 2:
            redeem_table = tables[1]

        redeem_list = []
        if redeem_table:
            rows = redeem_table.find_all('tr')[1:]  # 跳过表头
            for row in rows:
                tds = row.find_all('td')
                if len(tds) >= 2:
                    period = tds[0].get_text(strip=True).replace('\n', ' ').replace('\r', '')
                    rate = tds[1].get_text(strip=True)
                    redeem_list.append(f"{period}: {rate}")
        info["赎回费率"] = " | ".join(redeem_list) if redeem_list else "0.00%"
    except:
        pass

    return info


def main():
    print(f"[START] 开始执行，时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    codes = read_codes()

    results = []
    for idx, code in enumerate(codes, 1):
        print(f"\n[{idx}/{len(codes)}] 正在处理 {code} ...", flush=True)
        html = fetch_page(code)
        if not html:
            print(f"[SKIP] {code} 获取失败")
            continue

        try:
            info = parse_fee(html, code)
            results.append(info)
            print(f"[OK] {code} 解析完成 → {info['基金名称']}")
        except Exception as e:
            print(f"[FATAL] {code} 解析崩溃: {e}")
            import traceback
            traceback.print_exc()
            continue

        time.sleep(1.5)  # 友好爬取

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
        RESULT_CSV.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(RESULT_CSV, index=False, encoding="utf-8-sig")
        print(f"\n[SUCCESS] CSV 已写入: {RESULT_CSV.resolve()}")
        print(f" → 共 {len(results)} 条记录")
        print(f" → 预览第一条:\n{df.iloc[0]}")
    except Exception as e:
        print(f"[FATAL] 写入 CSV 失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
