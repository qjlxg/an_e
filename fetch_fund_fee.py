#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_fund_fee.py - 优化版：强制脚本目录 + 详细日志 + 鲁棒 HTML 解析 + 修复所有潜在崩溃点
"""
import os
import sys
import time
from pathlib import Path
import re 
from typing import List, Dict
import requests
from bs4 import BeautifulSoup
import pandas as pd
import traceback 

# ================== 强制使用脚本所在目录 ==================
SCRIPT_DIR = Path(__file__).parent.resolve()
os.chdir(SCRIPT_DIR)
print(f"[INFO] 工作目录已切换到: {SCRIPT_DIR}")

TXT_FILE = SCRIPT_DIR / "C类.txt"
RESULT_CSV = SCRIPT_DIR / "fund_fee_result.csv"

# ================== 配置 (已优化：TIMEOUT增加, 避免抓取频率过快) ==================
BASE_URL = "https://fundf10.eastmoney.com/jjfl_{code}.html"
HEADERS = {
    # 保持 User-Agent 最新
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
}
TIMEOUT = 30  # ***增加超时时间到 30 秒***
RETRY = 3
MAX_FUNDS = 10  # ***设置为 0，表示处理全部基金***


# =========================================
def read_codes() -> List[str]:
    """读取 C类.txt 文件中的基金代码"""
    if not TXT_FILE.exists():
        print(f"[ERROR] 未找到 {TXT_FILE}")
        sys.exit(1)

    codes = []
    try:
        with open(TXT_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                code = line.strip()
                if code and code.isdigit() and len(code) >= 6:
                    codes.append(code)
    except Exception as e:
        print(f"[ERROR] 读取 {TXT_FILE} 文件失败: {e}")
        sys.exit(1)
    
    if MAX_FUNDS > 0 and len(codes) > MAX_FUNDS:
        print(f"[WARN] 调试模式开启，只处理前 {MAX_FUNDS} 个基金")
        return codes[:MAX_FUNDS]

    return codes

def fetch_page(code: str) -> str:
    """尝试多次抓取基金费率页面 (已优化重试逻辑)"""
    url = BASE_URL.format(code=code)
    for i in range(RETRY):
        try:
            # 增加重试间的等待时间，避免被封
            if i > 0:
                wait_time = 3 + i * 2  # 例如：3秒，5秒，7秒
                print(f"[WAIT] {code} 等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)
                
            response = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            response.raise_for_status() # 检查 HTTP 状态码
            
            # 检查返回内容，确保抓取到的是目标页面
            if '<title>基金费率' in response.text:
                return response.text
            else:
                print(f"[WARN] {code} 抓取到非目标内容，重试 {i+1}/{RETRY}")
                # 针对非目标内容，也进行重试
                continue

        except requests.exceptions.Timeout:
            # 单独处理超时
            print(f"[ERROR] {code} 第 {i+1}/{RETRY} 次请求失败: 请求超时 (Timeout={TIMEOUT}s)")
            continue
        except requests.exceptions.RequestException as e:
            # 处理其他所有请求错误
            print(f"[ERROR] {code} 第 {i+1}/{RETRY} 次请求失败: {e}")
            continue

    print(f"[FATAL] {code} 最终抓取失败")
    return ""

# ================== 核心解析函数 (已优化) ==================
def parse_fee(html: str, code: str) -> Dict[str, str]:
    """
    使用基于内容定位的更精确逻辑解析 HTML 页面，提取基金费率信息。
    """
    soup = BeautifulSoup(html, 'html.parser')
    info = {
        "基金代码": code,
        "页面链接": BASE_URL.format(code=code),
    }

    # 1. 基金名称
    try:
        fund_title_element = soup.select_one('div.bs_jz h4.title a')
        if fund_title_element:
            name_text = fund_title_element.text.strip()
            # 移除基金代码部分及其前面的空格/括号
            info["基金名称"] = re.sub(r'\s*\(\s*' + re.escape(code) + r'\s*\)', '', name_text).strip().replace('...', '')
        else:
            # 从 <title> 标签中提取备用名称
            title_text = soup.find('title').text
            # 汇添富中证芯片产业指数增强发起式C(014194)基金费率 ... -> 汇添富中证芯片产业指数增强发起式C
            match = re.search(r'(.+)\(' + re.escape(code) + r'\)', title_text)
            if match:
                 info["基金名称"] = match.group(1).strip()
            else:
                 info["基金名称"] = "未知"
    except Exception:
        info["基金名称"] = "未知"

    # =======================================================
    # 辅助函数：根据 H4 标题查找对应的表格
    def find_section_table(title_text):
        """查找包含特定标题文本的 section 内部的表格"""
        # 查找 H4 标签中包含目标文本的元素
        h4 = soup.find('h4', string=lambda t: t and title_text in t)
        if h4:
            # 向上找到最近的 'boxitem' 容器
            boxitem = h4.find_parent('div', class_='boxitem')
            if boxitem:
                # 在 boxitem 内部查找 class='jjfl' 的表格
                return boxitem.find('table', class_='jjfl')
        return None

    # =======================================================
    # 2. 交易状态
    try:
        table_status = find_section_table("交易状态")
        if table_status:
            # 找到所有非 th 的 td 单元格 (即值)
            cells = table_status.select('tr:nth-of-type(1) td:not(.th)')
            info["申购状态"] = cells[0].text.strip() if len(cells) > 0 else "未知"
            info["赎回状态"] = cells[1].text.strip() if len(cells) > 1 else "未知"
            info["定投状态"] = cells[2].text.strip() if len(cells) > 2 else "未知"
        else:
            info["申购状态"] = info["赎回状态"] = info["定投状态"] = "未知"
    except Exception:
        info["申购状态"] = info["赎回状态"] = info["定投状态"] = "未知"

    # 3. 申购与赎回金额 (申购起点, 定投起点, 日累计申购限额)
    try:
        table_limit = find_section_table("申购与赎回金额")
        if table_limit:
            # 申购/定投/限额 在第一个 tr
            row_buy = table_limit.select('tbody tr')[0]
            # 找到所有非 th 的 td 单元格 (即值)
            cells = row_buy.select('td:not(.th)')
            info["申购起点"] = cells[0].text.strip() if len(cells) > 0 else "未知"
            info["定投起点"] = cells[1].text.strip() if len(cells) > 1 else "未知"
            info["日累计申购限额"] = cells[2].text.strip() if len(cells) > 2 else "未知"
        else:
            info["申购起点"] = info["定投起点"] = info["日累计申购限额"] = "未知"
    except Exception:
        info["申购起点"] = info["定投起点"] = info["日累计申购限额"] = "未知"

    # 4. 运作费用 (管理费率, 托管费率, 销售服务费率)
    try:
        table_op_fee = find_section_table("运作费用")
        if table_op_fee:
            # 找到所有非 th 的 td 单元格 (即值)
            cells = table_op_fee.select('tr:nth-of-type(1) td:not(.th)')
            info["管理费率"] = cells[0].text.strip() if len(cells) > 0 else "未知"
            info["托管费率"] = cells[1].text.strip() if len(cells) > 1 else "未知"
            info["销售服务费率"] = cells[2].text.strip() if len(cells) > 2 else "未知"
        else:
            info["管理费率"] = info["托管费率"] = info["销售服务费率"] = "未知"
    except Exception:
        info["管理费率"] = info["托管费率"] = info["销售服务费率"] = "未知"

    # 5. 赎回费率
    try:
        table_redeem = find_section_table("赎回费率")
        redeem_list = []
        if table_redeem:
            # 遍历 tbody 中的所有行
            rows = table_redeem.select('tbody tr')
            for row in rows:
                # 抓取所有 td 单元格
                cells = row.find_all('td')
                # 期望的结构: 适用金额(cells[0]) | 适用期限(cells[1]) | 赎回费率(cells[2])
                if len(cells) >= 3:
                    term = cells[1].text.strip() # 适用期限
                    rate = cells[2].text.strip() # 赎回费率
                    redeem_list.append(f"{term}: {rate}")

        info["赎回费率"] = " | ".join(redeem_list) if redeem_list else "0.00%"
    except Exception:
        info["赎回费率"] = "未知"
    
    # 6. 确保所有键都存在，以便写入 DataFrame
    columns_to_check = [\
        "基金名称", "申购状态", "赎回状态", "定投状态",\
        "申购起点", "定投起点", "日累计申购限额",\
        "管理费率", "托管费率", "销售服务费率",\
        "赎回费率"\
    ]
    for col in columns_to_check:
        if col not in info:
            info[col] = "未知"

    return info

# =========================================
def main():
    print(f"[START] 开始执行，时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    codes = read_codes()

    results = []
    for idx, code in enumerate(codes, 1):
        # 确保每次循环开始前有 1.5 秒的等待
        if idx > 1:
            time.sleep(1.5)
            
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
            traceback.print_exc()
            continue

    if not results:
        print("\n[WARN] 没有成功解析任何基金")
        return

    # 写入 CSV
    df = pd.DataFrame(results)
    columns = [\
        "基金代码", "基金名称", "申购状态", "赎回状态", "定投状态",\
        "申购起点", "定投起点", "日累计申购限额",\
        "管理费率", "托管费率", "销售服务费率",\
        "赎回费率", "页面链接"\
    ]
    # 确保 DataFrame 的列顺序正确
    df = df.reindex(columns=columns, fill_value='未知')
    
    # 使用 'utf_8_sig' 编码，确保 Excel 打开中文不乱码
    df.to_csv(RESULT_CSV, index=False, encoding='utf_8_sig')
    print(f"\n[DONE] 所有数据已保存到 {RESULT_CSV}")


if __name__ == '__main__':
    main()
