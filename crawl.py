#!/usr/bin/env python 
#-*- coding: UTF-8 -*-
###########################################################################
#
# Copyright (c) 2018 www.codingchen.com, Inc. All Rights Reserved
#
##########################################################################
'''
 @brief a spider of 天天基金(http://fund.eastmoney.com)
 @author chenhui
 @date 2018-12-17 19:12:24
'''
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service 
from selenium.webdriver.common.by import By 
#import js2py
import json
import time
import random
import pandas as pd
import os

# --------------------------------------------------------------------------
# 辅助函数：读取基金代码
# --------------------------------------------------------------------------
def read_fund_codes(file_name):
    """
    从指定文件中读取基金代码列表
    """
    fund_codes = []
    try:
        # 尝试使用 UTF-8 编码读取，如果失败则尝试 GBK 或其他常用编码
        with open(file_name, 'r', encoding='utf-8') as f:
            for line in f:
                code = line.strip()
                if code.isdigit() and len(code) in (6, 7): # 确保是有效的基金代码格式
                    fund_codes.append(code)
    except FileNotFoundError:
        print(f"Error: File '{file_name}' not found.")
    except Exception as e:
        print(f"Error reading file {file_name}: {e}")
    return fund_codes

# --------------------------------------------------------------------------
# 核心抓取函数：crawl_details
# --------------------------------------------------------------------------
def crawl_details(fund_ids):
    chrome_options = Options()
    
    # 性能和稳定性优化
    chrome_options.add_argument('--headless=new') 
    chrome_options.add_argument('--disable-dev-shm-usage') 
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--window-size=1920,1080')

    try:
        # 尝试使用预定路径，如果失败则让 Selenium 自动查找或配置
        service = Service(executable_path='/usr/bin/chromedriver')
        driver = webdriver.Chrome(service=service, options=chrome_options)
        print("WebDriver successfully initialized.")
    except Exception as e:
        print(f"Could not initialize driver. Trying auto-discovery. Error: {e}")
        # 如果 service 初始化失败，直接尝试默认方式
        driver = webdriver.Chrome(options=chrome_options)


    # 针对基金经理页面 (jjjl_XXXXXX.html) 结构进行定位
    summary_xpath_map = {
        '现任经理姓名': '//div[@class="jl_intro"]/div[@class="text"]/p[1]/a',
        '现任经理上任日期': '//div[@class="jl_intro"]/div[@class="text"]/p[2]',
        '现任经理简介': '//div[@class="jl_intro"]/div[@class="text"]/p[3]'
    }
    
    # 定义表格数据列名（用于最近一任基金经理变动一览）
    table_headers = ['最近任职-起始期', '最近任职-截止期', '最近任职-基金经理', '最近任职-任职期间', '最近任职-任职回报']

    fund_details = []
    
    for fund_id in fund_ids:
        # fund 列表用于存储单个基金的所有抓取结果
        fund = [fund_id] 
        # 访问基金经理页面
        url = f"http://fundf10.eastmoney.com/jjjl_{fund_id}.html"
        print(f"Start crawl fund manager page: {url}")
        
        try:
            driver.get(url)
            time.sleep(random.randint(3,5)) 
            
            # ----------------- 1. 抓取现任基金经理概要 -----------------
            for field_name, xpath in summary_xpath_map.items():
                try:
                    node = driver.find_element(By.XPATH, xpath)
                    # 清理文本，去除字段名和换行符
                    text = node.text.strip().replace('姓名：', '').replace('上任日期：', '').replace('\n', ' ')
                    fund.append(text)
                except Exception:
                    # 如果找不到元素，记录为 'N/A' (Not Available)
                    fund.append('N/A')

            # ----------------- 2. 抓取基金经理变动一览表格 -----------------
            # 定位表格 tbody (class="w782 comm jloff")
            change_table_xpath = '//div[@class="boxitem w790"]/table[@class="w782 comm jloff"]/tbody'
            
            try:
                table_body = driver.find_element(By.XPATH, change_table_xpath)
                rows = table_body.find_elements(By.TAG_NAME, 'tr')
                
                # 获取第一行数据（最近一次任职）
                if rows:
                    # 抓取所有 <td> 标签的文本
                    first_row_data = [td.text.strip() for td in rows[0].find_elements(By.TAG_NAME, 'td')]
                    fund.extend(first_row_data)
                else:
                    fund.extend(['N/A'] * len(table_headers)) 
                    
            except Exception:
                fund.extend(['N/A'] * len(table_headers))
                
        except Exception as e:
            print(f"Failed to crawl fund manager page for {fund_id}. General Error: {e}")
            # 即使抓取失败，也用 'N/A' 填充所有字段
            fund.extend(['N/A'] * (len(summary_xpath_map) + len(table_headers))) 
        
        fund_details.append(fund)
        time.sleep(random.uniform(1, 3)) # 增加随机等待时间，防止被封

    driver.quit()

    # ----------------- 3. 结果保存为 CSV -----------------
    header = ['基金代码'] + list(summary_xpath_map.keys()) + table_headers
    
    df = pd.DataFrame(fund_details, columns=header)
    
    # 定义输出路径
    output_dir = './data'
    output_file = os.path.join(output_dir, 'fund_manager_details.csv')

    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)
    
    # 写入 CSV 文件，使用 utf-8 编码支持中文
    try:
        df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"\nSuccessfully saved {len(fund_details)} records to {output_file}")
    except Exception as e:
        print(f"Error saving data to CSV: {e}")

# --------------------------------------------------------------------------
# 主函数：整合文件读取和抓取逻辑
# --------------------------------------------------------------------------
def main(): 
    # 假设 'C类.txt' 就在脚本运行的当前目录
    fund_ids = read_fund_codes('C类.txt') 
    
    if not fund_ids:
        print("No fund codes found. Exiting.")
        return
        
    print(f"Total {len(fund_ids)} fund codes read for scraping.")
    
    # 启动抓取
    crawl_details(fund_ids)

# (注意：原有 main 函数中的 crawlbyPage, parse, filter_fund_rank 已被移除，因为
# 您现在只要求基于 C类.txt 的代码进行抓取并保存为 CSV，不再需要原有的全量爬取和筛选逻辑)

if __name__ == '__main__':
    main()
