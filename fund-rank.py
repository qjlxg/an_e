#!/usr/bin/python
# -*- coding: utf-8 -*-
import time
import datetime
import glob
import urllib.request
import json
import sys
import re
import threading
import queue
import os
import math

# --- 常量定义 ---
# 假设年化交易日为252天
ANNUALIZATION_FACTOR = 252
# 假设年化无风险收益率为3% (可根据实际情况调整)
RISK_FREE_RATE = 0.03
# 均线周期定义
SHORT_MA_PERIOD = 20 # 短期均线 (约一个月交易日)
LONG_MA_PERIOD = 60  # 长期均线 (约一个季度交易日)

# --- 原始使用方法 ---
def usage():
    print('fund-rank.py usage:')
    print('\tpython fund.py start-date end-date fund-code=none\n')
    print('\tdate format ****-**-**')
    print('\t\tstart-date must before end-date')
    print('\tfund-code default none')
    print('\t\tif not input, get top 20 funds from all funds in C类.txt')
    print('\t\telse get that fund\'s rate of rise and risk metrics\n')
    print('\teg:\tpython fund-rank.py 2017-03-01 2017-03-25')
    print('\teg:\tpython fund-rank.py 2017-03-01 2017-03-25 377240')

# --- 原始函数：获取某一基金在某一日的累计净值数据 (为保留原功能而保留) ---
def get_jingzhi(strfundcode, strdate):
    try:
        url = 'http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=' + \
              strfundcode + '&page=1&per=20&sdate=' + strdate + '&edate=' + strdate
        response = urllib.request.urlopen(url)
    except urllib.error.HTTPError as e:
        return '-1'
    except Exception as e:
        return '-1'

    json_fund_value = response.read().decode('utf-8')
    tr_re = re.compile(r'<tr>(.*?)</tr>')
    item_re = re.compile(r'''<td>(\d{4}-\d{2}-\d{2})</td><td.*?>(.*?)</td><td.*?>(.*?)</td><td.*?>(.*?)</td><td.*?>(.*?)</td><td.*?>(.*?)</td><td.*?></td>''', re.X)

    jingzhi = '-1'
    for line in tr_re.findall(json_fund_value):
        match = item_re.match(line)
        if match:
            entry = match.groups()
            jingzhi1 = entry[1] # 单位净值
            jingzhi2 = entry[2] # 累计净值
            
            if jingzhi2.strip() == '':
                jingzhi = '-1'
            elif jingzhi2.find('%') > -1:
                jingzhi = '-1'
            elif float(jingzhi1) > float(jingzhi2):
                jingzhi = entry[1]
            else:
                jingzhi = entry[2]

    return jingzhi
    
# --- 新增函数：从本地文件加载历史净值数据 (已适应 CSV 格式) ---
def load_local_data(strfundcode, strsdate, stredate):
    """
    从fund_data目录加载基金历史净值数据，适应 CSV 格式：
    读取：第1列(日期) 和 第3列(累计净值)
    """
    # 优先尝试 .txt，再尝试 .csv
    data_file = os.path.join('fund_data', f'{strfundcode}.txt')
    if not os.path.exists(data_file):
        data_file = os.path.join('fund_data', f'{strfundcode}.csv')
        if not os.path.exists(data_file):
            return None

    net_values_map = {} # {date: net_value}

    try:
        with open(data_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            for line in lines[1:]: # 跳过第一行表头
                parts = line.strip().split(',')
                # 检查至少有3列：日期,单位净值,累计净值
                if len(parts) >= 3:
                    date_str = parts[0].strip()
                    try:
                        net_value = float(parts[2].strip()) # 从索引 2 读取累计净值
                        net_values_map[date_str] = net_value
                    except ValueError:
                        continue
    except Exception as e:
        return None
    
    # 筛选出在指定日期范围内的数据，并按日期排序
    sorted_dates = sorted([d for d in net_values_map.keys() if strsdate <= d <= stredate])
    
    if not sorted_dates:
        return None

    # 返回所有在范围内的日期和净值序列
    return sorted_dates, [net_values_map[d] for d in sorted_dates]

# --- 新增函数：计算简单移动平均 (SMA) ---
def calculate_moving_average(net_values, period):
    """
    计算净值序列末尾的简单移动平均。
    如果数据点数量不足 period，则返回 None。
    """
    if len(net_values) < period:
        return None
    
    # 截取序列末尾 period 个数据点
    ma_values = net_values[-period:]
    return sum(ma_values) / period

# --- 新增函数：计算最大回撤 ---
def calculate_mdd(net_values):
    """计算最大回撤（Maximum Drawdown）"""
    if not net_values:
        return 0.0

    peak_value = net_values[0]
    max_drawdown = 0.0

    for current_value in net_values:
        if current_value > peak_value:
            peak_value = current_value
        
        drawdown = (peak_value - current_value) / peak_value
        
        if drawdown > max_drawdown:
            max_drawdown = drawdown

    return round(max_drawdown * 100, 2) # 返回百分比

# --- 新增函数：计算夏普比率 (已添加健壮性处理) ---
def calculate_sharpe_ratio(net_values):
    """
    计算夏普比率（Sharpe Ratio）。
    返回: (sharpe_ratio, warning_type)
    """
    if len(net_values) < 2:
        return 0.0, "NODATA"

    daily_returns = []
    for i in range(1, len(net_values)):
        ret = (net_values[i] / net_values[i-1]) - 1
        daily_returns.append(ret)

    num_trading_days = len(daily_returns)
    
    # 警告：交易日不足以进行可靠的年化 (少于10个交易日)
    if num_trading_days < 10: 
        return 0.0, "INSUFFICIENT_DATA"

    avg_daily_return = sum(daily_returns) / num_trading_days
    
    variance = sum([(r - avg_daily_return) ** 2 for r in daily_returns]) / num_trading_days
    std_dev_daily_return = math.sqrt(variance)

    if std_dev_daily_return == 0:
        return 0.0, "ZERO_VOLATILITY"

    sharpe_ratio = (avg_daily_return * ANNUALIZATION_FACTOR - RISK_FREE_RATE) / \
                   (std_dev_daily_return * math.sqrt(ANNUALIZATION_FACTOR))
    
    return round(sharpe_ratio, 4), "OK"

# --- 线程工作函数 (已加入 MA 逻辑) ---
def worker(q, strsdate, stredate, result_queue):
    while not q.empty():
        fund = q.get()
        strfundcode = fund[0]
        
        local_data = load_local_data(strfundcode, strsdate, stredate)

        jingzhimin = '0'
        jingzhimax = '0'
        jingzhidif = 0.0
        jingzhirise = 0.0
        max_drawdown = 0.0
        sharpe_ratio = 0.0
        ma_trend = 'N/A' # 均线趋势

        if local_data:
            sorted_dates, net_values = local_data
            
            if len(net_values) > 1:
                jingzhimin = '%.4f' % net_values[0]
                jingzhimax = '%.4f' % net_values[-1]
                
                jingzhidif = float('%.4f' % (net_values[-1] - net_values[0]))
                
                if float(jingzhimin) != 0:
                    jingzhirise = float('%.2f' % (jingzhidif * 100 / float(jingzhimin)))
                
                max_drawdown = calculate_mdd(net_values)
                sharpe_ratio, warning_type = calculate_sharpe_ratio(net_values)
                
                # --- 均线计算和趋势判断 ---
                # 注意：这里计算均线，使用的是整个时间段内的净值序列
                sma20 = calculate_moving_average(net_values, SHORT_MA_PERIOD)
                sma60 = calculate_moving_average(net_values, LONG_MA_PERIOD)
                
                if sma20 is not None and sma60 is not None:
                    if sma20 > sma60:
                        ma_trend = '↑' # 短期均线高于长期均线：多头/上涨趋势
                    elif sma20 < sma60:
                        ma_trend = '↓' # 短期均线低于长期均线：空头/下跌趋势
                    else:
                        ma_trend = '—' # 持平
                else:
                    ma_trend = 'N/A' # 数据不足，无法计算均线
                # --- 均线计算结束 ---

                if warning_type != "OK":
                    if warning_type == "INSUFFICIENT_DATA":
                        print(f"Warning: Fund {strfundcode} ({fund[2]}) data is too short for reliable annualization. Sharpe set to 0.0.")
                    elif warning_type == "ZERO_VOLATILITY":
                         print(f"Warning: Fund {strfundcode} ({fund[2]}) has zero volatility (net value unchanged). Sharpe set to 0.0.")
            else:
                 print(f"Warning: Fund {strfundcode} ({fund[2]}) has insufficient data points (less than 2) in the period.")
        else:
             print(f"Warning: Fund {strfundcode} ({fund[2]}) local data not found or incomplete.")


        # fund: [0:代码, 1:简写, 2:名称, 3:类型, 4:状态, 5:净值min, 6:净值max, 7:净增长, 8:增长率, 9:最大回撤, 10:夏普比率, 11:MA趋势]
        fund.append(jingzhimin)     # 5
        fund.append(jingzhimax)     # 6
        fund.append(jingzhidif)     # 7
        fund.append(jingzhirise)    # 8
        fund.append(max_drawdown)   # 9 
        fund.append(sharpe_ratio)   # 10
        fund.append(ma_trend)       # 11 
        
        result_queue.put(fund)
        print('process fund:\t' + fund[0] + '\t' + fund[2])
        q.task_done()

# --- 主函数 (已更新输出格式) ---
def main(argv):
    gettopnum = 50
    
    if len(sys.argv) != 3 and len(sys.argv) != 4:
        usage()
        sys.exit(1)
    
    strsdate = sys.argv[1]
    stredate = sys.argv[2]
    
    strtoday = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d')
    tdatetime = datetime.datetime.strptime(strtoday, '%Y-%m-%d')
    
    sdatetime = datetime.datetime.strptime(strsdate, '%Y-%m-%d')
    if sdatetime.isoweekday() in [6, 7]:
        sdatetime += datetime.timedelta(days=- (sdatetime.isoweekday() - 5))
    strsdate = datetime.datetime.strftime(sdatetime, '%Y-%m-%d')

    edatetime = datetime.datetime.strptime(stredate, '%Y-%m-%d')
    if edatetime.isoweekday() in [6, 7]:
        edatetime += datetime.timedelta(days=- (edatetime.isoweekday() - 5))
    stredate = datetime.datetime.strftime(edatetime, '%Y-%m-%d')

    if edatetime <= sdatetime or tdatetime <= sdatetime or tdatetime <= edatetime:
        print('date input error!\n')
        usage()
        sys.exit(1)

    # --- 处理单个基金查询 (已更新输出格式和逻辑) ---
    if len(sys.argv) == 4:
        strfundcode = sys.argv[3]
        
        local_data = load_local_data(strfundcode, strsdate, stredate)
        
        if not local_data:
            print(f'Cannot find local data for {strfundcode} or data is incomplete/missing!\n')
            usage()
            sys.exit(1)

        sorted_dates, net_values = local_data
        
        if len(net_values) < 2:
             print(f'Local data for {strfundcode} has fewer than 2 entries in the period!\n')
             usage()
             sys.exit(1)

        jingzhimin = '%.4f' % net_values[0]
        jingzhimax = '%.4f' % net_values[-1]
        
        jingzhidif = float('%.4f' % (net_values[-1] - net_values[0]))
        jingzhirise = float('%.2f' % (jingzhidif * 100 / float(jingzhimin)))
        
        max_drawdown = calculate_mdd(net_values)
        sharpe_ratio, warning_type = calculate_sharpe_ratio(net_values)
        
        # --- 单个基金的均线计算 ---
        sma20 = calculate_moving_average(net_values, SHORT_MA_PERIOD)
        sma60 = calculate_moving_average(net_values, LONG_MA_PERIOD)
        ma_trend = 'N/A'
        
        if sma20 is not None and sma60 is not None:
            if sma20 > sma60:
                ma_trend = '↑' 
            elif sma20 < sma60:
                ma_trend = '↓' 
            else:
                ma_trend = '—'
        # --- 均线计算结束 ---

        if warning_type != "OK":
            if warning_type == "INSUFFICIENT_DATA":
                print(f"Warning: Fund {strfundcode} data is too short for reliable annualization. Sharpe set to 0.0.")
            elif warning_type == "ZERO_VOLATILITY":
                 print(f"Warning: Fund {strfundcode} has zero volatility (net value unchanged). Sharpe set to 0.0.")
                 
        print('fund:' + strfundcode + '\n')
        
        # 新增 MA 趋势输出
        print(f'{strsdate}\t{stredate}\t净增长\t增长率\t最大回撤\t夏普比率\tMA趋势 (20日>60日)')
        print(f'{jingzhimin}\t\t{jingzhimax}\t\t{str(jingzhidif)}\t{str(jingzhirise)}%\t\t{str(max_drawdown)}%\t\t{str(sharpe_ratio)}\t\t{ma_trend}')
        sys.exit(0)
        
    # --- 基金列表获取 (从 C类.txt 获取代码) ---
    
    c_funds_list = []
    c_list_file = 'C类.txt'
    if not os.path.exists(c_list_file):
        print(f'Error: C类.txt file not found in current directory!')
        sys.exit(1)
        
    print(f'从 {c_list_file} 读取基金代码...')
    try:
        with open(c_list_file, 'r', encoding='utf-8') as f:
            for line in f:
                code = line.strip()
                if code and code != 'code': # 排除文件中的 'code' 表头，如果存在的话
                    # 格式：[代码, 简写, 名称, 类型, 状态]
                    c_funds_list.append([code, 'N/A', 'N/A', 'C类', 'N/A'])
    except Exception as e:
        print(f'Error reading {c_list_file}: {e}')
        sys.exit(1)
        
    all_funds_list = c_funds_list
    print('已读取 C 类基金数量：' + str(len(all_funds_list)))
      
    print('start:')
    print(datetime.datetime.now())
    print('funds sum:' + str(len(all_funds_list)))
    
    # --- 并行处理部分开始 ---
    task_queue = queue.Queue()
    result_queue = queue.Queue()

    for fund in all_funds_list:
        task_queue.put(fund)

    threads = []
    num_threads = 10 
    for i in range(num_threads):
        t = threading.Thread(target=worker, args=(task_queue, strsdate, stredate, result_queue))
        t.daemon = True
        t.start()
        threads.append(t)

    task_queue.join()

    all_funds_list = []
    while not result_queue.empty():
        all_funds_list.append(result_queue.get())
    # --- 并行处理部分结束 ---

    fileobject = open('result_' + strsdate + '_' + stredate + '_C类_Local_Analysis.txt', 'w')
    
    # 排序：按增长率 (fund[8]) 降序排列
    all_funds_list.sort(key=lambda fund: fund[8], reverse=True) 
    
    # 新增 MA 趋势输出头
    strhead = '排序\t' + '编码\t\t' + '名称\t\t' + '类型\t\t' + \
    strsdate + '\t' + stredate + '\t' + '净增长\t' + '增长率\t' + '最大回撤\t' + '夏普比率\t' + 'MA趋势' + '\n'
    print(strhead)
    fileobject.write(strhead)
    
    # fund: [0:代码, 1:简写, 2:名称, 3:类型, 4:状态, 5:净值min, 6:净值max, 7:净增长, 8:增长率, 9:最大回撤, 10:夏普比率, 11:MA趋势]
    for index in range(len(all_funds_list)):
        fund_data = all_funds_list[index]
        # 新增 MA 趋势输出内容 (索引 11)
        strcontent = f"{index+1}\t" \
                     f"{fund_data[0]}\t" \
                     f"{fund_data[2]}\t\t" \
                     f"{fund_data[3]}\t\t" \
                     f"{fund_data[5]}\t\t" \
                     f"{fund_data[6]}\t\t" \
                     f"{str(fund_data[7])}\t" \
                     f"{str(fund_data[8])}%\t\t" \
                     f"{str(fund_data[9])}%\t\t" \
                     f"{str(fund_data[10])}\t\t" \
                     f"{fund_data[11]}\n"
                     
        print(strcontent)
        fileobject.write(strcontent)
        
        if index >= gettopnum:
            break
        
    fileobject.close()
    
    print('end:')
    print(datetime.datetime.now())
    
    sys.exit(0)
    
if __name__ == "__main__":
    main(sys.argv)
