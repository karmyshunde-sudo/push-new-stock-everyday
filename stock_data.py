import akshare as ak
import pandas as pd
import datetime
import pytz
import os
import logging

# 设置日志
logger = logging.getLogger(__name__)

def get_beijing_time():
    """获取当前北京时间"""
    tz = pytz.timezone('Asia/Shanghai')
    return datetime.datetime.now(tz)

def is_trading_day(date=None):
    """
    判断指定日期是否为交易日
    :param date: 日期，默认为今天
    :return: bool 是否为交易日
    """
    if date is None:
        date = get_beijing_time().date()
    
    try:
        # 获取上交所交易日历
        trade_cal_df = ak.tool_trade_date_hist_sina()
        trade_cal_df['trade_date'] = pd.to_datetime(trade_cal_df['trade_date'])
        trade_cal_df['is_open'] = trade_cal_df['is_open'].astype(int)
        
        # 转换为日期格式进行比较
        target_date = pd.to_datetime(date)
        is_open = trade_cal_df[trade_cal_df['trade_date'] == target_date]['is_open'].values
        
        return len(is_open) > 0 and is_open[0] == 1
    except Exception as e:
        logger.error(f"判断交易日出错: {str(e)}")
        # 出错时默认非周末视为交易日
        if date.weekday() < 5:  # 0-4是周一到周五
            return True
        return False

def get_new_stock_subscriptions(test=False):
    """
    获取当天可申购的新股信息
    :param test: 测试模式，使用最近的新股数据
    :return: DataFrame 新股申购信息
    """
    try:
        # 获取新股申购列表
        new_stock_sub_df = ak.stock_new_share_subscription()
        
        # 转换日期列为datetime格式
        new_stock_sub_df['申购日期'] = pd.to_datetime(new_stock_sub_df['申购日期'])
        
        # 获取当前北京时间
        today = get_beijing_time().date()
        
        if test:
            # 测试模式返回最近的3条数据
            return new_stock_sub_df.head(3)
        
        # 筛选出今天可申购的新股
        today_sub_df = new_stock_sub_df[new_stock_sub_df['申购日期'].dt.date == today]
        
        # 重命名并选择需要的列
        result_df = today_sub_df.rename(columns={
            '证券简称': '股票简称',
            '证券代码': '股票代码',
            '发行价格': '发行价格',
            '申购上限': '申购上限',
            '申购日期': '申购日期'
        })[['股票简称', '股票代码', '发行价格', '申购上限', '申购日期']]
        
        # 格式化日期为字符串
        result_df['申购日期'] = result_df['申购日期'].dt.strftime('%Y-%m-%d')
        
        logger.info(f"获取到{len(result_df)}只可申购新股")
        return result_df
    except Exception as e:
        logger.error(f"获取新股申购信息出错: {str(e)}")
        return None

def get_new_stock_listings(test=False):
    """
    获取当天新上市的股票信息
    :param test: 测试模式，使用最近的上市数据
    :return: DataFrame 新上市股票信息
    """
    try:
        # 获取新股上市列表
        new_stock_list_df = ak.stock_new_share_listing()
        
        # 转换日期列为datetime格式
        new_stock_list_df['上市日期'] = pd.to_datetime(new_stock_list_df['上市日期'])
        
        # 获取当前北京时间
        today = get_beijing_time().date()
        
        if test:
            # 测试模式返回最近的3条数据
            return new_stock_list_df.head(3)
        
        # 筛选出今天上市的新股
        today_list_df = new_stock_list_df[new_stock_list_df['上市日期'].dt.date == today]
        
        # 重命名并选择需要的列
        result_df = today_list_df.rename(columns={
            '证券简称': '股票简称',
            '证券代码': '股票代码',
            '发行价格': '发行价格',
            '上市日期': '上市日期'
        })[['股票简称', '股票代码', '发行价格', '上市日期']]
        
        # 格式化日期为字符串
        result_df['上市日期'] = result_df['上市日期'].dt.strftime('%Y-%m-%d')
        
        logger.info(f"获取到{len(result_df)}只新上市股票")
        return result_df
    except Exception as e:
        logger.error(f"获取新上市股票信息出错: {str(e)}")
        return None

def read_new_stock_pushed_flag(date):
    """检查新股信息是否已推送"""
    flag_dir = "data/flags"
    os.makedirs(flag_dir, exist_ok=True)
    flag_file = os.path.join(flag_dir, f"new_stock_pushed_{date.strftime('%Y%m%d')}.txt")
    
    if os.path.exists(flag_file):
        return flag_file, True
    return flag_file, False

def mark_new_stock_info_pushed():
    """标记新股信息已推送"""
    flag_dir = "data/flags"
    os.makedirs(flag_dir, exist_ok=True)
    flag_file = os.path.join(flag_dir, f"new_stock_pushed_{get_beijing_time().strftime('%Y%m%d')}.txt")
    
    with open(flag_file, "w") as f:
        f.write(f"Pushed at {get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')}")
    return flag_file

def read_listing_pushed_flag(date):
    """检查新上市股票信息是否已推送"""
    flag_dir = "data/flags"
    os.makedirs(flag_dir, exist_ok=True)
    flag_file = os.path.join(flag_dir, f"listing_pushed_{date.strftime('%Y%m%d')}.txt")
    
    if os.path.exists(flag_file):
        return flag_file, True
    return flag_file, False

def mark_listing_info_pushed():
    """标记新上市股票信息已推送"""
    flag_dir = "data/flags"
    os.makedirs(flag_dir, exist_ok=True)
    flag_file = os.path.join(flag_dir, f"listing_pushed_{get_beijing_time().strftime('%Y%m%d')}.txt")
    
    with open(flag_file, "w") as f:
        f.write(f"Pushed at {get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')}")
    return flag_file
