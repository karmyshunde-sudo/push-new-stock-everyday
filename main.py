import os
import sys
import time
import pandas as pd
import numpy as np
import datetime
import pytz
import json
import requests
from bs4 import BeautifulSoup
from config import Config
from logger import get_logger

# 初始化日志
logger = get_logger(__name__)

def get_beijing_time():
    """获取当前北京时间"""
    tz = pytz.timezone('Asia/Shanghai')
    return datetime.datetime.now(tz)

def is_trading_day():
    """判断今天是否为交易日（简单判断：非周末）"""
    # 更精确的判断可以调用akshare的交易日历接口
    today = get_beijing_time().weekday()
    # 0=周一, 4=周五，周末不交易
    return 0 <= today <= 4

def get_new_stock_subscriptions(test=False):
    """获取当天可申购的新股信息"""
    try:
        import akshare as ak
        
        # 如果是测试模式，获取最近的新股数据
        if test:
            start_date = (get_beijing_time() - datetime.timedelta(days=30)).strftime('%Y%m%d')
        else:
            start_date = get_beijing_time().strftime('%Y%m%d')
        
        end_date = get_beijing_time().strftime('%Y%m%d')
        
        # 使用akshare获取新股申购数据
        new_stock_df = ak.stock_new_share_subscription(symbol="最新")
        
        # 筛选出今天的新股
        if not new_stock_df.empty:
            # 转换日期格式并筛选
            new_stock_df['申购日期'] = pd.to_datetime(new_stock_df['申购日期']).dt.strftime('%Y-%m-%d')
            target_date = get_beijing_time().strftime('%Y-%m-%d')
            today_stocks = new_stock_df[new_stock_df['申购日期'] == target_date]
            
            if not today_stocks.empty:
                return today_stocks
            elif test:
                # 测试模式下返回最近的一条数据
                return new_stock_df.head(1)
                
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"获取新股申购信息失败: {str(e)}")
        return None

def get_new_stock_listings(test=False):
    """获取当天新上市的股票信息"""
    try:
        import akshare as ak
        
        # 获取新股上市数据
        new_listing_df = ak.stock_new_share_listing(symbol="最新")
        
        if not new_listing_df.empty:
            # 转换日期格式并筛选
            new_listing_df['上市日期'] = pd.to_datetime(new_listing_df['上市日期']).dt.strftime('%Y-%m-%d')
            target_date = get_beijing_time().strftime('%Y-%m-%d')
            today_listings = new_listing_df[new_listing_df['上市日期'] == target_date]
            
            if not today_listings.empty:
                return today_listings
            elif test:
                # 测试模式下返回最近的一条数据
                return new_listing_df.head(1)
                
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"获取新上市股票信息失败: {str(e)}")
        return None

def format_new_stock_subscriptions_message(new_stocks):
    """格式化新股申购信息消息"""
    if new_stocks is None or new_stocks.empty:
        return "今天没有新股可认购"
    
    message = "【今日新股申购信息】\n"
    for _, stock in new_stocks.iterrows():
        message += f"• {stock.get('股票简称', '')} ({stock.get('股票代码', '')})\n"
        message += f"  发行价: {stock.get('发行价格', '未知')}元\n"
        message += f"  申购上限: {stock.get('申购上限', '未知')}股\n"
        message += f"  申购日期: {stock.get('申购日期', '未知')}\n\n"
    
    return message

def format_new_stock_listings_message(new_listings):
    """格式化新上市交易股票信息消息"""
    if new_listings is None or new_listings.empty:
        return "今天没有新上市股票可供交易"
    
    message = "【今日新上市股票信息】\n"
    for _, stock in new_listings.iterrows():
        message += f"• {stock.get('股票简称', '')} ({stock.get('股票代码', '')})\n"
        message += f"  发行价: {stock.get('发行价格', '未知')}元\n"
        message += f"  上市日期: {stock.get('上市日期', '未知')}\n\n"
      
    return message

def send_wecom_message(content):
    """发送消息到企业微信"""
    webhook = os.getenv('WECOM_WEBHOOK', Config.WECOM_WEBHOOK)
    
    if not webhook:
        logger.error("企业微信Webhook未配置")
        return False
    
    try:
        headers = {'Content-Type': 'application/json'}
        data = {
            "msgtype": "text",
            "text": {
                "content": content
            }
        }
        
        response = requests.post(webhook, headers=headers, data=json.dumps(data))
        result = response.json()
        
        if result.get('errcode') == 0:
            logger.info("消息成功发送到企业微信")
            return True
        else:
            logger.error(f"发送消息失败: {result.get('errmsg')}")
            return False
    except Exception as e:
        logger.error(f"发送消息时发生错误: {str(e)}")
        return False

def read_new_stock_pushed_flag(date):
    """检查新股信息是否已推送"""
    flag_file = f"data/new_stock_pushed_{date.strftime('%Y%m%d')}.txt"
    try:
        # 检查文件是否存在
        if os.path.exists(flag_file):
            return flag_file, True
        return flag_file, False
    except Exception as e:
        logger.warning(f"检查新股推送标记时出错: {str(e)}")
        return flag_file, False

def mark_new_stock_info_pushed():
    """标记新股信息已推送"""
    try:
        # 创建data目录（如果不存在）
        if not os.path.exists('data'):
            os.makedirs('data')
            
        flag_file, _ = read_new_stock_pushed_flag(get_beijing_time().date())
        with open(flag_file, 'w') as f:
            f.write(f"Pushed at {get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')}")
        return True
    except Exception as e:
        logger.error(f"标记新股信息已推送失败: {str(e)}")
        return False

def read_listing_pushed_flag(date):
    """检查新上市股票信息是否已推送"""
    flag_file = f"data/listing_pushed_{date.strftime('%Y%m%d')}.txt"
    try:
        if os.path.exists(flag_file):
            return flag_file, True
        return flag_file, False
    except Exception as e:
        logger.warning(f"检查上市信息推送标记时出错: {str(e)}")
        return flag_file, False

def mark_listing_info_pushed():
    """标记新上市股票信息已推送"""
    try:
        if not os.path.exists('data'):
            os.makedirs('data')
            
        flag_file, _ = read_listing_pushed_flag(get_beijing_time().date())
        with open(flag_file, 'w') as f:
            f.write(f"Pushed at {get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')}")
        return True
    except Exception as e:
        logger.error(f"标记上市信息已推送失败: {str(e)}")
        return False

def push_new_stock_info(test=False):
    """推送当天新股信息到企业微信"""
    # 检查是否已经推送过
    if not test:
        flag_path, is_pushed = read_new_stock_pushed_flag(get_beijing_time().date())
        if is_pushed:
            logger.info("今天已经推送过新股信息，跳过")
            return True
    
    new_stocks = get_new_stock_subscriptions(test=test)
    if new_stocks is None or new_stocks.empty:
        message = "今天没有新股可认购"
    else:
        message = format_new_stock_subscriptions_message(new_stocks)
    
    if test:
        message = "【测试消息】" + message
    
    success = send_wecom_message(message)
    
    # 标记已推送
    if success and not test:
        mark_new_stock_info_pushed()
    
    return success

def push_listing_info(test=False):
    """推送当天新上市交易的新股信息到企业微信"""
    # 检查是否已经推送过
    if not test:
        flag_path, is_pushed = read_listing_pushed_flag(get_beijing_time().date())
        if is_pushed:
            logger.info("今天已经推送过新上市交易信息，跳过")
            return True
    
    new_listings = get_new_stock_listings(test=test)
    if new_listings is None or new_listings.empty:
        message = "今天没有新上市股票可供交易"
    else:
        message = format_new_stock_listings_message(new_listings)
    
    if test:
        message = "【测试消息】" + message
    
    success = send_wecom_message(message)
    
    # 标记已推送
    if success and not test:
        mark_listing_info_pushed()
    
    return success

def main():
    """主函数"""
    # 从环境变量获取任务类型
    task = os.getenv('TASK', 'test_message')
    
    logger.info(f"执行任务: {task}")
    
    # 根据任务类型执行不同操作
    if task == 'push_new_stock':
        # 检查是否为交易日
        if not is_trading_day():
            logger.info("今天不是交易日，跳过新股信息推送")
            response = {"status": "skipped", "message": "Not trading day"}
            print(json.dumps(response, indent=2))
            return response
        
        # 推送新股申购信息
        success_new_stock = push_new_stock_info()
        
        # 推送新上市交易股票信息
        success_listing = push_listing_info()
        
        response = {
            "status": "success" if success_new_stock and success_listing else "partial_success",
            "new_stock": "success" if success_new_stock else "failed",
            "listing": "success" if success_listing else "failed"
        }
        print(json.dumps(response, indent=2))
        return response
    elif task == 'test_message':
        # 发送测试消息
        success = send_wecom_message("【测试】新股推送服务正常运行中")
        response = {"status": "success" if success else "failed", "message": "Test message sent"}
        print(json.dumps(response, indent=2))
        return response
    else:
        error_msg = f"【系统错误】未知任务类型: {task}"
        logger.error(error_msg)
        send_wecom_message(error_msg)
        response = {"status": "error", "message": "Unknown task type"}
        print(json.dumps(response, indent=2))
        return response

if __name__ == '__main__':
    main()
