import os
import sys
import json
import logging
import requests
from stock_data import (
    get_new_stock_subscriptions, 
    get_new_stock_listings,
    read_new_stock_pushed_flag,
    mark_new_stock_info_pushed,
    read_listing_pushed_flag,
    mark_listing_info_pushed,
    get_beijing_time,
    is_trading_day
)
from logger import get_logger

# 初始化日志
logger = get_logger(__name__)

def send_wecom_message(message):
    """发送消息到企业微信"""
    webhook = os.getenv('WECOM_WEBHOOK')
    if not webhook:
        logger.error("未配置企业微信Webhook")
        return False
    
    try:
        data = {
            "msgtype": "text",
            "text": {
                "content": message
            }
        }
        
        response = requests.post(webhook, json=data)
        response.raise_for_status()
        
        result = response.json()
        if result.get('errcode') == 0:
            logger.info("消息发送成功")
            return True
        else:
            logger.error(f"消息发送失败: {result.get('errmsg')}")
            return False
    except Exception as e:
        logger.error(f"发送企业微信消息出错: {str(e)}")
        return False

def format_new_stock_subscriptions_message(new_stocks):
    """格式化新股申购信息消息"""
    if new_stocks is None or new_stocks.empty:
        return "今天没有新股可认购"
    
    message = "【今日新股申购信息】\n"
    for _, stock in new_stocks.iterrows():
        message += f"• {stock.get('股票简称', '')} ({stock.get('股票代码', '')})\n"
        message += f"  发行价: {stock.get('发行价格', '未知')}\n"
        message += f"  申购上限: {stock.get('申购上限', '未知')}\n"
        message += f"  申购日期: {stock.get('申购日期', '未知')}\n\n"
    
    return message

def format_new_stock_listings_message(new_listings):
    """格式化新上市交易股票信息消息"""
    if new_listings is None or new_listings.empty:
        return "今天没有新上市股票可供交易"
    
    message = "【今日新上市股票信息】\n"
    for _, stock in new_listings.iterrows():
        message += f"• {stock.get('股票简称', '')} ({stock.get('股票代码', '')})\n"
        message += f"  发行价: {stock.get('发行价格', '未知')}\n"
        message += f"  上市日期: {stock.get('上市日期', '未知')}\n\n"
      
    return message

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
        success = send_wecom_message("【测试消息】新股推送服务工作正常")
        response = {"status": "success" if success else "failed", "message": "Test message sent"}
        print(json.dumps(response, indent=2))
        return response
  
    else:
        error_msg = f"未知任务类型: {task}"
        logger.error(error_msg)
        send_wecom_message(f"【系统错误】{error_msg}")
        response = {"status": "error", "message": "Unknown task type"}
        print(json.dumps(response, indent=2))
        return response

if __name__ == '__main__':
    main()
