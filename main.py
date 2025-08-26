import os
import sys
import time
import pandas as pd
import numpy as np
import datetime
import pytz
import shutil
import requests
import subprocess
import json
from flask import Flask, request, jsonify, has_app_context
from config import Config
from logger import get_logger
from bs4 import BeautifulSoup


app = Flask(__name__)
logger = get_logger(__name__)

def format_new_stock_subscriptions_message(new_stocks):
    """格式化新股申购信息消息"""
    if new_stocks is None or new_stocks.empty:
        return "今天没有新股、新债、新债券可认购"
    
    message = "【今日新股申购push_newstock_everyday】\n"
    for _, stock in new_stocks.iterrows():
        message += f"• {stock.get('股票简称', '')} ({stock.get('股票代码', '')})\n"
        message += f"  发行价: {stock.get('发行价格', '未知')}\n"
        message += f"  申购上限: {stock.get('申购上限', '未知')}\n"
        message += f"  申购日期: {stock.get('申购日期', '未知')}\n\n"
    
    return message

def format_new_stock_listings_message(new_listings):
    """格式化新上市交易股票信息消息"""
    if new_listings is None or new_listings.empty:
        return "今天没有新上市股票、可转债、债券可供交易"
    
    message = "【今日新上市交易push_newstock_everyday】\n"
    for _, stock in new_listings.iterrows():
        message += f"• {stock.get('股票简称', '')} ({stock.get('股票代码', '')})\n"
        message += f"  发行价: {stock.get('发行价格', '未知')}\n"
        message += f"  上市日期: {stock.get('上市日期', '未知')}\n\n"
      
    return message

def push_new_stock_info(test=False):
    """推送当天新股信息到企业微信
    参数:
        test: 是否为测试模式
    返回:
        bool: 是否成功"""
    # 检查是否已经推送过
    if not test:
        flag_path, is_pushed = read_new_stock_pushed_flag(get_beijing_time().date())
        if is_pushed:
            logger.info("今天已经推送过新股信息，跳过")
            return True
    
    new_stocks = get_new_stock_subscriptions(test=test)
    if new_stocks is None or new_stocks.empty:
        message = "今天没有新股、新债、新债券可认购"
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
    """推送当天新上市交易的新股信息到企业微信
    参数:
        test: 是否为测试模式
    返回:
        bool: 是否成功"""
    # 检查是否已经推送过
    if not test:
        flag_path, is_pushed = read_listing_pushed_flag(get_beijing_time().date())
        if is_pushed:
            logger.info("今天已经推送过新上市交易信息，跳过")
            return True
    
    new_listings = get_new_stock_listings(test=test)
    if new_listings is None or new_listings.empty:
        message = "今天没有新上市股票、可转债、债券可供交易"
    else:
        message = format_new_stock_listings_message(new_listings)
    
    if test:
        message = "【测试消息】" + message
    
    success = send_wecom_message(message)
    
    # 标记已推送
    if success and not test:
        mark_listing_info_pushed()
    
    return success


@app.route('/cron/push_new_stock', methods=['GET', 'POST'])
def cron_push_new_stock():
    """定时推送新股信息（当天可申购的新股）和新上市交易股票信息"""
    logger.info("新股信息与新上市交易股票信息推送任务触发")
    
    # 检查是否为交易日
    if not is_trading_day():
        logger.info("今天不是交易日，跳过新股信息推送")
        response = {"status": "skipped", "message": "Not trading day"}
        return jsonify(response) if has_app_context() else response
    
    # 检查是否已经推送过
    flag_path, is_pushed = read_new_stock_pushed_flag(get_beijing_time().date())
    if is_pushed:
        logger.info("今天已经推送过新股信息，跳过")
        response = {
            "status": "skipped",
            "message": "New stock info already pushed today"
        }
        return jsonify(response) if has_app_context() else response
    
    # 推送新股申购信息
    success_new_stock = push_new_stock_info()
    
    # 推送新上市交易股票信息
    success_listing = push_listing_info()
    
    response = {
        "status": "success" if success_new_stock and success_listing else "partial_success",
        "new_stock": "success" if success_new_stock else "failed",
        "listing": "success" if success_listing else "failed"
    }
    return jsonify(response) if has_app_context() else response

def main():
    """主函数"""
    # 从环境变量获取任务类型
    task = os.getenv('TASK', 'test_message')
    
    logger.info(f"执行任务: {task}")
    
    # 根据任务类型执行不同操作
    if task == 'push_new_stock':
        # 9:36 AM：爬取并推送当天新股申购、新上市股票信息
        success_new_stock = push_new_stock_info()
        success_listing = push_listing_info()
        response = {
            "status": "success" if success_new_stock and success_listing else "partial_success",
            "new_stock": "success" if success_new_stock else "failed",
            "listing": "success" if success_listing else "failed"
        }
        print(json.dumps(response, indent=2))
        return response
  
    else:
        error_msg = f"【push_newstock_everyday系统错误】未知任务类型: {task}"
        logger.error(error_msg)
        send_wecom_message(error_msg)
        response = {"status": "error", "message": "Unknown task type"}
        print(json.dumps(response, indent=2))
        return response

if __name__ == '__main__':
    # 如果作为Flask应用运行
    if len(sys.argv) > 1 and sys.argv[1] == 'flask':
        app.run(host='0.0.0.0', port=int(os.getenv('PORT', 8080)))
    else:
        # 作为命令行任务运行
        main()
