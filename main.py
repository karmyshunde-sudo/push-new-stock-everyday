import os
import sys
import json
import logging
import requests
from datetime import time
from config import Config
from stock_data import (
    get_new_stock_subscriptions,
    get_new_stock_listings,
    read_new_stock_pushed_flag,
    mark_new_stock_info_pushed,
    read_listing_pushed_flag,
    mark_listing_info_pushed,
    get_beijing_time
)

# 初始化日志
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL, logging.INFO),
    format=Config.LOG_FORMAT,
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


# -------------------------
# 时间判断工具函数（仅保留时间检查）
# -------------------------
def is_in_trading_hours():
    """判断是否在交易时段（9:30-15:00）"""
    now = get_beijing_time()
    current_time = now.time()
    return time(9, 30) <= current_time <= time(15, 0)


def is_1430_deadline():
    """判断当前是否是14:30最终检查点"""
    now = get_beijing_time()
    current_time = now.time()
    # 允许±1分钟误差
    return time(14, 29) <= current_time <= time(14, 31)


# -------------------------
# 消息推送函数
# -------------------------
def send_wecom_message(message):
    """发送消息（自动添加末尾）"""
    wecom_webhook = os.getenv("WECOM_WEBHOOK", Config.WECOM_WEBHOOK)
    if not wecom_webhook:
        logger.error("企业微信Webhook未配置！")
        return False

    try:
        message_with_footer = f"{message}\n\n{Config.WECOM_MESFOOTER}"
        payload = {"msgtype": "text", "text": {"content": message_with_footer}}
        response = requests.post(wecom_webhook, json=payload)
        response.raise_for_status()
        return response.json().get("errcode") == 0
    except Exception as e:
        logger.error(f"消息发送失败: {str(e)}")
        return False


def send_force_alert():
    """14:30最终失败时发送强制提醒"""
    alert_msg = (
        "【紧急提醒】\n"
        "今天获取新股消息失败，可能存在未抓取到的新股申购信息！\n"
        "请尽快手动查看，避免错失申购机会！"
    )
    return send_wecom_message(alert_msg)


# -------------------------
# 消息格式化
# -------------------------
def format_new_stock_subscriptions_message(new_stocks_df):
    if new_stocks_df is None or new_stocks_df.empty:
        return "【今日新股申购信息】\n今天没有可申购的新股哦～"

    message = "【今日新股申购信息】\n"
    for idx, (_, stock) in enumerate(new_stocks_df.iterrows(), 1):
        message += f"""
{idx}. {stock['股票简称']}（代码：{stock['股票代码']}）
   • 发行价格：{stock['发行价格']}元
   • 申购上限：{stock['申购上限']}
   • 申购日期：{stock['申购日期']}
"""
    message += "\n温馨提示：请确认申购资格后操作，投资有风险～"
    return message


def format_new_stock_listings_message(new_listings_df):
    if new_listings_df is None or new_listings_df.empty:
        return "【今日新上市股票信息】\n今天没有新上市的股票哦～"

    message = "【今日新上市股票信息】\n"
    for idx, (_, stock) in enumerate(new_listings_df.iterrows(), 1):
        message += f"""
{idx}. {stock['股票简称']}（代码：{stock['股票代码']}）
   • 发行价格：{stock['发行价格']}元
   • 上市日期：{stock['上市日期']}
"""
    message += "\n温馨提示：新上市股票波动较大，请注意风险～"
    return message


# -------------------------
# 核心推送逻辑
# -------------------------
def push_new_stock_info(test_mode=False, force=False):
    """推送新股申购信息（返回是否成功）"""
    today = get_beijing_time().date()
    _, is_pushed = read_new_stock_pushed_flag(today)

    if test_mode or force or not is_pushed:
        logger.info(f"{'[测试]' if test_mode else ''} 开始爬取新股申购信息")
        new_stocks_df = get_new_stock_subscriptions(test_mode=test_mode or force)
        message = "[测试消息] " + format_new_stock_subscriptions_message(new_stocks_df) if test_mode else format_new_stock_subscriptions_message(new_stocks_df)
        send_success = send_wecom_message(message)
        
        if send_success and not test_mode:
            mark_new_stock_info_pushed()
            logger.info("新股信息推送成功并标记")
        return send_success
    else:
        logger.info("新股信息今日已推送，跳过")
        return True  # 已推送视为成功


def push_listing_info(test_mode=False, force=False):
    """推送新上市信息（返回是否成功）"""
    today = get_beijing_time().date()
    _, is_pushed = read_listing_pushed_flag(today)

    if test_mode or force or not is_pushed:
        logger.info(f"{'[测试]' if test_mode else ''} 开始爬取新上市信息")
        new_listings_df = get_new_stock_listings(test_mode=test_mode or force)
        message = "[测试消息] " + format_new_stock_listings_message(new_listings_df) if test_mode else format_new_stock_listings_message(new_listings_df)
        send_success = send_wecom_message(message)
        
        if send_success and not test_mode:
            mark_listing_info_pushed()
            logger.info("新上市信息推送成功并标记")
        return send_success
    else:
        logger.info("新上市信息今日已推送，跳过")
        return True  # 已推送视为成功


# -------------------------
# 测试任务专用函数
# -------------------------
def run_test_task():
    """测试任务：跳过所有检查，直接获取并推送新股信息"""
    logger.info("===== 测试任务开始 =====")
    logger.info("跳过所有检查，直接获取新股信息")
    
    # 强制获取并推送新股申购信息
    stock_success = push_new_stock_info(test_mode=True, force=True)
    
    # 强制获取并推送新上市信息
    listing_success = push_listing_info(test_mode=True, force=True)
    
    # 输出结果
    now = get_beijing_time()
    response = {
        "status": "success" if stock_success and listing_success else "partial_success",
        "details": {
            "new_stock": "success" if stock_success else "failed",
            "listing": "success" if listing_success else "failed"
        },
        "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
        "message": "测试任务完成"
    }
    print(json.dumps(response, indent=2, ensure_ascii=False))
    return response


# -------------------------
# 主入口（移除交易日判断）
# -------------------------
def main():
    task_type = os.getenv("TASK", "push_new_stock")
    test_mode = os.getenv("TEST_MODE", "false").lower() == "true"
    now = get_beijing_time()
    today = now.date()

    logger.info(f"===== 任务开始 =====")
    logger.info(f"当前时间: {now.strftime('%Y-%m-%d %H:%M:%S')}（北京时间）")
    logger.info(f"任务类型: {task_type} | 测试模式: {test_mode}")
    logger.info(f"是否交易时段: {is_in_trading_hours()} | 是否14:30检查点: {is_1430_deadline()}")
    logger.info(f"====================")

    # 如果是测试定时任务，直接运行测试任务
    if test_mode and task_type == "push_new_stock":
        return run_test_task()

    # 执行推送（不再判断是否为交易日）
    stock_success = push_new_stock_info(test_mode=test_mode)
    listing_success = push_listing_info(test_mode=test_mode)

    # 14:30最终检查：若仍失败则发送强制提醒
    if not test_mode and is_1430_deadline():
        # 检查是否仍未推送成功
        _, stock_pushed = read_new_stock_pushed_flag(today)
        _, listing_pushed = read_listing_pushed_flag(today)
        
        if not stock_pushed or not listing_pushed:
            logger.warning("14:30最终检查：仍有信息未推送成功，发送强制提醒")
            send_force_alert()

    # 输出结果
    response = {
        "status": "success" if stock_success and listing_success else "partial_success",
        "details": {
            "new_stock": "success" if stock_success else "failed",
            "listing": "success" if listing_success else "failed"
        },
        "timestamp": now.strftime("%Y-%m-%d %H:%M:%S")
    }
    print(json.dumps(response, indent=2, ensure_ascii=False))
    return response


if __name__ == "__main__":
    main()
