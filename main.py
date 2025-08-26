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

# 初始化日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


# -------------------------
# 企业微信消息推送
# -------------------------
def send_wecom_message(message):
    """
    发送文本消息到企业微信机器人
    :param message: 消息内容（str）
    :return: bool（True=发送成功，False=发送失败）
    """
    # 从环境变量获取Webhook（避免硬编码）
    wecom_webhook = os.getenv("WECOM_WEBHOOK")
    if not wecom_webhook:
        logger.error("企业微信Webhook未配置！请在GitHub Secrets中添加WECOM_WEBHOOK")
        return False

    try:
        # 企业微信机器人文本消息格式
        payload = {
            "msgtype": "text",
            "text": {
                "content": message,
                "mentioned_list": [],  # 可添加@的人（如["@all"]）
                "mentioned_mobile_list": []
            }
        }

        # 发送POST请求
        response = requests.post(
            url=wecom_webhook,
            json=payload,
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()  # 触发HTTP错误（如404、500）

        # 解析响应（企业微信返回errcode=0表示成功）
        result = response.json()
        if result.get("errcode") == 0:
            logger.info("企业微信消息发送成功")
            return True
        else:
            logger.error(f"企业微信消息发送失败: {result.get('errmsg')}（errcode: {result.get('errcode')}）")
            return False

    except Exception as e:
        logger.error(f"发送企业微信消息异常: {str(e)}", exc_info=True)
        return False


# -------------------------
# 消息格式化（美观易读）
# -------------------------
def format_new_stock_subscriptions_message(new_stocks_df):
    """
    格式化新股申购信息为文本
    :param new_stocks_df: 新股数据DataFrame
    :return: 格式化后的消息（str）
    """
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
    # 补充提示（可选）
    message += "\n温馨提示：请确认申购资格后操作，投资有风险～"
    return message


def format_new_stock_listings_message(new_listings_df):
    """格式化新上市股票信息为文本"""
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
# 推送逻辑（核心业务）
# -------------------------
def push_new_stock_info(test_mode=False):
    """
    推送新股申购信息（含防重复推送）
    :param test_mode: 是否测试模式（bool）
    :return: bool（True=推送成功/已推送，False=推送失败）
    """
    # 非测试模式：检查是否已推送（避免重复）
    if not test_mode:
        today = get_beijing_time().date()
        flag_file, is_pushed = read_new_stock_pushed_flag(today)
        if is_pushed:
            logger.info(f"新股申购信息今日已推送（标记文件：{flag_file}），跳过本次推送")
            return True

    # 获取新股数据
    new_stocks_df = get_new_stock_subscriptions(test_mode=test_mode)
    # 格式化消息（测试模式加前缀）
    if test_mode:
        message = "[测试消息] " + format_new_stock_subscriptions_message(new_stocks_df)
    else:
        message = format_new_stock_subscriptions_message(new_stocks_df)

    # 发送消息
    send_success = send_wecom_message(message)
    # 非测试模式：推送成功后标记
    if send_success and not test_mode:
        mark_new_stock_info_pushed()

    return send_success


def push_listing_info(test_mode=False):
    """推送新上市股票信息（含防重复推送）"""
    if not test_mode:
        today = get_beijing_time().date()
        flag_file, is_pushed = read_listing_pushed_flag(today)
        if is_pushed:
            logger.info(f"新上市股票信息今日已推送（标记文件：{flag_file}），跳过本次推送")
            return True

    new_listings_df = get_new_stock_listings(test_mode=test_mode)
    if test_mode:
        message = "[测试消息] " + format_new_stock_listings_message(new_listings_df)
    else:
        message = format_new_stock_listings_message(new_listings_df)

    send_success = send_wecom_message(message)
    if send_success and not test_mode:
        mark_listing_info_pushed()

    return send_success


# -------------------------
# 主入口（任务调度）
# -------------------------
def main():
    """主函数：根据环境变量执行对应任务"""
    # 从环境变量获取配置（由GitHub Actions传递）
    task_type = os.getenv("TASK", "push_new_stock")  # 任务类型
    test_mode = os.getenv("TEST_MODE", "false").lower() == "true"  # 测试模式标记

    logger.info(f"===== 任务开始 =====")
    logger.info(f"任务类型: {task_type} | 测试模式: {test_mode}")
    logger.info(f"当前北京时间: {get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"====================")

    # 执行对应任务
    if task_type in ["push_new_stock", "test_push"]:
        # 非测试模式：先判断是否为交易日
        if not test_mode and not is_trading_day():
            logger.info("今日非交易日，跳过新股信息推送")
            response = {
                "status": "skipped",
                "reason": "Not a trading day",
                "test_mode": test_mode,
                "timestamp": get_beijing_time().strftime("%Y-%m-%d %H:%M:%S")
            }
            print(json.dumps(response, ensure_ascii=False, indent=2))
            return response

        # 推送新股申购+新上市信息
        stock_push_success = push_new_stock_info(test_mode=test_mode)
        listing_push_success = push_listing_info(test_mode=test_mode)

        # 生成结果响应
        overall_status = "success" if stock_push_success and listing_push_success else "partial_success"
        response = {
            "status": overall_status,
            "details": {
                "new_stock_push": "success" if stock_push_success else "failed",
                "listing_push": "success" if listing_push_success else "failed"
            },
            "test_mode": test_mode,
            "timestamp": get_beijing_time().strftime("%Y-%m-%d %H:%M:%S")
        }
        logger.info(f"任务执行完成，结果: {overall_status}")
        print(json.dumps(response, ensure_ascii=False, indent=2))
        return response

    # 未知任务类型
    else:
        error_msg = f"未知任务类型: {task_type}（支持的类型：push_new_stock, test_push）"
        logger.error(error_msg)
        send_wecom_message(f"【系统错误】{error_msg}")
        response = {
            "status": "error",
            "message": error_msg,
            "timestamp": get_beijing_time().strftime("%Y-%m-%d %H:%M:%S")
        }
        print(json.dumps(response, ensure_ascii=False, indent=2))
        return response


if __name__ == "__main__":
    main()
