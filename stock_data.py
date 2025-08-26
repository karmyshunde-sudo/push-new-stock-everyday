import akshare as ak
import pandas as pd
import datetime
import pytz
import os
import logging
from retrying import retry  # 接口重试装饰器

# 初始化日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


# -------------------------
# 工具函数
# -------------------------
@retry(stop_max_attempt_number=3, wait_fixed=2000)
def akshare_retry(func, **kwargs):
    """AkShare接口重试封装"""
    return func(** kwargs)


def get_beijing_time():
    """获取当前北京时间（带时区）"""
    tz_shanghai = pytz.timezone("Asia/Shanghai")
    return datetime.datetime.now(tz_shanghai)


# -------------------------
# 数据完整性校验
# -------------------------
def check_new_stock_completeness(df):
    """校验新股申购数据完整性"""
    required_cols = ["股票代码", "股票简称", "发行价格", "申购上限", "申购日期"]
    return all(
        col in df.columns and not df[col].isnull().all()
        for col in required_cols
    )


def check_new_listing_completeness(df):
    """校验新上市股票数据完整性"""
    required_cols = ["股票代码", "股票简称", "发行价格", "上市日期"]
    return all(
        col in df.columns and not df[col].isnull().all()
        for col in required_cols
    )


# -------------------------
# 核心爬取函数
# -------------------------
def get_new_stock_subscriptions(test_mode=False):
    """获取新股申购信息"""
    try:
        today = get_beijing_time().date()
        logger.info(f"{'[测试模式]' if test_mode else '[正常模式]'} 开始获取新股申购信息（目标日期：{today}）")

        # 生成待尝试日期列表
        if test_mode:
            dates_to_try = [today - datetime.timedelta(days=i) for i in range(0, 22)]
        else:
            dates_to_try = [today]

        # 遍历日期尝试获取数据
        for date_obj in dates_to_try:
            date_str = date_obj.strftime("%Y-%m-%d")
            logger.info(f"尝试获取 {date_str} 的新股申购数据")

            try:
                raw_df = akshare_retry(ak.stock_xgsglb_em)
                if raw_df.empty:
                    logger.warning(f"{date_str} AkShare返回空数据，跳过")
                    continue

                logger.debug(f"原始数据列名: {raw_df.columns.tolist()} | 数据行数: {len(raw_df)}")

                # 动态匹配日期列
                date_col = next(
                    (col for col in raw_df.columns
                     if any(kw in col.lower() for kw in ["申购日期", "ipo_date", "issue_date"])),
                    None
                )
                if not date_col:
                    logger.warning(f"{date_str} 未找到日期列，跳过")
                    continue

                # 标准化日期格式
                if not pd.api.types.is_datetime64_any_dtype(raw_df[date_col]):
                    try:
                        raw_df[date_col] = pd.to_datetime(raw_df[date_col]).dt.strftime("%Y-%m-%d")
                    except Exception as e:
                        logger.error(f"{date_str} 日期格式转换失败: {str(e)}")
                        continue

                # 筛选当前尝试日期的数据
                target_df = raw_df[raw_df[date_col] == date_str].copy()
                if target_df.empty:
                    logger.info(f"{date_str} 无新股申购数据，继续尝试其他日期")
                    continue

                # 动态匹配核心字段
                code_col = next((col for col in raw_df.columns if "代码" in col or "code" in col.lower()), None)
                name_col = next((col for col in raw_df.columns if "名称" in col or "简称" in col or "name" in col.lower()), None)
                price_col = next((col for col in raw_df.columns if "价格" in col or "price" in col.lower()), None)
                limit_col = next((col for col in raw_df.columns if "上限" in col or "limit" in col.lower()), None)

                if not (code_col and name_col):
                    logger.warning(f"{date_str} 缺少股票代码/简称列，跳过")
                    continue

                # 构建标准化结果表
                result_df = target_df[[code_col, name_col]].rename(
                    columns={code_col: "股票代码", name_col: "股票简称"}
                )
                result_df["发行价格"] = target_df.get(price_col, "未知")
                result_df["申购上限"] = target_df.get(limit_col, "未知")
                result_df["申购日期"] = date_str
                result_df["类型"] = "股票"

                if check_new_stock_completeness(result_df):
                    logger.info(f"成功获取 {date_str} 新股申购数据（{len(result_df)}条）")
                    return result_df[["股票代码", "股票简称", "发行价格", "申购上限", "申购日期", "类型"]]
                else:
                    logger.warning(f"{date_str} 数据不完整，跳过")

            except Exception as e:
                logger.error(f"{date_str} AkShare接口调用失败: {str(e)}", exc_info=True)

        logger.info(f"未找到有效新股申购数据")
        return pd.DataFrame()

    except Exception as e:
        error_msg = f"获取新股申购信息异常: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return pd.DataFrame()


def get_new_stock_listings(test_mode=False):
    """获取新上市股票信息"""
    try:
        today = get_beijing_time().date()
        logger.info(f"{'[测试模式]' if test_mode else '[正常模式]'} 开始获取新上市股票信息（目标日期：{today}）")

        # 生成待尝试日期列表
        if test_mode:
            dates_to_try = [today - datetime.timedelta(days=i) for i in range(0, 22)]
        else:
            dates_to_try = [today]

        # 遍历日期尝试获取数据
        for date_obj in dates_to_try:
            date_str = date_obj.strftime("%Y-%m-%d")
            logger.info(f"尝试获取 {date_str} 的新上市股票数据")

            try:
                raw_df = akshare_retry(ak.stock_xgsglb_em)
                if raw_df.empty:
                    logger.warning(f"{date_str} AkShare返回空数据，跳过")
                    continue

                logger.debug(f"原始数据列名: {raw_df.columns.tolist()} | 数据行数: {len(raw_df)}")

                # 动态匹配上市日期列
                listing_date_col = next(
                    (col for col in raw_df.columns
                     if any(kw in col.lower() for kw in ["上市日期", "listing_date"])),
                    None
                )
                if not listing_date_col:
                    logger.warning(f"{date_str} 未找到上市日期列，跳过")
                    continue

                # 标准化日期格式
                if not pd.api.types.is_datetime64_any_dtype(raw_df[listing_date_col]):
                    try:
                        raw_df[listing_date_col] = pd.to_datetime(raw_df[listing_date_col]).dt.strftime("%Y-%m-%d")
                    except Exception as e:
                        logger.error(f"{date_str} 日期格式转换失败: {str(e)}")
                        continue

                # 筛选当前日期数据
                target_df = raw_df[raw_df[listing_date_col] == date_str].copy()
                if target_df.empty:
                    logger.info(f"{date_str} 无新上市股票数据，继续尝试其他日期")
                    continue

                # 动态匹配核心字段
                code_col = next((col for col in raw_df.columns if "代码" in col or "code" in col.lower()), None)
                name_col = next((col for col in raw_df.columns if "名称" in col or "简称" in col or "name" in col.lower()), None)
                price_col = next((col for col in raw_df.columns if "价格" in col or "price" in col.lower()), None)

                if not (code_col and name_col):
                    logger.warning(f"{date_str} 缺少股票代码/简称列，跳过")
                    continue

                # 构建标准化结果表
                result_df = target_df[[code_col, name_col]].rename(
                    columns={code_col: "股票代码", name_col: "股票简称"}
                )
                result_df["发行价格"] = target_df.get(price_col, "未知")
                result_df["上市日期"] = date_str
                result_df["类型"] = "股票"

                if check_new_listing_completeness(result_df):
                    logger.info(f"成功获取 {date_str} 新上市股票数据（{len(result_df)}条）")
                    return result_df[["股票代码", "股票简称", "发行价格", "上市日期", "类型"]]
                else:
                    logger.warning(f"{date_str} 数据不完整，跳过")

            except Exception as e:
                logger.error(f"{date_str} AkShare接口调用失败: {str(e)}", exc_info=True)

        logger.info(f"未找到有效新上市股票数据")
        return pd.DataFrame()

    except Exception as e:
        error_msg = f"获取新上市股票信息异常: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return pd.DataFrame()


# -------------------------
# 防重复推送标记
# -------------------------
def read_new_stock_pushed_flag(date):
    """检查新股申购信息是否已推送"""
    flag_dir = "data/flags"
    os.makedirs(flag_dir, exist_ok=True)
    flag_file = os.path.join(flag_dir, f"new_stock_pushed_{date.strftime('%Y%m%d')}.txt")
    return flag_file, os.path.exists(flag_file)


def mark_new_stock_info_pushed():
    """标记新股申购信息已推送"""
    flag_dir = "data/flags"
    os.makedirs(flag_dir, exist_ok=True)
    flag_file = os.path.join(flag_dir, f"new_stock_pushed_{get_beijing_time().strftime('%Y%m%d')}.txt")
    with open(flag_file, "w", encoding="utf-8") as f:
        f.write(f"Pushed at {get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"新股申购信息推送标记已创建: {flag_file}")
    return flag_file


def read_listing_pushed_flag(date):
    """检查新上市股票信息是否已推送"""
    flag_dir = "data/flags"
    os.makedirs(flag_dir, exist_ok=True)
    flag_file = os.path.join(flag_dir, f"listing_pushed_{date.strftime('%Y%m%d')}.txt")
    return flag_file, os.path.exists(flag_file)


def mark_listing_info_pushed():
    """标记新上市股票信息已推送"""
    flag_dir = "data/flags"
    os.makedirs(flag_dir, exist_ok=True)
    flag_file = os.path.join(flag_dir, f"listing_pushed_{get_beijing_time().strftime('%Y%m%d')}.txt")
    with open(flag_file, "w", encoding="utf-8") as f:
        f.write(f"Pushed at {get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"新上市股票信息推送标记已创建: {flag_file}")
    return flag_file
