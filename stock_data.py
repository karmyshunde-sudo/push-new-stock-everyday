import akshare as ak
import pandas as pd
import datetime
import pytz
import os
import logging
from retrying import retry  # 需确保 requirements.txt 包含 retrying

# 初始化日志
logger = logging.getLogger(__name__)

# 重试装饰器（解决网络波动问题）
@retry(stop_max_attempt_number=3, wait_fixed=2000)
def akshare_retry(func, **kwargs):
    """AkShare 接口重试封装"""
    return func(**kwargs)

def get_beijing_time():
    """获取当前北京时间"""
    tz = pytz.timezone('Asia/Shanghai')
    return datetime.datetime.now(tz)

def is_trading_day(date=None):
    """
    判断指定日期是否为交易日（基于 AkShare 日历）
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
        
        target_date = pd.to_datetime(date)
        is_open = trade_cal_df[trade_cal_df['trade_date'] == target_date]['is_open'].values
        
        return len(is_open) > 0 and is_open[0] == 1
    except Exception as e:
        logger.error(f"判断交易日出错: {str(e)}")
        # 降级逻辑：非周末默认视为交易日
        return date.weekday() < 5  # 0-4 为周一至周五

def check_new_stock_completeness(df):
    """检查新股申购数据完整性（核心字段非空）"""
    required_cols = ['股票代码', '股票简称', '发行价格', '申购上限', '申购日期']
    return all(col in df.columns and not df[col].isnull().all() for col in required_cols)

def check_new_listing_completeness(df):
    """检查新上市股票数据完整性（核心字段非空）"""
    required_cols = ['股票代码', '股票简称', '发行价格', '上市日期']
    return all(col in df.columns and not df[col].isnull().all() for col in required_cols)

def get_new_stock_subscriptions(test=False):
    """
    获取新股申购信息（仅用 AkShare，测试模式回溯 21 天）
    :param test: 是否为测试模式（测试模式回溯 21 天找数据）
    :return: pd.DataFrame 新股申购数据（空表表示无数据）
    """
    try:
        today = get_beijing_time().date()
        logger.info(f"{'测试模式' if test else '正常模式'}: 尝试获取 {today} 的新股申购信息...")
        
        # 测试模式：生成回溯 21 天的日期列表
        dates_to_try = [today - datetime.timedelta(days=i) for i in range(0, 22)] if test else [today]
        
        for date_obj in dates_to_try:
            date_str = date_obj.strftime('%Y-%m-%d')
            logger.info(f"尝试日期: {date_str}")
            
            # 调用 AkShare 接口（带重试）
            try:
                df = akshare_retry(ak.stock_xgsglb_em)  # 新股申购列表接口
                if df.empty:
                    logger.warning("AkShare 返回空数据，跳过当前日期")
                    continue
                
                # 日志记录原始数据结构（仅列名、行数）
                logger.info(f"AkShare 返回列数: {len(df.columns)} | 行数: {len(df)} | 列名: {df.columns.tolist()}")
                
                # 动态匹配日期列（兼容不同接口返回的列名）
                date_col = next(
                    (col for col in df.columns 
                     if any(kw in col.lower() for kw in ['申购日期', 'ipo_date', 'issue_date'])
                    ), None
                )
                if not date_col:
                    logger.warning("未找到日期列，跳过当前日期数据")
                    continue
                
                # 日期格式标准化
                if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
                    try:
                        df[date_col] = pd.to_datetime(df[date_col]).dt.strftime('%Y-%m-%d')
                    except Exception as e:
                        logger.error(f"日期格式转换失败: {str(e)}")
                        continue
                
                # 筛选目标日期数据
                target_df = df[df[date_col] == date_str].copy()
                if target_df.empty:
                    logger.info(f"{date_str} 无匹配数据，继续尝试其他日期")
                    continue
                
                # 动态修复核心列名
                code_col = next(
                    (col for col in df.columns 
                     if any(kw in col.lower() for kw in ['代码', 'code'])
                    ), None
                )
                name_col = next(
                    (col for col in df.columns 
                     if any(kw in col.lower() for kw in ['名称', 'name', '简称'])
                    ), None
                )
                price_col = next(
                    (col for col in df.columns 
                     if any(kw in col.lower() for kw in ['价格', 'price'])
                    ), None
                )
                limit_col = next(
                    (col for col in df.columns 
                     if any(kw in col.lower() for kw in ['上限', 'limit'])
                    ), None
                )
                
                # 核心列校验
                if not (code_col and name_col):
                    logger.warning("缺少股票代码/简称列，跳过当前日期数据")
                    continue
                
                # 构建结果表
                result_df = target_df[[code_col, name_col]].rename(
                    columns={code_col: '股票代码', name_col: '股票简称'}
                )
                if price_col:
                    result_df['发行价格'] = target_df[price_col]
                if limit_col:
                    result_df['申购上限'] = target_df[limit_col]
                result_df['申购日期'] = date_str
                result_df['类型'] = '股票'
                
                # 数据完整性校验
                if check_new_stock_completeness(result_df):
                    logger.info(f"成功获取 {len(result_df)} 条新股申购数据（日期: {date_str}）")
                    return result_df[['股票代码', '股票简称', '发行价格', '申购上限', '申购日期', '类型']]
                else:
                    logger.warning("数据不完整，跳过当前日期数据")
            
            except Exception as e:
                logger.error(f"AkShare 接口调用失败: {str(e)}", exc_info=True)
        
        logger.info("遍历所有日期后，未找到有效新股申购数据")
        return pd.DataFrame()
    
    except Exception as e:
        error_msg = f"【数据错误】获取新股申购信息失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        # 若需要，可在这里调用 send_wecom_message（需确保依赖注入）
        return pd.DataFrame()

def get_new_stock_listings(test=False):
    """
    获取新上市股票信息（仅用 AkShare，测试模式回溯 21 天）
    :param test: 是否为测试模式（测试模式回溯 21 天找数据）
    :return: pd.DataFrame 新上市股票数据（空表表示无数据）
    """
    try:
        today = get_beijing_time().date()
        logger.info(f"{'测试模式' if test else '正常模式'}: 尝试获取 {today} 的新上市交易信息...")
        
        # 测试模式：生成回溯 21 天的日期列表
        dates_to_try = [today - datetime.timedelta(days=i) for i in range(0, 22)] if test else [today]
        
        for date_obj in dates_to_try:
            date_str = date_obj.strftime('%Y-%m-%d')
            logger.info(f"尝试日期: {date_str}")
            
            # 调用 AkShare 接口（带重试）
            try:
                df = akshare_retry(ak.stock_xgsglb_em)  # 新股上市列表接口（与申购复用同一接口，靠日期区分）
                if df.empty:
                    logger.warning("AkShare 返回空数据，跳过当前日期")
                    continue
                
                # 日志记录原始数据结构（仅列名、行数）
                logger.info(f"AkShare 返回列数: {len(df.columns)} | 行数: {len(df)} | 列名: {df.columns.tolist()}")
                
                # 动态匹配上市日期列（兼容不同接口返回的列名）
                listing_date_col = next(
                    (col for col in df.columns 
                     if any(kw in col.lower() for kw in ['上市日期', 'listing_date'])
                    ), None
                )
                if not listing_date_col:
                    logger.warning("未找到上市日期列，跳过当前日期数据")
                    continue
                
                # 日期格式标准化
                if not pd.api.types.is_datetime64_any_dtype(df[listing_date_col]):
                    try:
                        df[listing_date_col] = pd.to_datetime(df[listing_date_col]).dt.strftime('%Y-%m-%d')
                    except Exception as e:
                        logger.error(f"日期格式转换失败: {str(e)}")
                        continue
                
                # 筛选目标日期数据
                target_df = df[df[listing_date_col] == date_str].copy()
                if target_df.empty:
                    logger.info(f"{date_str} 无匹配数据，继续尝试其他日期")
                    continue
                
                # 动态修复核心列名
                code_col = next(
                    (col for col in df.columns 
                     if any(kw in col.lower() for kw in ['代码', 'code'])
                    ), None
                )
                name_col = next(
                    (col for col in df.columns 
                     if any(kw in col.lower() for kw in ['名称', 'name', '简称'])
                    ), None
                )
                price_col = next(
                    (col for col in df.columns 
                     if any(kw in col.lower() for kw in ['价格', 'price'])
                    ), None
                )
                
                # 核心列校验
                if not (code_col and name_col):
                    logger.warning("缺少股票代码/简称列，跳过当前日期数据")
                    continue
                
                # 构建结果表
                result_df = target_df[[code_col, name_col]].rename(
                    columns={code_col: '股票代码', name_col: '股票简称'}
                )
                if price_col:
                    result_df['发行价格'] = target_df[price_col]
                result_df['上市日期'] = date_str
                result_df['类型'] = '股票'
                
                # 数据完整性校验
                if check_new_listing_completeness(result_df):
                    logger.info(f"成功获取 {len(result_df)} 条新上市股票数据（日期: {date_str}）")
                    return result_df[['股票代码', '股票简称', '发行价格', '上市日期', '类型']]
                else:
                    logger.warning("数据不完整，跳过当前日期数据")
            
            except Exception as e:
                logger.error(f"AkShare 接口调用失败: {str(e)}", exc_info=True)
        
        logger.info("遍历所有日期后，未找到有效新上市股票数据")
        return pd.DataFrame()
    
    except Exception as e:
        error_msg = f"【数据错误】获取新上市交易信息失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        # 若需要，可在这里调用 send_wecom_message（需确保依赖注入）
        return pd.DataFrame()

# -------------------------
# 以下为「防重复推送」标记逻辑（与数据源无关，保留）
# -------------------------
def read_new_stock_pushed_flag(date):
    """检查新股信息是否已推送"""
    flag_dir = "data/flags"
    os.makedirs(flag_dir, exist_ok=True)
    flag_file = os.path.join(flag_dir, f"new_stock_pushed_{date.strftime('%Y%m%d')}.txt")
    return flag_file, os.path.exists(flag_file)

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
    return flag_file, os.path.exists(flag_file)

def mark_listing_info_pushed():
    """标记新上市股票信息已推送"""
    flag_dir = "data/flags"
    os.makedirs(flag_dir, exist_ok=True)
    flag_file = os.path.join(flag_dir, f"listing_pushed_{get_beijing_time().strftime('%Y%m%d')}.txt")
    with open(flag_file, "w") as f:
        f.write(f"Pushed at {get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')}")
    return flag_file
