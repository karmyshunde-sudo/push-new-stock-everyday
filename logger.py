import logging
from config import Config

def get_logger(name):
    """获取配置好的日志器"""
    logger = logging.getLogger(name)
    logger.setLevel(Config.LOG_LEVEL)
    
    # 避免重复添加处理器
    if logger.handlers:
        return logger
    
    # 创建控制台处理器
    handler = logging.StreamHandler()
    handler.setLevel(Config.LOG_LEVEL)
    
    # 创建格式化器
    formatter = logging.Formatter(Config.LOG_FORMAT)
    handler.setFormatter(formatter)
    
    # 添加处理器到日志器
    logger.addHandler(handler)
    
    return logger
