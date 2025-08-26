class Config:
    """项目配置类"""
    # 默认的企业微信Webhook（实际使用中会被环境变量覆盖）
    WECOM_WEBHOOK = ""
    
    # 日志配置
    LOG_LEVEL = "INFO"
    LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
