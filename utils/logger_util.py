#!/usr/bin/env python
# -*- coding:utf-8 -*-

from loguru import logger
import sys
from pathlib import Path
from config.config import PROJECT_ROOT

class LoggerUtil:
    """统一日志工具类，基于loguru"""

    _initialized = False
    DEFAULT_LOG_DIR = PROJECT_ROOT / "logs"  # 默认日志目录为项目根目录下的logs

    @classmethod
    def setup_logger(cls, log_level: str = "INFO", log_file: str = None):
        """
        配置全局日志器

        Args:
            log_level: 日志级别 (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            log_file: 日志文件路径，如果为None则使用默认路径 logs/audit_system.log
        """
        if cls._initialized:
            return logger

        # 移除默认的控制台处理器
        logger.remove()

        # 添加控制台输出
        logger.add(
            sys.stdout,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                   "<level>{level: <8}</level> | "
                   "<cyan>{file}:{line}</cyan> | "
                   "<level>{message}</level>",
            level=log_level,
            colorize=True
        )

        # 设置默认日志文件路径
        if not log_file:
            cls.DEFAULT_LOG_DIR.mkdir(parents=True, exist_ok=True)
            log_file = cls.DEFAULT_LOG_DIR / "app.log"

        # 添加文件输出
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        logger.add(
            log_file,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {file}:{line} | {message}",
            level=log_level,
            rotation="10 MB",  # 文件大小超过10MB时轮转
            retention="7 days",  # 保留7天的日志
            compression="zip",  # 压缩旧日志文件
            encoding="utf-8"
        )

        cls._initialized = True
        logger.debug("日志系统初始化完成")
        return logger

    @classmethod
    def get_logger(cls):
        """
        获取统一的日志器实例，不需要传入模块名

        Returns:
            logger实例
        """
        if not cls._initialized:
            cls.setup_logger()
        return logger


# 创建统一的日志器获取函数
def get_logger():
    """获取统一日志器的便捷函数，不需要传入模块名"""
    return LoggerUtil.get_logger()


if __name__ == "__main__":
    # 初始化项目日志
    logger = get_logger()

    # 测试日志功能
    logger.debug("这是一个调试信息")
    logger.info("这是一个普通信息")
    logger.warning("这是一个警告信息")
    logger.error("这是一个错误信息")
