#!/usr/bin/env python
# -*- coding:utf-8 -*-
import os
import sys
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from sqlmodel import create_engine
from utils.logger_util import get_logger

# 创建日志引擎
logger = get_logger()

# 创建数据库引擎
engine = create_engine("mysql+pymysql://llmmodel:llmmodelNsRMSKC1@10.10.39.86:3306/finchinadb",
                       echo=False,
                       pool_pre_ping=True,  # 每次使用前 ping 一下，自动重连
                       pool_recycle=3600,  # 可选：每小时重建连接，避免超时
                       pool_size=10,  # 连接池大小
                       max_overflow=10  # 超出池大小的临时连接数
                       )
