#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Description：查询信用债股东信息

from pathlib import Path
import sys

project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))
from sqlmodel import Session, select
from typing import List
from structured.models.tq_shareholder_model import TQ_SK_SHAREHOLDER
from utils.logger_util import get_logger
from utils.db_util import engine

logger = get_logger()


def get_latest_shareholders(comp_code: str, session: Session) -> List[TQ_SK_SHAREHOLDER]:
    """
    查询公司最新股东名单（基于最新 UPDATEDATE）。

    :param comp_code: 公司代码
    :param session: 数据库会话
    :return: 最新股东名单列表
    :raises Exception: 未找到股东信息或查询错误
    """
    try:
        logger.info(f"查询公司代码 '{comp_code}' 的股东信息")

        results = session.exec(
            select(TQ_SK_SHAREHOLDER.SHHOLDERNAME, TQ_SK_SHAREHOLDER.SHHOLDERSECODE, TQ_SK_SHAREHOLDER.UPDATEDATE,
                   TQ_SK_SHAREHOLDER.HOLDERAMT, TQ_SK_SHAREHOLDER.HOLDERRTO)
            .where(TQ_SK_SHAREHOLDER.COMPCODE == comp_code)
            .where(TQ_SK_SHAREHOLDER.UPDATEDATE == (
                select(TQ_SK_SHAREHOLDER.UPDATEDATE)
                .where(TQ_SK_SHAREHOLDER.COMPCODE == comp_code)
                .order_by(TQ_SK_SHAREHOLDER.UPDATEDATE.desc())
                .limit(1)
                .scalar_subquery()
            ))
            .distinct()
        ).all()

        if not results:
            logger.error(f"未找到公司代码 '{comp_code}' 的股东信息")
            raise Exception(f"公司代码 '{comp_code}' 无股东信息")

        logger.info(f"查询到 {len(results)} 条股东信息")
        return [TQ_SK_SHAREHOLDER(
            SHHOLDERNAME=r[0],
            SHHOLDERSECODE=r[1],
            UPDATEDATE=r[2],
            HOLDERAMT=r[3],
            HOLDERRTO=r[4]
        ) for r in results]

    except Exception as e:
        logger.error(f"查询股东信息失败: {e}")
        raise


if __name__ == "__main__":
    test_comp_code = "81572830"
    with Session(engine) as session:
        try:
            print(get_latest_shareholders(test_comp_code, session))
        except Exception as e:
            print(f"错误: {e}")