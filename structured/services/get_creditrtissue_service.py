#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Description：查询信用债主体评级信息

from pathlib import Path
import sys
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))
from sqlmodel import Session, select, desc
from typing import List
from structured.models.tq_bd_creditrtissue_model import TQ_BD_CREDITRTISSUE
from utils.logger_util import get_logger
from utils.db_util import engine

logger = get_logger()

def get_credit_ratings(comp_code: str, session: Session) -> List[TQ_BD_CREDITRTISSUE]:
    """
    根据公司代码查询所有发债机构信用评级信息。

    :param comp_code: 公司代码
    :param session: 数据库会话
    :return: 信用评级信息列表
    :raises Exception: 未找到信用评级信息或查询错误
    """
    try:
        logger.info(f"查询公司代码 '{comp_code}' 的所有信用评级信息")

        results = session.exec(
            select(TQ_BD_CREDITRTISSUE)
            .where(TQ_BD_CREDITRTISSUE.COMPCODE == comp_code)
            .order_by(desc(TQ_BD_CREDITRTISSUE.PUBLISHDATE))
            .distinct()
        ).all()

        if not results:
            logger.error(f"未找到公司代码 '{comp_code}' 的信用评级信息")
            raise Exception(f"公司代码 '{comp_code}' 无信用评级信息")

        logger.info(f"查询到 {len(results)} 条信用评级信息")
        return [TQ_BD_CREDITRTISSUE(**r.model_dump()) for r in results]

    except Exception as e:
        logger.error(f"查询信用评级信息失败: {e}")
        raise

def get_latest_credit_rating(comp_code: str, session: Session) -> TQ_BD_CREDITRTISSUE:
    """
    根据公司代码查询最新发债机构信用评级信息（基于PUBLISHDATE）。

    :param comp_code: 公司代码
    :param session: 数据库会话
    :return: 最新信用评级信息
    :raises Exception: 未找到信用评级信息或查询错误
    """
    try:
        logger.info(f"查询公司代码 '{comp_code}' 的最新信用评级信息")

        result = session.exec(
            select(TQ_BD_CREDITRTISSUE)
            .where(TQ_BD_CREDITRTISSUE.COMPCODE == comp_code)
            .order_by(desc(TQ_BD_CREDITRTISSUE.PUBLISHDATE))
        ).first()

        if not result:
            logger.error(f"未找到公司代码 '{comp_code}' 的信用评级信息")
            raise Exception(f"公司代码 '{comp_code}' 无信用评级信息")

        logger.info(f"查询到最新信用评级信息")
        return TQ_BD_CREDITRTISSUE(**result.model_dump())

    except Exception as e:
        logger.error(f"查询最新信用评级信息失败: {e}")
        raise

if __name__ == "__main__":
    test_comp_code = "81572830"
    with Session(engine) as session:
        try:
            all_ratings = get_credit_ratings(test_comp_code, session)
            print("所有信用评级信息:", all_ratings)
            latest_rating = get_latest_credit_rating(test_comp_code, session)
            print("最新信用评级信息:", latest_rating)
        except Exception as e:
            print(f"错误: {e}")