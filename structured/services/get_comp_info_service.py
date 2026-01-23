#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Description：信用债【公司基本信息】章节内容
# TODO 缺少 实缴资本 需要从募集说明书中取值

from pathlib import Path
import sys
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))
from sqlmodel import Session, select
from typing import Optional
from structured.models.tq_comp_info_model import TQ_COMP_INFO
from utils.db_util import engine
from utils.logger_util import get_logger

logger = get_logger()

def get_company_info(comp_code: str, session: Session) -> TQ_COMP_INFO:
    """
    根据公司代码查询公司基本信息。

    :param comp_code: 公司代码
    :param session: 数据库会话
    :return: 包含公司基本信息的 TQ_COMP_INFO 对象
    :raises Exception: 如果未找到公司信息或发生其他错误
    """
    try:
        logger.info(f"查询公司代码 '{comp_code}' 的基本信息")

        company_info: Optional[TQ_COMP_INFO] = session.exec(
            select(TQ_COMP_INFO)
            .where(TQ_COMP_INFO.COMPCODE == comp_code)
        ).first()

        if company_info is None:
            logger.error(f"未找到公司代码 '{comp_code}' 的基本信息")
            raise Exception(f"公司代码 '{comp_code}' 不存在或无基本信息")

        logger.info(f"查询成功，公司名称: {company_info.COMPNAME}")
        return TQ_COMP_INFO(**company_info.model_dump())

    except Exception as e:
        logger.error(f"查询公司基本信息时出错: {e}")
        raise


def get_compcode_by_name(company_name: str, session: Session) -> str:
    """
    根据公司名称从 TQ_COMP_INFO 表中查询 COMPCODE
    """
    try:
        logger.info(f"正在从 TQ_COMP_INFO 表查询公司 '{company_name}' 的内码")
        result = session.exec(
            select(TQ_COMP_INFO.COMPCODE)
            .where(TQ_COMP_INFO.COMPNAME == company_name)
            .distinct()
        ).first()

        if not result:
            logger.error(f"在 TQ_COMP_INFO 中未找到公司: {company_name}")
            raise Exception(f"公司 '{company_name}' 在机构资料表中不存在")

        return result
    except Exception as e:
        logger.error(f"查询公司内码失败: {e}")
        raise

if __name__ == "__main__":
    test_comp_code = "81572830"
    with Session(engine) as session:
        try:
            print(get_company_info(test_comp_code, session))
        except Exception as e:
            print(f"错误: {e}")