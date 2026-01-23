#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Description：查询信用债担保人信息

from pathlib import Path
import sys

project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))
from sqlmodel import Session, select
from typing import List
from structured.models.tq_comp_guaranteedetails_model import TQ_COMP_GUARANTEEDETAILS
from utils.logger_util import get_logger
from utils.db_util import engine
from datetime import datetime

logger = get_logger()


def get_guarantors(company_code: str, session: Session) -> List[TQ_COMP_GUARANTEEDETAILS]:
    """
    根据公司代码查询担保人信息。

    :param company_code: 公司代码
    :param session: 数据库会话
    :return: 担保人信息列表
    :raises Exception: 未找到担保人信息或查询错误
    """
    try:
        logger.info(f"查询公司代码 '{company_code}' 的担保人信息")

        current_date = datetime.now().strftime("%Y%m%d")

        results = session.exec(
            select(TQ_COMP_GUARANTEEDETAILS.GUARNAME, TQ_COMP_GUARANTEEDETAILS.GUARCODE)
            .where(TQ_COMP_GUARANTEEDETAILS.SECUREDPARTYCODE == company_code)
            .where(TQ_COMP_GUARANTEEDETAILS.GUARSTATUS == '1')
            .where(TQ_COMP_GUARANTEEDETAILS.GUARENDDATE > current_date)
            .distinct()
        ).all()

        if not results:
            logger.error(f"未找到公司代码 '{company_code}' 的担保人信息")
            raise Exception(f"公司代码 '{company_code}' 无担保人信息")

        logger.info(f"查询到 {len(results)} 条担保人信息")
        return [TQ_COMP_GUARANTEEDETAILS(GUARNAME=r[0], GUARCODE=r[1]) for r in results]

    except Exception as e:
        logger.error(f"查询担保人信息失败: {e}")
        raise


if __name__ == "__main__":
    test_company_code = "81572830"
    with Session(engine) as session:
        try:
            print(get_guarantors(test_company_code, session))
        except Exception as e:
            print(f"错误: {e}")