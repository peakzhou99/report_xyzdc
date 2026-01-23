#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Description：信用债【经济指标查询】

from pathlib import Path
import sys

project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from sqlmodel import Session, select
from typing import List
from structured.models.tq_fin_prgindicdata_model import TQ_FIN_PRGINDICDATA
from structured.models.tq_fin_prgbalsheetnew_model import TQ_FIN_PRGBALSHEETNEW
from structured.models.tq_cibd_regifin_new_model import TQ_CIBD_REGIFIN_NEW
from structured.models.tq_cibd_regifinaplat_model import TQ_CIBD_REGIFINAPLAT
from utils.db_util import engine
from utils.logger_util import get_logger

logger = get_logger()


def get_latest_prgindicdata(compcode: str, session: Session) -> List[TQ_FIN_PRGINDICDATA]:
    """
    根据公司内码查询最新资产负债率

    :param compcode: 公司内码
    :param session: 数据库会话
    :return: 最新资产负债率数据列表
    """
    try:
        logger.info(f"查询公司 '{compcode}' 的最新资产负债率")

        results = session.exec(
            select(TQ_FIN_PRGINDICDATA)
            .where(TQ_FIN_PRGINDICDATA.COMPCODE == compcode)
            .where(TQ_FIN_PRGINDICDATA.REPORTTYPE == "3")
            .order_by(TQ_FIN_PRGINDICDATA.ENDDATE.desc())
            .limit(1)
        ).all()

        logger.info(f"查询成功，返回 {len(results)} 条记录")
        return results

    except Exception as e:
        logger.error(f"查询最新资产负债率时出错: {e}")
        raise


def get_latest_prgbalsheetnew(compcode: str, session: Session) -> List[TQ_FIN_PRGBALSHEETNEW]:
    """
    根据公司内码查询最新一般企业资产负债表数据

    :param compcode: 公司内码
    :param session: 数据库会话
    :return: 最新资产负债表数据列表
    """
    try:
        logger.info(f"查询公司 '{compcode}' 的最新一般企业资产负债表数据")

        results = session.exec(
            select(TQ_FIN_PRGBALSHEETNEW)
            .where(TQ_FIN_PRGBALSHEETNEW.COMPCODE == compcode)
            .where(TQ_FIN_PRGBALSHEETNEW.REPORTTYPE == "1")
            .order_by(TQ_FIN_PRGBALSHEETNEW.ENDDATE.desc())
            .limit(1)
        ).all()

        logger.info(f"查询成功，返回 {len(results)} 条记录")
        return results

    except Exception as e:
        logger.error(f"查询最新一般企业资产负债表数据时出错: {e}")
        raise


def get_latest_regifin(regioncode: int, session: Session) -> List[TQ_CIBD_REGIFIN_NEW]:
    """
    根据地区编码查询一般公共预算收入经济数据

    :param regioncode: 地区编码
    :param session: 数据库会话
    :return: 一般公共预算收入经济数据列表
    """
    try:
        logger.info(f"查询地区 '{regioncode}' 的最新一般公共预算收入经济数据")

        results = session.exec(
            select(TQ_CIBD_REGIFIN_NEW)
            .where(TQ_CIBD_REGIFIN_NEW.REGIONCODE == regioncode)
            .where(TQ_CIBD_REGIFIN_NEW.INDICNAME == "一般公共预算收入")
            .order_by(TQ_CIBD_REGIFIN_NEW.ENTRYDATE.desc())
            .limit(1)
        ).all()

        logger.info(f"查询成功，返回 {len(results)} 条记录")
        return results

    except Exception as e:
        logger.error(f"查询最新一般公共预算收入经济数据时出错: {e}")
        raise


def get_latest_regifin_by_company(company_code: str, session: Session) -> List[TQ_CIBD_REGIFIN_NEW]:
    """
    根据公司代码查询一般公共预算收入经济数据（通过获取融资归属地代码）

    :param company_code: 公司代码
    :param session: 数据库会话
    :return: 一般公共预算收入经济数据列表
    """
    try:
        logger.info(f"根据公司代码 '{company_code}' 查询最新一般公共预算收入经济数据")

        results = session.exec(
            select(TQ_CIBD_REGIFIN_NEW)
            .where(
                TQ_CIBD_REGIFIN_NEW.REGIONCODE ==
                select(TQ_CIBD_REGIFINAPLAT.FINAFFCODE)
                .where(TQ_CIBD_REGIFINAPLAT.ITCODE == company_code)
                .limit(1)
                .scalar_subquery()
            )
            .where(TQ_CIBD_REGIFIN_NEW.INDICNAME == "一般公共预算收入")
            .order_by(TQ_CIBD_REGIFIN_NEW.ENTRYDATE.desc())
            .limit(1)
        ).all()

        logger.info(f"查询成功，返回 {len(results)} 条记录")
        return results

    except Exception as e:
        logger.error(f"根据公司代码查询最新一般公共预算收入经济数据时出错: {e}")
        raise


if __name__ == "__main__":
    test_compcode = "81572830"
    test_regioncode = 370900

    with Session(engine) as session:
        try:
            # 测试查询最新资产负债率
            print("=" * 50)
            print("测试查询最新资产负债率:")
            result1 = get_latest_prgindicdata(test_compcode, session)
            print(result1)

            # 测试查询最新资产负债表数据
            print("=" * 50)
            print("测试查询最新一般企业资产负债表数据:")
            result2 = get_latest_prgbalsheetnew(test_compcode, session)
            print(result2)

            # 测试查询最新经济数据（地区编码）
            print("=" * 50)
            print("测试查询最新一般公共预算收入经济数据 (地区):")
            result3 = get_latest_regifin(test_regioncode, session)
            print(result3)

            # 测试查询最新经济数据（公司代码）
            print("=" * 50)
            print("测试查询最新一般公共预算收入经济数据 (公司):")
            result4 = get_latest_regifin_by_company(test_compcode, session)
            print(result4)

        except Exception as e:
            print(f"测试查询经济指标失败: {e}")
