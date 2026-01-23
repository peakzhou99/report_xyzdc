#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Description：查询信用债债券基本信息

from pathlib import Path
import sys

project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))
from sqlmodel import Session
from typing import List, Optional
from pydantic import BaseModel, Field
from sqlalchemy import text
from datetime import datetime
from utils.db_util import engine
from utils.logger_util import get_logger

logger = get_logger()


class BondInfoSummary(BaseModel):
    """债券信息汇总模型"""
    issuer_name: Optional[str] = Field(None, description="发行人")
    total_bonds: int = Field(0, description="债券总数")
    total_current_amount: float = Field(0.0, description="总当前余额（亿元）")
    total_issue_amount: float = Field(0.0, description="总发行规模（亿元）")


class BondInfoResponse(BaseModel):
    """债券基本信息响应模型"""
    seq: Optional[int] = Field(None, description="序号")
    bondsname: Optional[str] = Field(None, description="债券简称")
    creditrate: Optional[str] = Field(None, description="债券评级")
    currentamt: Optional[str] = Field(None, description="债券余额（亿元）")
    remaining_term: Optional[str] = Field(None, description="剩余期限（年）")
    actissamt: Optional[str] = Field(None, description="发行规模（亿元）")
    issbegdate: Optional[str] = Field(None, description="发行日期")
    maturityyear: Optional[str] = Field(None, description="债券期限（年）")
    couponrate: Optional[str] = Field(None, description="票面利率（%）")


class BondInfoFullResponse(BaseModel):
    """债券信息完整响应模型，包含列表和汇总"""
    summary: BondInfoSummary = Field(description="汇总信息")
    bonds: List[BondInfoResponse] = Field(description="债券详细列表")


def get_bond_info(issuer_code: str, session: Session) -> BondInfoFullResponse:
    """
    根据发行人代码查询债券基本信息，同时返回汇总统计。

    :param issuer_code: 发行人代码
    :param session: 数据库会话
    :return: 债券信息汇总和详细列表
    :raises Exception: 未找到债券信息或查询错误
    """
    try:
        logger.info(f"查询发行人代码 '{issuer_code}' 的债券基本信息")

        # 使用原生SQL查询
        current_date = datetime.now().strftime('%Y%m%d')
        sql_query = text("""
                         SELECT t1.ID,
                                t1.SECODE,
                                t1.SECURITYID,
                                t1.SYMBOL,
                                t1.BONDNAME,
                                t1.BONDSNAME,
                                t1.EXCHANGE,
                                t1.ISSUECOMPCODE,
                                t1.INITIALCREDITRATE,
                                ROUND(t1.MATURITYYEAR, 0) as MATURITYYEAR,
                                t1.MATURITYDATE,
                                ROUND(t1.COUPONRATE, 2)   as COUPONRATE,
                                t1.PUBLISHDATE,
                                t3.CURRENTAMT,
                                t3.COMPNAME,
                                t3.CURRENCYRATE,
                                t3.NEWRATE,
                                t3.ACTISSAMT,
                                t3.ISSBEGDATE             as NEWEST_ISSBEGDATE,
                                t3.RAISEMODE,
                                t3.LEADUWER,
                                t4.CREDITRATE,
                                t4.EXPTRATING,
                                t4.RADJUSTDIR,
                                t4.PUBLISHDATE            as RATING_PUBLISHDATE
                         FROM (SELECT b.*,
                                      ROW_NUMBER() OVER (PARTITION BY SECURITYID ORDER BY exchg_type) rn
                               FROM (SELECT a.*,
                                            CASE
                                                WHEN EXCHANGE = '001005' THEN '0'
                                                WHEN EXCHANGE = '001002' THEN '1'
                                                WHEN EXCHANGE = '001003' THEN '2'
                                                WHEN EXCHANGE = '001007' THEN '3'
                                                WHEN EXCHANGE = '001006' THEN '4'
                                                WHEN EXCHANGE = '001018' THEN '5'
                                                ELSE '6' END exchg_type
                                     FROM TQ_BD_BASICINFO a
                                     WHERE ISSUECOMPCODE = :issuer_code
                                       AND (MATURITYYEAR IS NULL OR MATURITYYEAR > 0)) b) t1
                                  JOIN (SELECT DISTINCT SECURITYID
                                        FROM TQ_BD_BASICINFO
                                        WHERE ISVALID = 1
                                          AND ISSUECOMPCODE = :issuer_code
                                          AND MATURITYDATE >= :current_date
                                          AND BONDNAME NOT LIKE '%回拨%'
                                          AND BONDNAME NOT LIKE '%发行失败%'
                                          AND BONDNAME NOT LIKE '%取消发行%') t2
                                       ON t2.SECURITYID = t1.SECURITYID AND t1.rn = 1
                                  JOIN (SELECT DISTINCT SECURITYID,
                                                        CURRENTAMT,
                                                        COMPNAME,
                                                        CURRENCYRATE,
                                                        NEWRATE,
                                                        ACTISSAMT,
                                                        ISSBEGDATE,
                                                        RAISEMODE,
                                                        LEADUWER
                                        FROM TQ_BD_NEWESTBASICINFO
                                        WHERE ISSUECOMPCODE = :issuer_code) t3 ON t3.SECURITYID = t2.SECURITYID
                                  LEFT JOIN (SELECT SECODE,
                                                    CREDITRATE,
                                                    EXPTRATING,
                                                    RADJUSTDIR,
                                                    PUBLISHDATE,
                                                    ROW_NUMBER() OVER (PARTITION BY SECODE ORDER BY PUBLISHDATE DESC) as rating_rn
                                             FROM TQ_BD_CREDITRATE) t4 ON t4.SECODE = t1.SECODE AND t4.rating_rn = 1
                         ORDER BY t1.MATURITYDATE DESC
                         """)

        results = session.execute(sql_query, {
            "issuer_code": issuer_code,
            "current_date": current_date
        }).fetchall()

        if not results:
            logger.error(f"未找到发行人代码 '{issuer_code}' 的债券信息")
            return BondInfoFullResponse(
                summary=BondInfoSummary(),
                bonds=[]
            )

        def calculate_remaining_term(maturity_date_str: str) -> Optional[float]:
            """计算剩余期限（年），对应SQL中的datediff计算"""
            if not maturity_date_str or maturity_date_str == '19000101':
                return None
            try:
                maturity_date = datetime.strptime(maturity_date_str, "%Y%m%d")
                current_date = datetime.now()
                if maturity_date < current_date:
                    return 0.0
                delta = maturity_date - current_date
                return round(delta.days / 365.0, 2)
            except ValueError:
                return None

        def format_date(date_str: str) -> Optional[str]:
            """格式化日期显示"""
            if not date_str or date_str == '19000101':
                return None
            try:
                date_obj = datetime.strptime(date_str, "%Y%m%d")
                return date_obj.strftime("%Y-%m-%d")
            except ValueError:
                return date_str

        def format_decimal(value, decimals=2) -> Optional[str]:
            """格式化Decimal值"""
            if value is None:
                return None
            return f"{float(value):.{decimals}f}"

        bond_list = []
        total_current_amount = 0.0
        total_issue_amount = 0.0
        issuer_name = None

        for index, row in enumerate(results):
            # 计算剩余期限
            remaining_term = calculate_remaining_term(row.MATURITYDATE)

            # 优先使用最新基本要素表中的发行日期
            issue_date = row.NEWEST_ISSBEGDATE if row.NEWEST_ISSBEGDATE else row.PUBLISHDATE

            # 计算汇总数据（假设单位为亿元，与原始逻辑一致）
            if row.CURRENTAMT:
                total_current_amount += float(row.CURRENTAMT)
            if row.ACTISSAMT:
                total_issue_amount += float(row.ACTISSAMT)

            # 保存发行人名称用于汇总
            if issuer_name is None and row.COMPNAME:
                issuer_name = row.COMPNAME

            bond_info = BondInfoResponse(
                seq=index + 1,
                bondsname=row.BONDSNAME,
                creditrate=row.CREDITRATE,
                currentamt=format_decimal(row.CURRENTAMT),
                remaining_term=f"{remaining_term:.2f}" if remaining_term is not None else None,
                actissamt=format_decimal(row.ACTISSAMT),
                issbegdate=format_date(issue_date),
                maturityyear=format_decimal(row.MATURITYYEAR, 0) if row.MATURITYYEAR else None,
                couponrate=format_decimal(row.COUPONRATE)
            )
            bond_list.append(bond_info)

        # 构建汇总信息
        summary = BondInfoSummary(
            issuer_name=issuer_name,
            total_bonds=len(bond_list),
            total_current_amount=round(total_current_amount, 2),
            total_issue_amount=round(total_issue_amount, 2)
        )

        logger.info(f"查询到 {len(bond_list)} 条债券信息")
        return BondInfoFullResponse(
            summary=summary,
            bonds=bond_list
        )

    except Exception as e:
        logger.error(f"查询债券基本信息出错: {e}")
        return BondInfoFullResponse(
            summary=BondInfoSummary(),
            bonds=[]
        )


if __name__ == "__main__":
    test_issuer_code = "81572830"
    with Session(engine) as session:
        try:
            response = get_bond_info(test_issuer_code, session)
            print("=== 债券基本信息汇总 ===")
            print(f"发行人: {response.summary.issuer_name}")
            print(f"债券总数: {response.summary.total_bonds}")
            print(f"总当前余额: {response.summary.total_current_amount} 亿元")
            print(f"总发行规模: {response.summary.total_issue_amount} 亿元")
            print("\n=== 债券详细信息 ===")
            for idx, bond in enumerate(response.bonds, 1):
                print(f"\n第 {idx} 条债券:")
                print(f"  债券简称: {bond.bondsname}")
                print(f"  债券评级: {bond.creditrate}")
                print(f"  债券余额: {bond.currentamt} 亿元")
                print(f"  剩余期限: {bond.remaining_term} 年")
                print(f"  发行规模: {bond.actissamt} 亿元")
                print(f"  发行日期: {bond.issbegdate}")
                print(f"  债券期限: {bond.maturityyear} 年")
                print(f"  票面利率: {bond.couponrate}%")
                print("-" * 50)
        except Exception as e:
            print(f"错误: {e}")
