#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Description：融资平台【每日利差】章节内容，对应预警通【城投分析-利差分析】

from pathlib import Path
import sys
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))
from sqlmodel import Session
from sqlmodel import select
from typing import List, Optional
from pydantic import BaseModel, Field
from datetime import datetime, timedelta
from utils.db_util import engine
from structured.models.tq_cibd_platspreadstat_model import TQ_CIBD_PLATSPREADSTAT
from utils.logger_util import get_logger

logger = get_logger()


class PlatSpreadResponse(BaseModel):
    tradedate: Optional[str] = Field(None, description="交易日期")
    itname: Optional[str] = Field(None, description="公司名称")
    spread: Optional[str] = Field(None, description="利差")

def get_plat_spreads(itcode: str, session: Session) -> List[PlatSpreadResponse]:
    """
    根据公司代码查询并返回最近七天的融资平台每日利差信息
    """
    try:
        # 计算七天前的日期
        seven_days_ago_str = (datetime.now().date() - timedelta(days=7)).strftime('%Y%m%d')

        results = session.exec(
            select(
                TQ_CIBD_PLATSPREADSTAT.TRADEDATE,
                TQ_CIBD_PLATSPREADSTAT.ITNAME,
                TQ_CIBD_PLATSPREADSTAT.SPREAD
            )
            .where(TQ_CIBD_PLATSPREADSTAT.ITCODE == itcode)
            .where(TQ_CIBD_PLATSPREADSTAT.TRADEDATE >= seven_days_ago_str)
            .order_by(TQ_CIBD_PLATSPREADSTAT.TRADEDATE.desc())
        ).all()

        spread_list = [
            PlatSpreadResponse(
                tradedate=row.TRADEDATE,
                itname=row.ITNAME,
                spread=f"{float(row.SPREAD):.2f} BP" if row.SPREAD is not None else None
            ) for index, row in enumerate(results)
        ]

        return spread_list

    except Exception as e:
        logger.error(f"查询出错: {e}")
        return []

if __name__ == "__main__":
    """
    方法2 算平均
    SELECT tradedate, AVG(spread) AS avg_spread
    FROM 
        TQ_CIBD_BONDSPREADSTAT
    WHERE ITCODE='81572830'
    GROUP BY tradedate
    ORDER BY tradedate DESC;
    """
    test_itcode = "81572830"
    with Session(engine) as session:
        try:
            spreads = get_plat_spreads(test_itcode, session)

            for idx, spread in enumerate(spreads, 1):
                print(spread)
        except Exception as e:
            print(f"错误: {e}")

