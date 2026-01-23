#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Description：查询信用债区域排名信息

from pathlib import Path
import sys
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))
from sqlmodel import Session, select, desc
from typing import List
from pydantic import BaseModel, Field
from structured.models.tq_cibd_regifinaplat_model import TQ_CIBD_REGIFINAPLAT
from structured.models.tq_cibd_platscore_model import TQ_CIBD_PLATSCORE
from structured.models.tq_bd_creditrtissue_model import TQ_BD_CREDITRTISSUE
from utils.logger_util import get_logger
from utils.db_util import engine

logger = get_logger()

class PlatformScoreResponse(BaseModel):
    """响应模型"""
    itname: str | None = Field(None, description="公司名称")
    regionname: str | None = Field(None, description="区域名称")
    score_all: float | None = Field(None, description="总体评分")
    bondbalance: float | None = Field(None, description="债券余额")
    rank: str | None = Field(None, description="省内排名，格式为 '排名/总数'")
    credit_rate: str | None = Field(None, description="最新信用评级")

def get_platform_score_by_company_code(company_code: str, session: Session) -> List[PlatformScoreResponse]:
    """
    根据公司代码查询同一区域内所有城投企业的评分及其关联的地方融资平台数据，并计算省内排名（基于SCORE_ALL降序），添加最新信用评级信息。

    :param company_code: 公司代码
    :param session: 数据库会话
    :return: 区域内城投企业信息列表
    :raises Exception: 未找到公司或区域信息，或查询错误
    """
    try:
        logger.info(f"查询公司代码 '{company_code}' 的区域排名信息")

        # 查询给定公司的融资平台数据以获取 FINAFFCODE 和 REGLANCODE_P
        company_platform = session.exec(
            select(TQ_CIBD_REGIFINAPLAT)
            .where(TQ_CIBD_REGIFINAPLAT.ITCODE == company_code)
        ).first()

        if not company_platform or not company_platform.FINAFFCODE or not company_platform.REGLANCODE_P:
            logger.error(f"未找到公司代码 '{company_code}' 的融资归属地或省份信息")
            raise Exception(f"公司代码 '{company_code}' 无融资平台信息")

        # 查询同一区域内所有公司融资平台数据
        region_platforms = session.exec(
            select(TQ_CIBD_REGIFINAPLAT)
            .where(TQ_CIBD_REGIFINAPLAT.FINAFFCODE == company_platform.FINAFFCODE)
        ).all()

        if not region_platforms:
            logger.error(f"未找到公司代码 '{company_code}' 的区域内融资平台数据")
            raise Exception(f"未找到区域内融资平台数据")

        # 获取同一省份所有公司的评分
        prov_scores = session.exec(
            select(TQ_CIBD_PLATSCORE.ITCODE, TQ_CIBD_PLATSCORE.SCORE_ALL)
            .join(TQ_CIBD_REGIFINAPLAT, TQ_CIBD_PLATSCORE.ITCODE == TQ_CIBD_REGIFINAPLAT.ITCODE)
            .where(TQ_CIBD_REGIFINAPLAT.REGLANCODE_P == company_platform.REGLANCODE_P)
        ).all()

        # 按 SCORE_ALL 降序排序，处理 None 值
        sorted_prov_scores = sorted(prov_scores, key=lambda x: x.SCORE_ALL if x.SCORE_ALL is not None else float('-inf'), reverse=True)

        # 创建 ITCODE 到省内排名的字典，处理分数相同的情况
        rank_dict = {}
        current_rank = 1
        prev_score = None
        for idx, (itcode, score_all) in enumerate(sorted_prov_scores):
            if score_all != prev_score:
                current_rank = idx + 1
            rank_dict[itcode] = current_rank
            prev_score = score_all

        # 构建响应
        formatted_results = []
        for platform in region_platforms:
            # 查询关联评分数据
            score = session.exec(
                select(TQ_CIBD_PLATSCORE)
                .where(TQ_CIBD_PLATSCORE.ITCODE == platform.ITCODE)
            ).first()

            # 查询最新信用评级
            latest_rating = session.exec(
                select(TQ_BD_CREDITRTISSUE)
                .where(TQ_BD_CREDITRTISSUE.COMPCODE == platform.ITCODE)
                .order_by(desc(TQ_BD_CREDITRTISSUE.PUBLISHDATE))
            ).first()

            # 获取排名
            int_rank = rank_dict.get(platform.ITCODE)
            rank_str = f"{int_rank}/{len(prov_scores)}" if int_rank is not None else None

            # 使用评分表中的名称和区域（优先），否则用融资平台表
            itname = score.ITNAME if score else platform.ITNAME
            regionname = score.REGIONNAME if score else platform.FINAFFNAME

            formatted_results.append(PlatformScoreResponse(
                itname=itname,
                regionname=regionname,
                score_all=score.SCORE_ALL if score else None,
                bondbalance=platform.BONDBALANCE,
                rank=rank_str,
                credit_rate=latest_rating.CREDITRATE if latest_rating else None,
            ))

        # 按排名升序排序
        formatted_results.sort(key=lambda x: int(x.rank.split('/')[0]) if x.rank else float('inf'))

        logger.info(f"查询到 {len(formatted_results)} 条区域排名信息")
        return formatted_results

    except Exception as e:
        logger.error(f"查询区域排名信息失败: {e}")
        raise

if __name__ == "__main__":
    test_company_code = "81572830"
    with Session(engine) as session:
        try:
            response = get_platform_score_by_company_code(test_company_code, session)
            print("区域排名信息:")
            for item in response:
                print(item)
        except Exception as e:
            print(f"错误: {e}")