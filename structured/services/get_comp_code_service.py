#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Description: 获取公司代码和统一社会信用代码

from pathlib import Path
import sys
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from sqlmodel import Session, select
from typing import Optional
from structured.models.tq_comp_codecor_model import TQ_COMP_CODECOR
from utils.logger_util import get_logger
from utils.db_util import engine
from pydantic import BaseModel, Field

logger = get_logger()

class CompanyInfoResponse(BaseModel):
    COMPCODE: Optional[str] = Field(None, description="公司代码")
    OUTCODE: Optional[str] = Field(None, description="统一社会信用代码")

def get_company_code(company_name: str, session: Session) -> CompanyInfoResponse:
    """
    根据公司名称查询公司代码和统一社会信用代码（CTYPE=1 时的 OutCode）。
    如果公司代码不存在，抛出异常；统一社会信用代码可选。

    :param company_name: 公司名称
    :param session: 数据库会话
    :return: 包含公司代码和统一社会信用代码（如有）的响应对象
    :raises ValueError: 如果未找到公司代码
    """
    try:
        logger.info(f"查询公司 '{company_name}' 的公司代码和统一社会信用代码")

        # 查询 COMPCODE
        comp_code: Optional[str] = session.exec(
            select(TQ_COMP_CODECOR.COMPCODE)
            .where(TQ_COMP_CODECOR.COMPNAME == company_name)
            .distinct()
        ).first()

        if comp_code is None:
            logger.error(f"未找到公司 '{company_name}' 的公司代码")
            raise ValueError(f"公司 '{company_name}' 的公司代码不存在")

        # 查询 OutCode（仅当 CTYPE=1）
        out_code: Optional[str] = session.exec(
            select(TQ_COMP_CODECOR.OutCode)
            .where(TQ_COMP_CODECOR.COMPNAME == company_name)
            .where(TQ_COMP_CODECOR.CTYPE == 1)
            .distinct()
        ).first()

        logger.info(f"查询成功，公司代码: {comp_code}，统一社会信用代码: {out_code or '无'}")
        return CompanyInfoResponse(COMPCODE=comp_code, OUTCODE=out_code)

    except Exception as e:
        logger.error(f"查询公司代码和统一社会信用代码时出错: {e}")
        raise

if __name__ == "__main__":
    test_company_name = "泰安泰山城乡建设发展有限公司"
    with Session(engine) as session:
        try:
            print(get_company_code(test_company_name, session))
        except Exception as e:
            print(f"错误: {e}")