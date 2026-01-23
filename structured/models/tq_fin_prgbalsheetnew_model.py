#!/usr/bin/env python
# -*- coding:utf-8 -*-

from sqlmodel import SQLModel, Field
from typing import Optional
from decimal import Decimal
from pydantic import computed_field


REPORTTYPE_MAPPING = {
    "1": "合并期末",
    "2": "母公司期末",
    "3": "合并期初",
    "4": "母公司期初"
}

class TQ_FIN_PRGBALSHEETNEW(SQLModel, table=True):
    """
    表英文：TQ_FIN_PRGBALSHEETNEW
    表中文：一般企业资产负债表(新准则产品表)
    元数据查询平台：https://datadict.finchina.com/index.html#/tableView/datatree?id=267_2543
    本地表结构文档：data/database_table_schema/TQ_FIN_PRGBALSHEETNEW-一般企业资产负债表(新准则产品表).docx
    """
    ID: Optional[int] = Field(default=None, primary_key=True, description="流水号")
    COMPCODE: Optional[str] = Field(default=None, max_length=20, description="公司内码")
    ENDDATE: Optional[str] = Field(default=None, max_length=8, description="截止日期")
    REPORTTYPE: Optional[str] = Field(default=None, max_length=10, description="报表类型")
    PUBLISHDATE: Optional[str] = Field(default=None, max_length=8, description="发布日期")
    CURFDS: Optional[Decimal] = Field(default=None, description="货币资金")
    TOTASSET: Optional[Decimal] = Field(default=None, description="资产总计")
    TOTLIAB: Optional[Decimal] = Field(default=None, description="负债合计")
    RIGHAGGR: Optional[Decimal] = Field(default=None, description="所有者权益(或股东权益)合计")
    TOTALCURRLIAB: Optional[Decimal] = Field(default=None, description="流动负债合计")
    PAIDINCAPI: Optional[Decimal] = Field(default=None, max_length=20, description="实收资本(或股本)")

    @computed_field
    @property
    def REPORTTYPE_VALUE(self) -> Optional[str]:
        """Return the human-readable value for REPORTTYPE."""
        return REPORTTYPE_MAPPING.get(self.REPORTTYPE, self.REPORTTYPE) if self.REPORTTYPE else None