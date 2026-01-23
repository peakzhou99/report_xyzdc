#!/usr/bin/env python
# -*- coding:utf-8 -*-

from sqlmodel import SQLModel, Field
from typing import Optional
from decimal import Decimal
from pydantic import computed_field

REPORTTYPE_MAPPING = {
    "1": "合并期末",
    "2": "母公司期末",
    "3": "合并期末_调整",
    "4": "母公司期末_调整"
}


class TQ_FIN_PRGINDICDATA(SQLModel, table=True):
    """
    表英文：TQ_FIN_PRGINDICDATA
    表中文：衍生财务指标(产品表)_V3
    元数据查询平台：https://datadict.finchina.com/index.html#/tableView/datatree?id=267_2543
    本地表结构文档：data/database_table_schema/TQ_FIN_PRGINDICDATA-衍生财务指标(产品表)_V3.docx
    """
    ID: Optional[int] = Field(default=None, primary_key=True, description="流水号")
    COMPCODE: Optional[str] = Field(default=None, max_length=20, description="公司内码")
    ENDDATE: Optional[str] = Field(default=None, max_length=8, description="截止日期")
    REPORTTYPE: Optional[str] = Field(default=None, max_length=10, description="报表类型")
    ASSLIABRT: Optional[Decimal] = Field(default=None, description="资产负债率")

    @computed_field
    @property
    def REPORTTYPE_VALUE(self) -> Optional[str]:
        """Return the human-readable value for REPORTTYPE."""
        return REPORTTYPE_MAPPING.get(self.REPORTTYPE, self.REPORTTYPE) if self.REPORTTYPE else None