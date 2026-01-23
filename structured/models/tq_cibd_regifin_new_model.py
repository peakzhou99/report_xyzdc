#!/usr/bin/env python
# -*- coding:utf-8 -*-

from sqlmodel import SQLModel, Field
from typing import Optional
from decimal import Decimal
from datetime import datetime
from pydantic import computed_field

UNIT_MAPPING = {
    "1": "元",
    "10": "元/平方米",
    "2": "万元",
    "3": "亿元",
    "4": "千美元",
    "5": "万美元",
    "6": "万人",
    "7": "%",
    "8": "亿美元",
    "9": "平方公里"
}


class TQ_CIBD_REGIFIN_NEW(SQLModel, table=True):
    """
    表英文：TQ_CIBD_REGIFIN_NEW
    表中文：城投经济数据表
    元数据查询平台：https://datadict.finchina.com/index.html#/tableView/datatree?id=63_4627
    本地表结构文档：data/database_table_schema/TQ_CIBD_REGIFIN_NEW-城投经济数据(产品表).docx
    """
    ID: Optional[int] = Field(default=None, primary_key=True, description="流水号")
    REGIONCODE: Optional[int] = Field(default=None, description="地区编码")
    REGIONNAME: Optional[str] = Field(default=None, max_length=50, description="地区名称")
    INDICNAME: Optional[str] = Field(default=None, max_length=100, description="指标名称")
    INDICCODE: Optional[str] = Field(default=None, max_length=10, description="指标代码")
    ENTRYDATE: Optional[datetime] = Field(default=None, max_length=8, description="录入日期")
    MVALUE: Optional[Decimal] = Field(default=None, description="值")
    DISPLAYCUNIT: Optional[str] = Field(default=None, max_length=10, description="显示单位")

    @computed_field
    @property
    def DISPLAYCUNIT_VALUE(self) -> Optional[str]:
        """Return the human-readable value for DISPLAYCUNIT."""
        return UNIT_MAPPING.get(self.DISPLAYCUNIT, self.DISPLAYCUNIT) if self.DISPLAYCUNIT else None

    class Config:
        json_encoders = {
            datetime: lambda dt: dt.strftime("%Y%m%d") if dt else None
        }