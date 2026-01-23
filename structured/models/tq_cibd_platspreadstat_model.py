#!/usr/bin/env python
# -*- coding:utf-8 -*-

from sqlmodel import SQLModel, Field
from typing import Optional
from decimal import Decimal

class TQ_CIBD_PLATSPREADSTAT(SQLModel, table=True):
    """
    表名：融资平台每日利差(TQ_CIBD_PLATSPREADSTAT)
    元数据查询平台：https://datadict.finchina.com/index.html#/tableView/datatree?id=63_4601
    本地表结构：data/schema/大智慧财汇元数据表结构（mysql).xlsx/53.融资平台每日利差(TQ_CIBD_PLATSPREADSTAT)
    """
    ID: Optional[int] = Field(default=None, primary_key=True, description="流水号")
    TRADEDATE: Optional[str] = Field(default=None, max_length=8, description="交易日期")
    ITCODE: Optional[str] = Field(default=None, max_length=20, description="公司代码（8位）")
    ITNAME: Optional[str] = Field(default=None, max_length=200, description="公司名称")
    REGIONCODE: Optional[str] = Field(default=None, max_length=20, description="地区代码")
    REGIONNAME: Optional[str] = Field(default=None, max_length=50, description="地区名称")
    SPREAD: Optional[Decimal] = Field(default=None, description="当前利差")
    ISVALID: Optional[int] = Field(default=None, description="是否有效")