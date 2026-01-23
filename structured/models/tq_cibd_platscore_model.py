#!/usr/bin/env python
# -*- coding:utf-8 -*-

from sqlmodel import SQLModel, Field
from typing import Optional
from datetime import datetime

class TQ_CIBD_PLATSCORE(SQLModel, table=True):
    """
    表名：城投企业评分(TQ_CIBD_PLATSCORE)
    元数据查询平台：https://datadict.finchina.com/index.html#/tableView/datatree?id=63_4722
    本地表结构：data/schema/大智慧财汇元数据表结构（mysql).xlsx/61.城投企业评分(TQ_CIBD_PLATSCORE)
    """
    ID: Optional[int] = Field(default=None, primary_key=True, description="流水号")
    ITCODE: Optional[str] = Field(default=None, max_length=20, description="公司内码")
    ITNAME: Optional[str] = Field(default=None, max_length=500, description="公司名称")
    REGIONCODE: Optional[str] = Field(default=None, max_length=10, description="地区代码")
    REGIONNAME: Optional[str] = Field(default=None, max_length=50, description="地区名称")
    SCORE_ALL: Optional[float] = Field(default=None, description="综合评分")
    ENTRYDATE: Optional[datetime] = Field(default=None, max_length=8, description="入库日期")


