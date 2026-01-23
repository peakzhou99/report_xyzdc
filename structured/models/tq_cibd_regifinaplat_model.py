#!/usr/bin/env python
# -*- coding:utf-8 -*-

from sqlmodel import SQLModel, Field
from typing import Optional

class TQ_CIBD_REGIFINAPLAT(SQLModel, table=True):
    """
    表名：地方融资平台(TQ_CIBD_REGIFINAPLAT)
    元数据查询平台：https://datadict.finchina.com/index.html#/tableView/datatree?id=63_2309
    本地表结构：data/schema/大智慧财汇元数据表结构（mysql).xlsx/43.地方融资平台(TQ_CIBD_REGIFINAPLAT)
    """
    ID: Optional[int] = Field(default=None, primary_key=True, description="流水号")
    ITCODE: Optional[str] = Field(default=None, max_length=20, description="公司代码")
    ITNAME: Optional[str] = Field(default=None, max_length=200, description="公司名称")
    REGLANNAME_P: Optional[str] = Field(default=None, max_length=50, description="注册地省份")
    REGLANCODE_P: Optional[str] = Field(default=None, max_length=20, description="注册地省份代码")
    REGLANCODE_C: Optional[str] = Field(default=None, max_length=20, description="注册地市县代码")
    REGLANNAME_C: Optional[str] = Field(default=None, max_length=50, description="注册地市县")
    FINAFFCODE: Optional[str] = Field(default=None, max_length=20, description="融资归属地代码")
    FINAFFNAME: Optional[str] = Field(default=None, max_length=50, description="融资归属地")
    TERRITORYTYPE: Optional[str] = Field(default=None, max_length=50, description="归属地属性")
    BONDBALANCE: Optional[float] = Field(default=None, description="债券余额")
