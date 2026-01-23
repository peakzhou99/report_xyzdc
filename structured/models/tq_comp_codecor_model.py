#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Description: 获取公司代码｜社会信用代码

from sqlmodel import SQLModel, Field
from typing import Optional

class TQ_COMP_CODECOR(SQLModel, table=True):
    """
    表名：机构代码外部对应表(TQ_COMP_CODECOR)
    元数据查询平台：https://datadict.finchina.com/index.html#/tableView/datatree?id=147_1994
    本地表结构：data/schema/大智慧财汇元数据表结构（mysql).xlsx/24.机构代码外部对应表(TQ_COMP_CODECOR)
    """
    ID: Optional[int] = Field(default=None, primary_key=True, description="流水号")
    COMPCODE: Optional[str] = Field(default=None, max_length=10, description="公司代码")
    COMPNAME: Optional[str] = Field(default=None, max_length=200, description="公司名称")
    CTYPE: Optional[int] = Field(default=None, description="对应类型")
    OutCode: Optional[str] = Field(default=None, max_length=20, description="外部代码")
