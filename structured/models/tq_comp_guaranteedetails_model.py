#!/usr/bin/env python
# -*- coding:utf-8 -*-
from sqlmodel import SQLModel, Field
from typing import Optional
from datetime import datetime


class TQ_COMP_GUARANTEEDETAILS(SQLModel, table=True):
    """
    表名：发债人对外担保明细(TQ_COMP_GUARANTEEDETAILS)
    元数据查询平台：https://datadict.finchina.com/index.html#/tableView/datatree?id=121_2503
    本地表结构：data/schema/大智慧财汇元数据表结构（mysql).xlsx/19.发债人对外担保明细(TQ_COMP_GUARANTEEDETAILS)
    """
    ID: Optional[int] = Field(default=None, primary_key=True, description="流水号")
    COMPANYCODE: Optional[str] = Field(default=None, max_length=10, description="公司代码")
    GUARCODE: Optional[str] = Field(default=None, max_length=10, description="担保方代码")
    GUARNAME: Optional[str] = Field(default=None, description="担保方名称")
    SECUREDPARTYCODE: Optional[str] = Field(default=None, max_length=10, description="被担保方代码")
    SECUREDPARTYNAME: Optional[str] = Field(default=None, description="被担保方名称")
    GUARSTATUS: Optional[str] = Field(default=None, max_length=10, description="担保状态")
    GUARENDDATE: Optional[str] = Field(default=None, max_length=8, description="担保截止日")
