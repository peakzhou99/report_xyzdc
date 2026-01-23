#!/usr/bin/env python
# -*- coding:utf-8 -*-
from sqlmodel import SQLModel, Field
from typing import Optional
from decimal import Decimal


class TQ_SK_SHAREHOLDER(SQLModel, table=True):
    """
    表名：股东名单(TQ_SK_SHAREHOLDER)
    元数据查询平台：https://datadict.finchina.com/index.html#/tableView/datatree?id=70_753
    本地表结构：data/schema/大智慧财汇元数据表结构（mysql).xlsx/1.股东名单(TQ_SK_SHAREHOLDER)
    """
    COMPCODE: Optional[str] = Field(default=None, primary_key=True, max_length=10, description="公司内码")
    SHHOLDERNAME: Optional[str] = Field(default=None, max_length=200, description="股东名称")
    SHHOLDERSECODE: Optional[str] = Field(default=None, max_length=20, description="股东代码")
    UPDATEDATE: Optional[str] = Field(default=None, max_length=8, description="信息更新日期")
    HOLDERAMT: Optional[Decimal] = Field(default=None, description="持股数量")
    HOLDERRTO: Optional[Decimal] = Field(default=None, description="持股数量占总股本比例")
