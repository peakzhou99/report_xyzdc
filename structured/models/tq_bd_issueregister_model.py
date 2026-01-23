#!/usr/bin/env python
# -*- coding:utf-8 -*-

from sqlmodel import SQLModel, Field
from typing import Optional
from decimal import Decimal


class TqBdIssueregister(SQLModel, table=True):
    """
    表英文：TQ_BD_ISSUEREGISTER
    表中文：债券发行登记注册信息表
    """
    __tablename__ = "TQ_BD_ISSUEREGISTER"

    ID: Optional[int] = Field(default=None, primary_key=True, description="流水号")
    BONDIRIID: Optional[int] = Field(default=None, description="债券发行登记注册信息表ID")
    ANNOUNCEMTID: Optional[int] = Field(default=None, description="机构公告表ID")
    DECLAREDATE: Optional[str] = Field(default=None, max_length=8, description="公告日期")
    COMPCODE: Optional[str] = Field(default=None, max_length=8, description="公司代码")
    BONDTYPE: Optional[int] = Field(default=None, description="债券类型")
    REGISTERLIMIT: Optional[Decimal] = Field(default=None, description="注册额度", max_digits=19, decimal_places=6)
    REGISTERBEGINDATE: Optional[str] = Field(default=None, max_length=8, description="注册有效起始日")
    REGISTERENDDATE: Optional[str] = Field(default=None, max_length=8, description="注册有效终止日")
    SERIALNUM: Optional[int] = Field(default=None, description="序号")
    REFERENCENUM: Optional[str] = Field(default=None, max_length=50, description="发文字号")
    MEMO: Optional[str] = Field(default=None, description="备注")
    ISVALID: Optional[int] = Field(default=None, description="是否有效")
    TMSTAMP: Optional[str] = Field(default=None, description="时间标识")
