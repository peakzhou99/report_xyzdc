#!/usr/bin/env python
# -*- coding:utf-8 -*-

from sqlmodel import SQLModel, Field
from typing import Optional
from decimal import Decimal


class TqBdNdrcabondinfo(SQLModel, table=True):
    """
    表英文：TQ_BD_NDRCABONDINFO
    表中文：发改委批复债券信息表
    """
    __tablename__ = "TQ_BD_NDRCABONDINFO"

    ID: Optional[int] = Field(default=None, primary_key=True, description="流水号")
    NDRCABONDINFOID: Optional[int] = Field(default=None, description="发改委批复债券信息来源表ID")
    DECLAREDATE: Optional[str] = Field(default=None, max_length=8, description="公告日期")
    ABPCODE: Optional[str] = Field(default=None, max_length=20, description="项目代码")
    ABPNAME: Optional[str] = Field(default=None, max_length=200, description="项目名称")
    APPROVEDATE: Optional[str] = Field(default=None, max_length=8, description="审批日期")
    AMOUNT: Optional[Decimal] = Field(default=None, description="金额", max_digits=19, decimal_places=6)
    BCODE: Optional[str] = Field(default=None, max_length=20, description="债券组合代码")
    BONDTYPE: Optional[str] = Field(default=None, max_length=10, description="债券类型")
    APPROVALIDATE: Optional[Decimal] = Field(default=None, description="审批有效期", max_digits=9, decimal_places=2)
    ISSUEMODE: Optional[str] = Field(default=None, max_length=10, description="发行方式")
    ISVALID: Optional[int] = Field(default=None, description="是否有效")
    EXCHANGE: Optional[str] = Field(default=None, max_length=10, description="预计交易市场")
    REGISTERENDDATE: Optional[str] = Field(default=None, max_length=8, description="有效截止日")