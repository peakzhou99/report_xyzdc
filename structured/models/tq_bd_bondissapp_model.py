#!/usr/bin/env python
# -*- coding:utf-8 -*-

from sqlmodel import SQLModel, Field
from typing import Optional
from decimal import Decimal


class TqBdBondissapp(SQLModel, table=True):
    """
    表英文：TQ_BD_BONDISSAPP
    表中文：债券募集申请批复信息
    """
    __tablename__ = "TQ_BD_BONDISSAPP"

    ID: Optional[int] = Field(default=None, primary_key=True, description="流水号")
    BONDISSAPPID: Optional[int] = Field(default=None, description="债券募集申请批复信息来源表ID")
    APPDATE: Optional[str] = Field(default=None, max_length=8, description="批复日期")
    UPDATEDATE: Optional[str] = Field(default=None, max_length=8, description="更新日期")
    APPCOMPCODE: Optional[str] = Field(default=None, max_length=10, description="申请机构代码")
    APPCOMPNAME: Optional[str] = Field(default=None, max_length=100, description="申请机构全称")
    BONDTYPE: Optional[str] = Field(default=None, max_length=10, description="债券类型")
    MATURITY: Optional[Decimal] = Field(default=None, description="债券期限", max_digits=19, decimal_places=4)
    SCALE: Optional[Decimal] = Field(default=None, description="募集规模", max_digits=21, decimal_places=4)
    REPPERIOD: Optional[int] = Field(default=None, description="批复有效期限")
    REPBEGINDATE: Optional[str] = Field(default=None, max_length=8, description="批复有效起始日")
    REPENDDATE: Optional[str] = Field(default=None, max_length=8, description="批复有效终止日")
    REPCOMPCODE: Optional[str] = Field(default=None, max_length=10, description="批复机构")
    ISVALID: Optional[int] = Field(default=None, description="是否有效")
    PREMARKET: Optional[str] = Field(default=None, max_length=10, description="预交易市场")