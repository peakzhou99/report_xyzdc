#!/usr/bin/env python
# -*- coding:utf-8 -*-
# TODO 缺少 实缴资本 需要从募集说明书中取值

from sqlmodel import SQLModel, Field
from typing import Optional
from decimal import Decimal

class TQ_COMP_INFO(SQLModel, table=True):
    """
    表名：机构资料表(TQ_COMP_INFO)
    元数据查询平台：https://datadict.finchina.com/index.html#/tableView/datatree?id=147_495
    本地表结构：data/schema/大智慧财汇元数据表结构（mysql).xlsx/23.机构资料表(TQ_COMP_INFO)
    """
    COMPCODE: Optional[str] = Field(default=None, primary_key=True, max_length=10, description="机构内码")
    COMPNAME: Optional[str] = Field(default=None, max_length=200, description="机构全称")
    FOUNDDATE: Optional[str] = Field(default=None, max_length=8, description="成立日期")
    REGCAPITAL: Optional[Decimal] = Field(default=None, description="注册资本")
    LEGREP: Optional[str] = Field(default=None, max_length=100, description="法人代表")
    COMPTEL: Optional[str] = Field(default=None, max_length=100, description="公司电话")
    COMPFAX: Optional[str] = Field(default=None, max_length=100, description="公司传真")
    REGADDR: Optional[str] = Field(default=None, max_length=200, description="注册地址")
    OFFICEADDR: Optional[str] = Field(default=None, max_length=200, description="办公地址")
    OFFICEZIPCODE: Optional[str] = Field(default=None, max_length=50, description="办公地址邮编")
    BIZSCOPE: Optional[str] = Field(default=None, description="经营范围")
