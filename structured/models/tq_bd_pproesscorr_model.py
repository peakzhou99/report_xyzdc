#!/usr/bin/env python
# -*- coding:utf-8 -*-

from sqlmodel import SQLModel, Field
from typing import Optional

class TqBdPproesscorr(SQLModel, table=True):
    """
    表英文：TQ_BD_PPROESSCORR
    表中文：债券项目进程对应表
    元数据查询平台：https://datadict.finchina.com/index.html#/tableView/datatree?id=111_4629
    本地表结构文档：data/database_table_schema/TQ_BD_PPROESSCORR-债券项目进程对应表.docx
    """
    __tablename__ = "TQ_BD_PPROESSCORR"
    ID: Optional[int] = Field(default=None, primary_key=True, description="流水号")
    RECORDID: Optional[int] = Field(default=None, description="备案表ID")
    RECORDTABLE: Optional[str] = Field(default=None, max_length=50, description="备案阶段表名")
    REGTABLE: Optional[str] = Field(default=None, max_length=50, description="注册阶段表名")
    REGID: Optional[int] = Field(default=None, description="注册表ID")
    ISSUEID: Optional[int] = Field(default=None, description="发行表ID")