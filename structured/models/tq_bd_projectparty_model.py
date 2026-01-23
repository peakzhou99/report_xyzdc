#!/usr/bin/env python
# -*- coding:utf-8 -*-

from sqlmodel import SQLModel, Field
from typing import Optional

class TqBdProjectParty(SQLModel, table=True):
    """
    表英文：TQ_BD_PROJECTPARTY
    表中文：项目当事人表
    暂无表详细信息
    """
    __tablename__ = "TQ_BD_PROJECTPARTY"
    ID: Optional[int] = Field(default=None, primary_key=True, description="流水号")
    PROJECTCODE: Optional[str] = Field(default=None, max_length=14, description="项目编号")
    PARTYTYPE: Optional[str] = Field(default=None, description="参与方类型，2表示发行人")
    PARTYCODE: Optional[str] = Field(default=None, max_length=10, description="参与方代码")
    PARTYNAME: Optional[str] = Field(default=None, max_length=200, description="参与方名称")
    ISVALID: Optional[int] = Field(default=1, description="是否有效")
