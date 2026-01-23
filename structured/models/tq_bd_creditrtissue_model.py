#!/usr/bin/env python
# -*- coding:utf-8 -*-
from sqlmodel import SQLModel, Field
from typing import Optional
from pydantic import computed_field

RATING_OUTLOOK_DICT = {
    "1": "正面",
    "2": "稳定",
    "3": "负面",
    "4": "列入评级观察(可能调高)",
    "5": "列入评级观察(可能调低)",
    "6": "列入评级观察(走势不明)",
    "7": "待决",
    "9": "无"
}

COMPANY_TYPE_DICT = {
    "1": "发行人",
    "2": "担保人",
    "3": "其他",
    "4": "再担保人"
}

class TQ_BD_CREDITRTISSUE(SQLModel, table=True):
    """
    表名：发债机构信用评级(TQ_BD_CREDITRTISSUE)
    元数据查询平台：https://datadict.finchina.com/index.html#/tableView/datatree?id=116_670
    本地表结构：data/schema/大智慧财汇元数据表结构（mysql).xlsx/14.发债机构信用评级(TQ_BD_CREDITRTISSUE)
    """
    ID: Optional[int] = Field(default=None, primary_key=True, description="流水号")
    PUBLISHDATE: Optional[str] = Field(default=None, max_length=8, description="评级日期")
    COMPCODE: Optional[str] = Field(default=None, max_length=10, description="公司内码")
    COMTYPE: Optional[str] = Field(default=None, max_length=10, description="公司当事人属性")
    RATECOMNAME: Optional[str] = Field(default=None, max_length=100, description="资信评估机构名称")
    CREDITRATE: Optional[str] = Field(default=None, max_length=10, description="信用评级")
    EXPTRATING: Optional[str] = Field(default=None, max_length=10, description="评级展望")
    DECLAREDATE: Optional[str] = Field(default=None, max_length=8, description="公告日期")
    CREDITRATEENDDATE: Optional[str] = Field(default=None, max_length=8, description="评级有效截止日期")

    @computed_field
    @property
    def COMTYPE_value(self) -> Optional[str]:
        """Return the human-readable value for COMTYPE."""
        return COMPANY_TYPE_DICT.get(self.COMTYPE, self.COMTYPE) if self.COMTYPE else None

    @computed_field
    @property
    def EXPTRATING_value(self) -> Optional[str]:
        """Return the human-readable value for EXPTRATING."""
        return RATING_OUTLOOK_DICT.get(self.EXPTRATING, self.EXPTRATING) if self.EXPTRATING else None
