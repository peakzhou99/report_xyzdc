#!/usr/bin/env python
# -*- coding:utf-8 -*-

from sqlmodel import SQLModel, Field
from typing import Optional
from decimal import Decimal
from datetime import datetime
from pydantic import computed_field

# 枚举映射字典
PROJECTTYPE_DICT = {
    "1": "资产支持证券", "10": "PRN", "11": "PB", "12": "PB-MTN", "13": "大公募",
    "14": "企业债券", "15": "PB-CP", "16": "SMECN", "17": "PB-RB", "18": "TDFI",
    "19": "PPN", "2": "小公募", "20": "PB-DFI", "21": "CB", "3": "私募", "4": "MTN",
    "5": "SCP", "6": "CP", "7": "DFI", "8": "ABN", "9": "PN"
}

EXCHANGE_DICT = {
    "1": "上交所", "2": "深交所", "3": "银行间", "4": "机构间市场", "5": "证券公司", "6": "北京证券交易所"
}

PROCESSTYPE_DICT = {
    "1": "交易所已受理项目", "10": "交易商协会预评中", "11": "交易商协会反馈中", "12": "交易商协会待上会",
    "13": "交易商协会已上会", "14": "交易商协会完成注册", "15": "提交注册（交易所）", "16": "注册生效（交易所）",
    "17": "不予注册（交易所）", "18": "中央结算公司已申报", "19": "中央结算公司受理反馈待答复", "2": "交易所已反馈意见",
    "20": "中央结算公司已受理", "21": "已注册（中央结算公司）", "22": "中央结算公司终止受理", "23": "中央结算公司已终止",
    "24": "中央结算已撤销申请", "25": "交易商协会待受理", "26": "交易商协会已撤卷", "27": "交易商协会链接失效",
    "28": "终止注册（交易所）", "3": "交易所已接收反馈意见", "4": "项目主体已回复交易所意见", "5": "交易所通过项目",
    "6": "交易所未通过项目", "7": "交易所终止项目", "8": "交易所中止项目", "9": "交易商协会已受理"
}

DATASOURCE_DICT = {
    "1": "上海证券交易所", "2": "深圳证券交易所", "3": "中国证券投资基金业协会", "4": "中国债券信息网",
    "5": "中国银行间市场交易商协会", "6": "北京证券交易所"
}



class TqBdProjectInfo(SQLModel, table=True):
    """
    表英文：TQ_BD_PROJECTINFO
    表中文：项目基本信息表
    元数据查询平台：https://datadict.finchina.com/index.html#/tableView/datatree?id=114_2135
    本地表结构文档：data/database_table_schema/TQ_BD_PROJECTINFO-项目基本信息表.docx
    """
    __tablename__ = "TQ_BD_PROJECTINFO"
    ID: Optional[int] = Field(default=None, primary_key=True, description="流水号")
    UPDATEDATE: Optional[str] = Field(default=None, max_length=8, description="更新日期")
    PROJECTCODE: Optional[str] = Field(default=None, max_length=14, description="项目编号")
    PROJECTTYPE: Optional[str] = Field(default=None, max_length=10, description="项目类别")
    PROJECTNAME: Optional[str] = Field(default=None, max_length=200, description="项目名称")
    PLANAMOUNT: Optional[Decimal] = Field(default=None, description="拟发行金额", max_length=19)
    EXCHANGE: Optional[str] = Field(default=None, max_length=10, description="交易市场")
    PROCESSTYPE: Optional[str] = Field(default=None, max_length=10, description="项目进展状况")
    PROCESSDATE: Optional[str] = Field(default=None, max_length=8, description="项目进展日期")
    REGISTERENDDATE: Optional[str] = Field(default=None, max_length=8, description="注册有效终止日")
    ENTRYDATE: Optional[datetime] = Field(default=None, description="录入日期")
    DATASOURCE: Optional[str] = Field(default=None, max_length=10, description="数据来源")
    PROJECTID: Optional[int] = Field(default=None, description="债券项目信息表ID")
    REGREPLAYCODE: Optional[str] = Field(default=None, max_length=100, description="注册批复文号")
    REGISTERAMT: Optional[Decimal] = Field(default=None, description="注册金额", max_length=19)

    @computed_field
    @property
    def PROJECTTYPE_VALUE(self) -> Optional[str]:
        """Return the human-readable value for PROJECTTYPE."""
        return PROJECTTYPE_DICT.get(self.PROJECTTYPE, self.PROJECTTYPE) if self.PROJECTTYPE else None

    @computed_field
    @property
    def EXCHANGE_VALUE(self) -> Optional[str]:
        """Return the human-readable value for EXCHANGE."""
        return EXCHANGE_DICT.get(self.EXCHANGE, self.EXCHANGE) if self.EXCHANGE else None

    @computed_field
    @property
    def PROCESSTYPE_VALUE(self) -> Optional[str]:
        """Return the human-readable value for PROCESSTYPE."""
        return PROCESSTYPE_DICT.get(self.PROCESSTYPE, self.PROCESSTYPE) if self.PROCESSTYPE else None

    @computed_field
    @property
    def DATASOURCE_VALUE(self) -> Optional[str]:
        """Return the human-readable value for DATASOURCE."""
        return DATASOURCE_DICT.get(self.DATASOURCE, self.DATASOURCE) if self.DATASOURCE else None

    class Config:
        json_encoders = {
            datetime: lambda dt: dt.strftime("%Y%m%d") if dt else None
        }