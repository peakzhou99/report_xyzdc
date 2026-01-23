from sqlalchemy import Column, Integer, String, Numeric
from sqlalchemy.orm import declarative_base
Base = declarative_base()


class TqcibdRegifinNew(Base):
    """区域经济"""
    __tablename__ = "TQ_CIBD_REGIFIN_NEW"

    # 字段
    regionCode = Column("REGIONCODE", String, primary_key=True, comment="区域编码")
    regionName = Column("REGIONNAME", String, comment="区域名称")
    gdp = Column("GDP", Numeric(precision=16, scale=2), comment="GDP")
    gpBugetRev = Column("GP_BUGET_REV", Numeric(precision=16, scale=2), comment="一般公共预算收入")
    govFundRev = Column("GOV_FUND_REV", Numeric(precision=16, scale=2), comment="政府性基金收入")
    broadDebtRatio = Column("BROAD_DEBT_RATIO", Numeric(precision=16, scale=2), comment="债务率（宽口径）")
    fssRatio = Column("FSS_RATIO", Numeric(precision=16, scale=4), comment="财政自给率")
    endDate = Column("ENDDATE", String, comment="截止日期")

    def to_dict(self):
        return {attr.key: getattr(self, attr.key) for attr in self.__mapper__.attrs}

    def to_comment_dict(self):
        return {col.comment: getattr(self, attr.key) for attr,col in zip(self.__mapper__.attrs, self.__table__.columns)}