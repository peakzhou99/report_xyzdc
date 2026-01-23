from sqlalchemy import Column, Integer, String, Numeric
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class TqbdCreditlinedetails(Base):
    """授信额度"""

    __tablename__ = "TQ_BD_CREDITLINEDETAILS"

    # 字段
    enddate = Column("ENDDATE", String, comment="截止时间")
    unit = Column("UNIT", String, comment="单位")
    creditcompname = Column("CREDITCOMPNAME", String, primary_key=True, comment="授信机构名称")
    creditline = Column("CREDITLINE", Numeric(precision=19, scale=2), comment="授信额度")
    usedquota = Column("USEDQUOTA", Numeric(precision=19, scale=2), comment="已使用额度")
    unusedquota = Column("UNUSEDQUOTA", Numeric(precision=19, scale=2), comment="未使用额度")

    def to_dict(self):
        return {attr.key: getattr(self, attr.key) for attr in self.__mapper__.attrs}

    def to_comment_dict(self):
        return {col.comment: getattr(self, attr.key) for attr, col in zip(self.__mapper__.attrs, self.__table__.columns)}