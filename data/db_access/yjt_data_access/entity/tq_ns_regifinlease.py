from sqlalchemy import Column, String, Numeric
from sqlalchemy.orm import declarative_base
Base = declarative_base()

class TqnsRegifinlease(Base):
    """非标融资"""

    __tablename__ = "TQ_NS_REGIFINLEASE"

    # 字段
    reportdate = Column("REPORTDATE", String, comment="截止日期")
    finname = Column("FINNAME", String, primary_key=True, comment="融资方")
    ftype = Column("FTYPE", String, comment="非标类型")
    creditname = Column("CREDITNAME", String, primary_key=True, comment="债权人/项目")
    finbalance = Column("FINBALANCE", Numeric(precision=19, scale=2), comment="融资余额（亿元）")
    inrate = Column("INRATE", Numeric(precision=8, scale=2), comment="融资利率")
    duration = Column("DURATION", String, comment="期限")
    loanbegindate = Column("LOANBEGINDATE", String, comment="起始日期")
    loanenddate = Column("LOANENDDATE", String, comment="到期日")

    def to_dict(self):
        return {attr.key: getattr(self, attr.key) for attr in self.__mapper__.attrs}

    def to_comment_dict(self):
        return {col.comment: getattr(self, attr.key) for attr, col in
                zip(self.__mapper__.attrs, self.__table__.columns)}


if __name__ == "__main__":
    tqnsRegifinlease = TqnsRegifinlease(**{})
    print(tqnsRegifinlease.to_comment_dict())
