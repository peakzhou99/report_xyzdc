from sqlalchemy import Column, String, Numeric
from sqlalchemy.orm import declarative_base
Base = declarative_base()

class TqfinPrgcfstatementnew(Base):
    """现金流量"""
    __tablename__ = "TQ_FIN_PRGCFSTATEMENTNEW"
    # 字段
    enddate = Column("ENDDATE", String, primary_key=True, comment="截止时间")
    mananetr = Column("MANANETR", Numeric(precision=19, scale=2), comment="经营活动产生的现金流量净额")
    invnetcashflow = Column("INVNETCASHFLOW", Numeric(precision=19, scale=2), comment="投资活动产生的现金流量净额")
    finnetcflow = Column("FINNETCFLOW", Numeric(precision=19, scale=2), comment="筹资活动产生的现金流量净额")

    def to_dict(self):
        return {attr.key: getattr(self, attr.key) for attr in self.__mapper__.attrs}

    def to_comment_dict(self):
        return {col.comment: getattr(self, attr.key) for attr, col in
                zip(self.__mapper__.attrs, self.__table__.columns)}

    def transpose_comment_dict(self):
        _date_title = "enddate"
        return [{"指标名称": col.comment, getattr(self, _date_title): getattr(self, attr.key)}
                for attr, col in zip(self.__mapper__.attrs, self.__table__.columns) if attr.key != _date_title]

