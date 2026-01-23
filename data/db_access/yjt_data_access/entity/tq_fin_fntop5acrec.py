from sqlalchemy import Column, Integer, String, Numeric, TEXT
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class TqfinFntop5acrec(Base):
    """应收账款"""
    __tablename__ = "TQ_FIN_FNTOP5ACREC"

    # 字段
    enddate = Column("ENDDATE", String, comment="报表日期")
    arlitname = Column("ARLITNAME", TEXT, primary_key=True, comment="机构名称")
    amount = Column("AMOUNT", Numeric(precision=23, scale=2), comment="金额")
    arlratio = Column("ARLRATIO", Numeric(precision=23, scale=2), comment="比例")

    def to_dict(self):
        return {attr.key: getattr(self, attr.key) for attr in self.__mapper__.attrs}

    def to_comment_dict(self):
        return {col.comment: getattr(self, attr.key) for attr, col in zip(self.__mapper__.attrs, self.__table__.columns)}