from sqlalchemy import Column, Integer, String, Numeric
from sqlalchemy.orm import declarative_base
Base = declarative_base()


class TqskShareholder(Base):
    """股权结构"""

    __tablename__ = "TQ_SK_SHAREHOLDER"

    # 字段
    enddate = Column("ENDDATE", String, comment="截止时间")
    shholdercode = Column("SHHOLDERCODE", String, primary_key=True, comment="股东代码")
    shholdername = Column("SHHOLDERNAME", String, comment="股东名称")
    holderamt = Column("HOLDERAMT", Numeric(precision=8, scale=2), comment="持股数量")
    holderrto = Column("HOLDERRTO", Numeric(precision=8, scale=2), comment="持股比例")

    def to_dict(self):
        return {attr.key: getattr(self, attr.key) for attr in self.__mapper__.attrs}

    def to_comment_dict(self):
        return {col.comment: getattr(self, attr.key) for attr, col in zip(self.__mapper__.attrs, self.__table__.columns)}

if __name__ == "__main__":
    tqskShareholder = TqskShareholder(**{})
    print(tqskShareholder.to_comment_dict())
