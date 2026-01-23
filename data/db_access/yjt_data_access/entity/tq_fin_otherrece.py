from sqlalchemy import Column, String, Numeric, TEXT
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class TqfinOtherrece(Base):
    """其他应收"""
    __tablename__ = "TQ_FIN_OTHERRECE"

    # 字段
    enddate = Column("ENDDATE", String, comment="截止日期")
    fnotesproname = Column("FNOTESPRONAME", TEXT, primary_key=True, comment="项目名称")
    amtep = Column("AMTEP", Numeric(precision=19, scale=2), comment="期末余额")
    ratio = Column("RATIO", String, comment="比例")

    def to_dict(self):
        return {attr.key: getattr(self, attr.key) for attr in self.__mapper__.attrs}

    def to_comment_dict(self):
        return {col.comment: getattr(self, attr.key) for attr, col in zip(self.__mapper__.attrs, self.__table__.columns)}

if __name__ == '__main__':
    obj = TqfinOtherrece()
    print(obj.to_dict())