from sqlalchemy import Column, Integer, String, Numeric
from sqlalchemy.orm import declarative_base
import json
Base = declarative_base()


class TqbdBasicInfo(Base):
    """存量债券"""
    __tablename__ = "TQ_BD_BASICINFO"

    # 字段
    symbol = Column("SYMBOL", String, primary_key=True, comment="债券代码")
    bondsname = Column("BONDSNAME", String, comment="债券简称")
    bondtype1 = Column("BONDTYPE1", String, comment="债券类型")
    initialcreditrate = Column("INITIALCREDITRATE", String, comment="债券评级")
    currentamt = Column("CURRENTAMT", Numeric(precision=16, scale=2), comment="债券余额")
    remainterm = Column("REMAINTERM", String, comment="剩余期限")
    totalissuescale = Column("TOTALISSUESCALE", Numeric(precision=16, scale=2), comment="发行规模")
    issbegdate = Column("ISSBEGDATE", String, comment="发行日期")
    raisemode = Column("RAISEMODE", String, comment="募集方式")
    maturityyear = Column("MATURITYYEAR", String, comment="债券期限")
    couponrate = Column("COUPONRATE", Numeric(precision=9, scale=2), comment="票面利率")
    maturitydate = Column("MATURITYDATE", String, comment="到期日期")

    def to_dict(self):
        return {attr.key: getattr(self, attr.key) for attr in self.__mapper__.attrs}

    def to_comment_dict(self):
        return {
            col.comment: getattr(self, attr.key)
            if self.type_code_name().get(col.name) is None
            else self.type_code_name().get(col.name).get(str(getattr(self, attr.key)))
            for attr, col in zip(self.__mapper__.attrs, self.__table__.columns)
        }

    def type_code_name(self):
        return {
            "BONDTYPE1": {
                "1": "政府债券",
                "10": "项目收益票据",
                "11": "大额存单",
                "2": "央行票据",
                "3": "政府支持机构债券",
                "4": "国际开发机构债券",
                "5": "金融债",
                "6": "企业(公司)债券",
                "7": "资产支持证券",
                "8": "资产支持票据",
                "9": "大额可转让同业存单"
            },
            "RAISEMODE": {
                "1": "公募债券",
                "2": "私募债券"
            }
        }

if __name__ == "__main__":
    item = {"SYMBOL": "200222",
            "BONDSNAME": "202简称",
            "BONDTYPE": "223",
            "INITIALCREDITRATE": "22",
            "BONDBALANCE": "232323",
            "text": ""
            }

    column_mappings = {col.name.upper(): attr.key for col, attr in
                       zip(TqbdBasicInfo.__mapper__.columns, TqbdBasicInfo.__mapper__.attrs)}
    print(column_mappings)
    _data = {column_mappings[key]: value for key, value in item.items() if key in column_mappings.keys()}
    print(_data)
    basicinfo = TqbdBasicInfo(**_data)
    print(basicinfo.to_dict())
    print(basicinfo.to_comment_dict())