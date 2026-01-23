from sqlalchemy import Column, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class TqcibdNewregifinaplat(Base):
    """发行平台区域城投平台"""
    __tablename__ = "TQ_CIBD_NEWREGIFINAPLAT"

    # 字段
    itcode = Column('ITCODE', String, primary_key=True, comment='公司代码')
    itname = Column('ITNAME', String, comment='公司名称')
    CREDITRATE = Column('CREDITRATE', String, comment='主体评级')
    PROVINCE = Column('PROVINCE', String, comment='省')
    CITY = Column('CITY', String, comment='市')
    COUNTRY = Column('COUNTRY', String, comment='区')
    ITNAME_P = Column('ITNAME_P', String, comment='实际控制人')

    def to_dict(self):
        return {attr.key: getattr(self, attr.key) for attr in self.__mapper__.attrs}

    def to_comment_dict(self):
        return {
            col.comment: getattr(self, attr.key)
            if self.type_code_name().get(col.name) is None
            else self.type_code_name().get(col.name).get(str(getattr(self, attr.key)))
            for attr, col in zip(self.__mapper__.attrs, self.__table__.columns)}

    def type_code_name(self):
        return {
            "TERRITORYTYPE": {
                "地市级": "短期融资券",
                "省直辖市级": "省级",
                "县市级": "区县级",
            }
        }
