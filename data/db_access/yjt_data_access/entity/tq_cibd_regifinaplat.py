from sqlalchemy import Column, Integer, String, Numeric
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class TqcibdRegifinaplat(Base):
    """发行平台区域城投平台"""
    __tablename__ = "TQ_CIBD_REGIFINAPLAT"

    # 字段
    reportdate = Column('REPORTDATE', String, primary_key=True, comment='截止时间')
    unit = Column('UNIT', String, comment='单位')
    itcode = Column('ITCODE', String, comment='公司代码')
    itname = Column('ITNAME', String, comment='公司名称')
    finaffcode = Column('FINAFFCODE', String, comment='区域编码')
    finaffname = Column('FINAFFNAME', String, comment='区域名称')
    territorytype = Column('TERRITORYTYPE', String, comment='行政级别')
    assetscale = Column('ASSETSCALE', Numeric(precision=20, scale=2), comment='总资产')

    def to_dict(self):
        return {attr.key: getattr(self, attr.key) for attr in self.__mapper__.attrs}

    def to_comment_dict(self):
        return {col.comment: getattr(self, attr.key)
            if self.type_code_name().get(col.name) is None
            else self.type_code_name().get(col.name).get(str(getattr(self, attr.key)))
                for attr, col in zip(self.__mapper__.attrs, self.__table__.columns)}

    def type_code_name(self):
        return {
            "TERRITORYTYPE": {
                "省级": "省级",
                "地市级": "地市级",
                "区县级": "区县级",
                "短期融资券": "地市级",
                "省直辖市级": "省级",
                "县市级": "区县级",
            }
        }
