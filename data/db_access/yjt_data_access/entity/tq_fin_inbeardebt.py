from sqlalchemy import Column, Integer, String, Numeric, Date, TIMESTAMP
from sqlalchemy.orm import declarative_base
import pandas as pd

Base = declarative_base()


class TqfinInbeardebt(Base):
    """有息债券"""
    __tablename__ = "TQ_FIN_INBEARDEBT"

    # 字段
    reportdate = Column("REPORTDATE", String, primary_key=True, comment="报表日期")
    inbeardebt = Column("INBEARDEBT", Numeric(precision=23, scale=6), comment="有息债务")
    shtdebt = Column("SHTDEBT", Numeric(precision=23, scale=6), comment="短期债务")
    shorttermborr = Column("SHORTTERMBORR", Numeric(precision=23, scale=6), comment="短期借款")
    morshortborr = Column("MORSHORTBORR", Numeric(precision=23, scale=6), comment="短期抵押借款")
    ensshortborr = Column("ENSSHORTBORR", Numeric(precision=23, scale=6), comment="短期保证借款")
    creshortborr = Column("CRESHORTBORR", Numeric(precision=23, scale=6), comment="短期信用借款")
    pleshortborr = Column("PLESHORTBORR", Numeric(precision=23, scale=6), comment="短期质押借款")
    bankdiscloan = Column("BANKDISCLOAN", Numeric(precision=23, scale=6), comment="银行承兑汇票贴现借款")
    comediscloan = Column("COMEDISCLOAN", Numeric(precision=23, scale=6), comment="商业承兑汇票贴现借款")
    notespaya = Column("NOTESPAYA", Numeric(precision=23, scale=6), comment="应付票据")
    shorttermbdspaya = Column("SHORTTERMBDSPAYA", Numeric(precision=23, scale=6), comment="应付短期债券")
    duenoncliab = Column("DUENONCLIAB", Numeric(precision=23, scale=6), comment="一年内到期的非流动负债")
    duelongborr = Column("DUELONGBORR", Numeric(precision=23, scale=6), comment="一年内到期的长期借款")
    duebdspaya = Column("DUEBDSPAYA", Numeric(precision=23, scale=6), comment="一年内到期的应付债券")
    duelongpaya = Column("DUELONGPAYA", Numeric(precision=23, scale=6), comment="一年内到期的长期应付款")
    duefinleases = Column("DUEFINLEASES", Numeric(precision=23, scale=6), comment="一年内到期的应付融资租赁款")
    ltmdebt = Column("LTMDEBT", Numeric(precision=23, scale=6), comment="长期债务")
    longborr = Column("LONGBORR", Numeric(precision=23, scale=6), comment="长期借款")
    morlongborr = Column("MORLONGBORR", Numeric(precision=23, scale=6), comment="长期抵押借款")
    enslongborr = Column("ENSLONGBORR", Numeric(precision=23, scale=6), comment="长期保证借款")
    crelongborr = Column("CRELONGBORR", Numeric(precision=23, scale=6), comment="长期信用借款")
    plelongborr = Column("PLELONGBORR", Numeric(precision=23, scale=6), comment="长期质押借款")
    rduelongborr = Column("RDUELONGBORR", Numeric(precision=23, scale=6), comment="减：一年内到期的长期借款")
    longtermbond = Column("LONGTERMBOND", Numeric(precision=23, scale=6), comment="应付长期债券")
    tranfinaliab = Column("TRANFINALIAB", Numeric(precision=23, scale=6), comment="交易性金融负债")
    apshtfinancing = Column("APSHTFINANCING", Numeric(precision=23, scale=6), comment="应付短期融资款")
    bankdepoandborr = Column("BANKDEPOANDBORR", Numeric(precision=23, scale=6), comment="同业存入及拆入")
    borrfd = Column("BORRFD", Numeric(precision=23, scale=6), comment="借入资金")
    cenbankborr = Column("CENBANKBORR", Numeric(precision=23, scale=6), comment="向中央银行借款")
    cliedeposit = Column("CLIEDEPOSIT", Numeric(precision=23, scale=6), comment="吸收存款")
    depofromcorrbanks = Column("DEPOFROMCORRBANKS", Numeric(precision=23, scale=6), comment="联行存放款项")
    deponetr = Column("DEPONETR", Numeric(precision=23, scale=6), comment="同业存放款项")
    fdsborr = Column("FDSBORR", Numeric(precision=23, scale=6), comment="拆入资金")
    issdepocert = Column("ISSDEPOCERT", Numeric(precision=23, scale=6), comment="发行存款证")
    leaseliab = Column("LEASELIAB", Numeric(precision=23, scale=6), comment="租赁负债")
    sellrepasse = Column("SELLREPASSE", Numeric(precision=23, scale=6), comment="卖出回购金融资产款")

    def to_dict(self):
        return {attr.key: getattr(self, attr.key) for attr in self.__mapper__.attrs}

    def to_comment_dict(self):
        return {col.comment: getattr(self, attr.key) for attr, col in zip(self.__mapper__.attrs, self.__table__.columns)}

    def transpose_comment_dict(self):
        _date_title = "reportdate"
        return [{"指标名称": col.comment, getattr(self, _date_title): getattr(self, attr.key)}
                for attr, col in zip(self.__mapper__.attrs, self.__table__.columns) if attr.key != _date_title]

if __name__ == "__main__":
    arr = [
        TqfinInbeardebt(**{"reportdate": "20240630", "inbeardebt": 200}).to_transpose(),
        TqfinInbeardebt(**{"reportdate": "20231231", "inbeardebt": 300}).to_transpose(),
        TqfinInbeardebt(**{"reportdate": "20221231", "inbeardebt": 400}).to_transpose()
    ]

    df = pd.concat([pd.DataFrame(ele).set_index("指标名称") for ele in arr], axis=1)
    print(df.reset_index())