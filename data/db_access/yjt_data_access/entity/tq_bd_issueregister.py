from sqlalchemy import Column, Integer, String, Numeric
from sqlalchemy.orm import declarative_base
Base = declarative_base()

class TqbdIssueregister(Base):
    """DCM融资"""

    __tablename__ = "TQ_BD_ISSUEREGISTER"

    # 字段
    bondtype = Column("BONDTYPE", String, comment="债券类型")
    registerlimit = Column("REGISTERLIMIT", Numeric(precision=19, scale=2), comment="注册额度")
    actissamt = Column("ACTISSAMT", Numeric(precision=19, scale=2), comment="累计使用")
    currentamt = Column("CURRENTAMT", Numeric(precision=19, scale=2), comment="已使用")
    unusedamt = Column("UNUSEDAMT", Numeric(precision=19, scale=2), comment="未使用")
    registerbegindate = Column("REGISTERBEGINDATE", String, primary_key=True, comment="有效起始日")
    registerenddate = Column("REGISTERENDDATE", String, comment="有效终止日")

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
            "BONDTYPE": {
                "1": "短期融资券",
                "10": "债务融资工具(DFI)",
                "11": "自贸试验区债务融资工具",
                "12": "主权政府人民币债券",
                "13": "扶贫票据(PAN)",
                "14": "乡村振兴票据(RVN)",
                "15": "债务融资工具(TDFI)",
                "16": "资产担保债务融资工具（CB）",
                "2": "中期票据",
                "3": "集合票据",
                "4": "超短期融资券",
                "5": "信用风险缓释凭证",
                "6": "定向工具",
                "7": "资产支持票据",
                "8": "项目收益票据",
                "9": "绿色债务融资工具",
            }
        }

if __name__ == "__main__":
    obj1 = TqbdIssueregister(**{"bondtype": "2"})
    print(obj1.to_comment_dict())
