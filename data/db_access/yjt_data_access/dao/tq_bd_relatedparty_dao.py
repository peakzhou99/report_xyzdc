from data.db_access.mysql_base_dao import MySQLDao

class TqBdRelatedpartyDao(MySQLDao):

    def select_relatedparty(self,compCode):
        """查询实际控制人"""
        query = "select * from TQ_COMP_RELATEDPARTY where COMPCODE=:compCode and RELATYPECODE='A' order by ENTRYDATE desc limit 1"
        params = {"compCode": compCode}
        result = self.select_one_by_sql(query, params)
        if result is not None:
            return result.get("RELANAME")