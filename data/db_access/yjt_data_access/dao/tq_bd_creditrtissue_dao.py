from data.db_access.mysql_base_dao import MySQLDao

class TqBdCreditrtissueDao(MySQLDao):

    def select_credit_rate(self,compCode):
        """查询主体评级"""
        query = "select CREDITRATE from TQ_BD_CREDITRTISSUE where COMPCODE=:compCode and ISVALID=1 order by PUBLISHDATE desc limit 1"
        params = {"compCode": compCode}
        result = self.select_one_by_sql(query, params)
        if result is not None:
            return result.get("CREDITRATE")

