from data.db_access.mysql_base_dao import MySQLDao
from data.db_access.yjt_data_access.entity.tq_sk_shareholder import TqskShareholder

class TqskShareholderDao(MySQLDao):

    def select_share_holder(self, com_code):
        query = """
        SELECT t1.ENDDATE,t1.SHHOLDERCODE,t1.SHHOLDERNAME,t1.HOLDERAMT,t1.HOLDERRTO
        FROM TQ_SK_SHAREHOLDER t1
        join (select max(UPDATEDATE) UPDATEDATE 
            from TQ_SK_SHAREHOLDER 
            where COMPCODE=:compCode
        ) t2 
          on t1.ISVALID = 1 
         AND t1.COMPCODE=:compCode 
         AND t1.UPDATEDATE=t2.UPDATEDATE
        """

        params = {"compCode": com_code}
        result = self.select_list_by_sql(query, params, TqskShareholder)
        return result