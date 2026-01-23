from data.db_access.mysql_base_dao import MySQLDao
from data.db_access.yjt_data_access.entity.tq_bd_creditlinedetails import TqbdCreditlinedetails
from datetime import datetime, timedelta, date

class TqbdCreditlinedetailsDao(MySQLDao):
    def select_credit_line(self, comCode):
        query = """
        select t2.ENDDATE
              ,'亿' UNIT
              ,t1.CREDITCOMPNAME
              ,round(t1.CREDITLINE,2) CREDITLINE
              ,round(t1.USEDQUOTA,2) USEDQUOTA
              ,round(t1.UNUSEDQUOTA,2) UNUSEDQUOTA
        from tq_bd_creditlinedetails t1
        join (
            select t21.CREDITLINEID,t21.ENDDATE
            from tq_bd_creditline t21
            join (select max(ENDDATE) ENDDATE from tq_bd_creditline where COMPCODE=:comCode and ISVALID=1) t22
            on t22.ENDDATE=t21.ENDDATE and t21.COMPCODE=:comCode and t21.ISVALID=1
        ) t2
        on t2.CREDITLINEID=t1.CREDITLINEID
        """
        params = {"comCode": comCode}
        result = self.select_list_by_sql(query, params, TqbdCreditlinedetails)
        return result