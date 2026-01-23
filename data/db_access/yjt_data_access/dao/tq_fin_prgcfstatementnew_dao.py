from data.db_access.mysql_base_dao import MySQLDao
from data.db_access.yjt_data_access.entity.tq_fin_prgcfstatementnew import TqfinPrgcfstatementnew

class TqfinPrgcfstatementnewDao(MySQLDao):
    """单位：亿元"""

    def select_cash_flow(self, com_code):
        """查询公司编码"""
        query = """
        select t.ENDDATE
              ,round(t.MANANETR,2) MANANETR
              ,round(t.INVNETCASHFLOW,2) INVNETCASHFLOW
              ,round(t.FINNETCFLOW,2) FINNETCFLOW
        from (
            select t1.REPORTYEAR,t1.ENDDATE
                  ,round(t1.MANANETR/100000000,2) MANANETR
                  ,round(t1.INVNETCASHFLOW/100000000,2) INVNETCASHFLOW
                  ,round(t1.FINNETCFLOW/100000000,2) FINNETCFLOW
                  ,row_number() over(partition by t1.REPORTYEAR order by t1.ENDDATE desc) as row_num
            from TQ_FIN_PRGCFSTATEMENTNEW t1
            join (
                select distinct REPORTYEAR
                from TQ_FIN_PRGCFSTATEMENTNEW
                where REPORTTYPE='1' and COMPCODE=:comCode
                order by REPORTYEAR desc
                limit 3
            ) t2
              on t1.REPORTTYPE='1' and t1.COMPCODE=:comCode and t1.REPORTYEAR=t2.REPORTYEAR
        ) t
        where t.row_num = 1
        """

        params = {"comCode": com_code}
        result = self.select_list_by_sql(query, params, TqfinPrgcfstatementnew)
        return result