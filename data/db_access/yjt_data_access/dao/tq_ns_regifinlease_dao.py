from data.db_access.mysql_base_dao import MySQLDao
from data.db_access.yjt_data_access.entity.tq_ns_regifinlease import TqnsRegifinlease

class TqnsRegifinleaseDao(MySQLDao):

    def select_fin_lease(self, com_code):
        """非标融资"""
        query = """
        select concat(t1.REPORTYEAR,"1231") as REPORTDATE
              ,IF(t1.FINNAME is null,t1.ITNAME,t1.FINNAME) FINNAME
              ,t1.FTYPE,t1.CREDITNAME
              ,round(t1.FINBALANCE/100000000,2) FINBALANCE
              ,t1.INRATE,t1.DURATION,t1.LOANBEGINDATE,t1.LOANENDDATE
        from TQ_NS_REGIFINLEASE t1
        join (
            select max(REPORTYEAR) as REPORTYEAR
            from TQ_NS_REGIFINLEASE
            where ITCODE=:comCode
        ) t2 on t1.ITCODE=:comCode and t1.REPORTYEAR=t2.REPORTYEAR
        """

        params = {"comCode": com_code}
        result = self.select_list_by_sql(query, params, TqnsRegifinlease)
        return result