from data.db_access.mysql_base_dao import MySQLDao
from data.db_access.yjt_data_access.entity.tq_fin_otherrece import TqfinOtherrece

class TqfinOtherreceDao(MySQLDao):
    """有息负债"""

    def select_other_rece(self, com_code):
        """应收账款"""
        query = """
        select t.ENDDATE,t.FNOTESPRONAME
              ,round(t.AMTEP/10000,2) AMTEP
              ,t.RATIO
        from (
            select s.ENDDATE,s.REPORTYEAR,s.FNOTESPRONAME,s.AMTEP,REGEXP_REPLACE(s.MEMO,'[^0-9.]+', '') RATIO
                  ,row_number() over(partition by s.ENDDATE order by s.AMTEP desc) as row_num
            from TQ_FIN_OTHERRECE s
            join (
                select r.ENDDATE
                from (
                    select t1.ENDDATE
                          ,row_number() over(partition by t2.REPORTYEAR order by t1.ENDDATE desc) as row_num
                    from (select * from TQ_FIN_OTHERRECE where FNOTESPROCODE!='10000' or FNOTESPROCODE is null) t1
                    join (
                        select distinct REPORTYEAR
                        from TQ_FIN_OTHERRECE
                        where COMPCODE=:comCode and REPORTTYPE='1'
                        order by REPORTYEAR desc
                        limit 1
                    ) t2 on t1.COMPCODE=:comCode and t1.REPORTTYPE='1' and t1.REPORTYEAR=t2.REPORTYEAR
                ) r where r.row_num=1
            ) k
              where s.ENDDATE=k.ENDDATE and (s.FNOTESPROCODE!='10000' or s.FNOTESPROCODE is null)
              and s.COMPCODE=:comCode and s.REPORTTYPE='1'
        ) t where t.row_num<=5
        """

        params = {"comCode": com_code}
        result = self.select_list_by_sql(query, params, TqfinOtherrece)
        return result