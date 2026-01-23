from data.db_access.mysql_base_dao import MySQLDao
from data.db_access.yjt_data_access.entity.tq_fin_fntop5acrec import TqfinFntop5acrec

class TqfinFntop5acrecDao(MySQLDao):
    """有息负债"""

    def select_ac_rec(self, com_code):
        """应收账款"""
        query = """
        select t.ENDDATE,t.ARLITCODE,t.ARLITNAME,t.AMOUNT,t.ARLRATIO
        from (
            select s.ENDDATE,s.ARLITCODE,s.ARLITNAME,s.AMOUNT,s.ARLRATIO
                  ,row_number() over(partition by s.ENDDATE order by s.AMOUNT desc) as row_num
            from TQ_FIN_FNTOP5ACREC s
            join (
                select r.ENDDATE
                from (
                    select t1.ENDDATE
                          ,row_number() over(partition by t2.ENDYEAR order by t1.ENDDATE desc) as row_num
                    from (select * from TQ_FIN_FNTOP5ACREC where ARLITCODE !='10000' or ARLITCODE is null) t1
                    join (
                        select distinct substr(ENDDATE,1,4) as ENDYEAR
                        from TQ_FIN_FNTOP5ACREC
                        where COMPCODE=:comCode and reportrange='1'
                        order by ENDYEAR desc
                        limit 1
                    ) t2 on t1.COMPCODE=:comCode and t1.reportrange='1' and substr(t1.ENDDATE,1,4)=t2.ENDYEAR
                ) r where r.row_num=1
            ) k
              where s.ENDDATE=k.ENDDATE and (s.ARLITCODE !='10000' or s.ARLITCODE is null)
              and s.COMPCODE=:comCode and s.reportrange='1'
        ) t where t.row_num<=5
        """

        params = {"comCode": com_code}
        result = self.select_list_by_sql(query, params, TqfinFntop5acrec)
        return result