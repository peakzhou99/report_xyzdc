from data.db_access.mysql_base_dao import MySQLDao
from datetime import datetime, timedelta, date
from data.db_access.yjt_data_access.entity.tq_bd_basicinfo import TqbdBasicInfo

class TqbdBasicinfoDao(MySQLDao):
    def select_outstanding_bonds(self, comCode):
        # 存量债券
        query = """
        select 
            t1.SYMBOL,t1.BONDSNAME,t1.BONDTYPE1,t1.INITIALCREDITRATE
            ,round(t1.TOTALISSUESCALE/10000,2) TOTALISSUESCALE
            ,round(datediff(str_to_date(MATURITYDATE,'%Y%m%d'),curdate())/365,1) REMAINTERM
            ,t1.ISSBEGDATE
            ,t1.RAISEMODE
            ,IF(CVTBDEXPIREMEMP is null,round(MATURITYYEAR,0),CVTBDEXPIREMEMP) MATURITYYEAR
            ,round(COUPONRATE,2) COUPONRATE
            ,MATURITYDATE
            ,LEADUWER
            ,round(t3.CURRENTAMT,2) CURRENTAMT
        from (
            select b.*,
                row_number() over(partition by securityid order by exchg_type) rn
            from (
                select a.*,
                case when exchange='001005' then '0'
                    when exchange='001002' then '1'
                    when exchange='001003' then '2'
                    when exchange='001007' then '3'
                    when exchange='001006' then '4'
                    when exchange='001018' then '5'
                    else '6' END exchg_type
                FROM TQ_BD_BASICINFO a
                WHERE ISSUECOMPCODE=:comCode and round(MATURITYYEAR,0)>0
            ) b
        ) t1
        join (
            select distinct securityid
            from TQ_BD_BASICINFO
            where ISVALID = 1
            AND ISSUECOMPCODE=:comCode
            AND MATURITYDATE>=:today
            AND BONDNAME not like '%回拨%' AND BONDNAME NOT LIKE '%发行失败%' AND BONDNAME NOT LIKE '%取消发行%'
        ) t2 on t2.securityid=t1.securityid and t1.rn=1
        join (
            select distinct securityid,CURRENTAMT
            from TQ_BD_NEWESTBASICINFO
            where ISSUECOMPCODE=:comCode
        ) t3 on t3.securityid=t2.securityid
        """

        today = datetime.today().strftime("%Y%m%d")
        params = {"comCode":comCode,"today":today}
        result = self.select_list_by_sql(query, params, TqbdBasicInfo)
        return result