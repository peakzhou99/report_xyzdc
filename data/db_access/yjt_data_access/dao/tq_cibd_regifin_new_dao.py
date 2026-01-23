from data.db_access.mysql_base_dao import MySQLDao
from datetime import datetime, timedelta, date
from data.db_access.yjt_data_access.entity.tq_cibd_regifin_new import TqcibdRegifinNew


class TqcibdRegifinNewDao(MySQLDao):
    def select_national_eco(self):
        """查询全国所有省份"""
        query = """
        select
            ENDDATE
            ,REGIONCODE
            ,REGIONNAME
            ,round(sum(if(INDICCODE='0101',MVALUE,0)),2) as GDP
            ,round(sum(if(INDICCODE='0201',MVALUE,0)),2) as GP_BUGET_REV
            ,round(sum(if(INDICCODE='0723',MVALUE,0)),2) as GOV_FUND_REV
            ,round(sum(if(INDICCODE='1206',MVALUE,0)),2) as BROAD_DEBT_RATIO
            ,round(sum(if(INDICCODE='1201',MVALUE,0)),2) as FSS_RATIO
        from TQ_CIBD_REGIFIN_NEW t1
        join (select max(NYEAR) NYEAR from TQ_CIBD_REGIFIN_NEW where REGIONCODE='110000' and INDICCODE='0101') t2
        on t1.NYEAR=t2.NYEAR and t1.INDICCODE in ('0101','0201','0723','1206','1201') and right(t1.REGIONCODE,4)='0000'
        group by ENDDATE,REGIONCODE,REGIONNAME
        """
        params = None
        result = self.select_list_by_sql(query,params,TqcibdRegifinNew)
        return result

    def select_local_eco(self,province):
        """地方区域经济"""
        query = """
        select
            ENDDATE
            ,REGIONCODE
            ,REGIONNAME
            ,round(sum(if(INDICCODE='0101',MVALUE,0)),2) as GDP
            ,round(sum(if(INDICCODE='0201',MVALUE,0)),2) as GP_BUGET_REV
            ,round(sum(if(INDICCODE='0723',MVALUE,0)),2) as GOV_FUND_REV
            ,round(sum(if(INDICCODE='1206',MVALUE,0)),2) as BROAD_DEBT_RATIO
            ,round(sum(if(INDICCODE='1201',MVALUE,0)),2) as FSS_RATIO
        from TQ_CIBD_REGIFIN_NEW t1
        join (select max(NYEAR) NYEAR from TQ_CIBD_REGIFIN_NEW where REGIONCODE='110000' and INDICCODE='0101') t2
        on t1.NYEAR=t2.NYEAR and t1.INDICCODE in ('0101','0201','0723','1206','1201') 
        and instr(t1.REGIONNAME,:province)>0
        group by ENDDATE,REGIONCODE,REGIONNAME
        """
        params = {"province":province}
        result = self.select_list_by_sql(query, params, TqcibdRegifinNew)
        return result