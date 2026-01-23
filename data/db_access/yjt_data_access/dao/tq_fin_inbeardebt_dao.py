from data.db_access.mysql_base_dao import MySQLDao
from data.db_access.yjt_data_access.entity.tq_fin_inbeardebt import TqfinInbeardebt

class TqfinInbeardebtDao(MySQLDao):
    """有息负债"""

    def select_inbeardebt(self, com_code):
        """查询公司编码"""
        query = """
        select 
            t.REPORTDATE
            ,round(t.INBEARDEBT/10000 ,2) INBEARDEBT
            ,round(t.SHTDEBT/10000   ,2) SHTDEBT
            ,round(t.SHORTTERMBORR/10000 ,2) SHORTTERMBORR
            ,round(t.MORSHORTBORR/10000  ,2) MORSHORTBORR
            ,round(t.ENSSHORTBORR/10000  ,2) ENSSHORTBORR
            ,round(t.CRESHORTBORR/10000  ,2) CRESHORTBORR
            ,round(t.PLESHORTBORR/10000  ,2) PLESHORTBORR
            ,round(t.BANKDISCLOAN/10000  ,2) BANKDISCLOAN
            ,round(t.COMEDISCLOAN/10000  ,2) COMEDISCLOAN
            ,round(t.NOTESPAYA/10000     ,2) NOTESPAYA
            ,round(t.SHORTTERMBDSPAYA/10000 ,2) SHORTTERMBDSPAYA
            ,round(t.DUENONCLIAB/10000   ,2) DUENONCLIAB
            ,round(t.DUELONGBORR/10000   ,2) DUELONGBORR
            ,round(t.DUEBDSPAYA/10000    ,2) DUEBDSPAYA
            ,round(t.DUELONGPAYA/10000   ,2) DUELONGPAYA
            ,round(t.DUEFINLEASES/10000  ,2) DUEFINLEASES
            ,round(t.LTMDEBT/10000       ,2) LTMDEBT
            ,round(t.LONGBORR/10000      ,2) LONGBORR
            ,round(t.MORLONGBORR/10000   ,2) MORLONGBORR
            ,round(t.ENSLONGBORR/10000   ,2) ENSLONGBORR
            ,round(t.CRELONGBORR/10000   ,2) CRELONGBORR
            ,round(t.PLELONGBORR/10000  ,2) PLELONGBORR
            ,round(t.RDUELONGBORR/10000  ,2) RDUELONGBORR
            ,round(t.LONGTERMBOND/10000  ,2) LONGTERMBOND
            ,round(t.TRANFINALIAB/10000   ,2) TRANFINALIAB
            ,round(t.APSHTFINANCING/10000,2) APSHTFINANCING
            ,round(t.BANKDEPOANDBORR/10000,2) BANKDEPOANDBORR
            ,round(t.BORRFD/10000        ,2) BORRFD
            ,round(t.CENBANKBORR/10000   ,2) CENBANKBORR
            ,round(t.CLIEDEPOSIT/10000   ,2) CLIEDEPOSIT
            ,round(t.DEPOFROMCORRBANKS/10000,2) DEPOFROMCORRBANKS
            ,round(t.DEPONETR/10000       ,2) DEPONETR
            ,round(t.FDSBORR/10000       ,2) FDSBORR
            ,round(t.ISSDEPOCERT/10000   ,2) ISSDEPOCERT
            ,round(t.LEASELIAB/10000     ,2) LEASELIAB
            ,round(t.SELLREPASSE/10000   ,2) SELLREPASSE
        from (
            select t1.*
                  ,row_number() over(partition by t2.REPORTYEAR order by REPORTDATE desc) as row_num
            from TQ_FIN_INBEARDEBT t1
            join (
                select distinct substr(REPORTDATE,1,4) as REPORTYEAR
                from TQ_FIN_INBEARDEBT
                where ITCODE=:comCode and reportrange='1'
                order by REPORTYEAR desc
                limit 3
            ) t2 on t1.ITCODE=:comCode and t1.reportrange='1' and substr(t1.REPORTDATE,1,4)=t2.REPORTYEAR
        ) t where t.row_num=1
        """

        params = {"comCode": com_code}
        result = self.select_list_by_sql(query, params, TqfinInbeardebt)
        return result