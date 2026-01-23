from data.db_access.mysql_base_dao import MySQLDao
from data.db_access.yjt_data_access.entity.tq_cibd_newregifinaplat import TqcibdNewregifinaplat

class TqcibdNewregifinaplatDao(MySQLDao):

    def select_region_and_credit_rate(self,com_code):
        """区域评级"""
        query = """
        select ITCODE,ITNAME,CREDITRATE,REGLANNAME_P as PROVINCE,REGLANNAME_C AS CITY,COUNTRY,ITNAME_P 
        from TQ_CIBD_NEWREGIFINAPLAT where ITCODE=:comCode
        """
        params = {"comCode": com_code}
        result = self.select_list_by_sql(query, params,TqcibdNewregifinaplat)
        return result