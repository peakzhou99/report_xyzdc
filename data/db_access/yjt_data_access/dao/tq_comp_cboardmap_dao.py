from data.db_access.mysql_base_dao import MySQLDao

class TqCompCboardmapDao(MySQLDao):

    def select_cboardmap_info(self,com_code):
        """查询所属区域"""
        query = """
        select BOARDCODE,KEYCODE,KEYNAME from TQ_COMP_CBOARDMAP where COMPCODE=:compCode and BOARDCODE in ('1101','1102','1103')
        """
        params = {"compCode": com_code}
        result = self.select_list_by_sql(query, params)
        return result