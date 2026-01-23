from data.db_access.mysql_base_dao import MySQLDao


class TqCompInfoDao(MySQLDao):

    def select_comp_info(self, com_code):
        """公司编码，主体评级，所属区域，实际控制人"""
        query = """
        select COMPCODE, COMPNAME, MAJORBIZ
        from TQ_COMP_INFO
        where COMPCODE = :comCode 
        """
        params = {"comCode": com_code}
        result = self.select_one_by_sql(query, params)
        return result

    def select_comp_code(self, com_name):
        """查询公司编码"""
        query = "select COMPCODE from TQ_COMP_INFO where ISVALID=1 and COMPNAME=:comName"
        params = {"comName": com_name}
        result = self.select_one_by_sql(query, params)
        if result is not None:
            return result.get("COMPCODE")
