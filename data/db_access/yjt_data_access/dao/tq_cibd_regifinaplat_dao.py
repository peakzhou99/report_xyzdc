from data.db_access.mysql_base_dao import MySQLDao
from data.db_access.yjt_data_access.entity.tq_cibd_regifinaplat import TqcibdRegifinaplat


class TqcibdRegifinaplatDao(MySQLDao):
    def select_comcode(self,com_name):
        """查询公司编码"""
        query = "select ITCODE from tq_cibd_regifinaplat where ISVALID=1 and ITNAME=:comName"
        params = {"comName":com_name}
        result = self.select_one_by_sql(query, params)
        if not result:
            raise Exception(f"{com_name}预警通未列为城投平台，无法生成。")
        return result.get("ITCODE")

    def select_region(self, com_name):
        """查询区域"""
        query = "select FINAFFNAME from tq_cibd_regifinaplat where ISVALID=1 and ITNAME=:comName"
        params = {"comName": com_name}
        result = self.select_one_by_sql(query, params)
        if result:
            return result.get("FINAFFNAME")

    def select_region_companys(self, region):
        """区域城投平台"""
        query = """
        select t2.REPORTDATE
        ,'亿' UNIT
        ,t1.ITCODE,t1.ITNAME,t1.FINAFFCODE,t1.FINAFFNAME,t1.TERRITORYTYPE,
        round(t2.ASSETSCALE/100000000,2) as ASSETSCALE
        from (select * from tq_cibd_regifinaplat where ISVALID=1 and FINAFFNAME like :region) t1
        left join  tq_cibd_regifinaplatsub t2
        on t1.ITCODE = t2.ITCODE
        """
        params = {"region": f"%{region}"}
        result = self.select_list_by_sql(query, params,TqcibdRegifinaplat)
        return result