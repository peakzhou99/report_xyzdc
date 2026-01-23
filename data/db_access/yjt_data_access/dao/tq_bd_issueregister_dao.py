from data.db_access.mysql_base_dao import MySQLDao
from data.db_access.yjt_data_access.entity.tq_bd_issueregister import TqbdIssueregister

class TqbdIssueregisterDao(MySQLDao):
    def select_dcm_bond(self, com_code):
        """"""
        query1 = """
        select BONDIRIID,BONDTYPE
              ,round(REGISTERLIMIT,2) REGISTERLIMIT
              ,REGISTERBEGINDATE,REGISTERENDDATE
        from tq_bd_issueregister where compcode=:comCode
        """

        params1 = {"comCode": com_code}
        result1 = self.select_list_by_sql(query1, params1)

        # 查询债券信息（累计使用，已使用）
        query2 = """
        select b.BONDIRIID
              ,IF(a.ACTISSAMT is null,null,round(a.ACTISSAMT,2)) ACTISSAMT
              ,IF(a.CURRENTAMT is null,null,round(a.CURRENTAMT,2)) CURRENTAMT
        from TQ_BD_NEWESTBASICINFO a
        join tq_bd_issue b
          on a.SECODE = b.SECODE and a.ISSUECOMPCODE=:comCode
        where b.BONDIRIID is not null
        """
        params2 = {"comCode": com_code}
        result2 = self.select_list_by_sql(query2, params2)

        # 遍历已使用情况
        used_dict = {}
        for ele2 in result2:
            used_dict.update({str(ele2.get("BONDIRIID")): ele2})

        # 填充信息
        tmp_result = [dict(row) for row in result1]
        for ele1 in tmp_result:
            usedinfo = used_dict.get(str(ele1.get("BONDIRIID")))
            if usedinfo is None:
                ele1.update({"ACTISSAMT": None, "CURRENTAMT": None, "UNUSEDAMT": None})
            else:
                ele1.update({
                    "ACTISSAMT": usedinfo.get("ACTISSAMT"),
                    "CURRENTAMT": usedinfo.get("CURRENTAMT"),
                    "UNUSEDAMT": ele1.get("REGISTERLIMIT") if usedinfo.get("CURRENTAMT") is None else (ele1.get("REGISTERLIMIT") - usedinfo.get("CURRENTAMT"))
                })

        result = self.convert_result_type(tmp_result, TqbdIssueregister)
        return result