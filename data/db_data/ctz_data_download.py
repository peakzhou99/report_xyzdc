import os
import re
from datetime import datetime
import json
import pandas as pd
from data.db_access.yjt_data_access.dao.tq_cibd_regifin_new_dao import TqcibdRegifinNewDao
from data.db_access.yjt_data_access.dao.tq_cibd_regifinaplat_dao import TqcibdRegifinaplatDao
from data.db_access.yjt_data_access.dao.tq_sk_shareholder_dao import TqskShareholderDao
from data.db_access.yjt_data_access.dao.tq_fin_inbeardebt_dao import TqfinInbeardebtDao
from data.db_access.yjt_data_access.dao.tq_fin_fntop5acrec_dao import TqfinFntop5acrecDao
from data.db_access.yjt_data_access.dao.tq_fin_otherrece_dao import TqfinOtherreceDao
from data.db_access.yjt_data_access.dao.tq_bd_creditlinedetails_dao import TqbdCreditlinedetailsDao
from data.db_access.yjt_data_access.dao.tq_fin_prgbalsheetnew_dao import TqfinPrgbalsheetnewDao
from data.db_access.yjt_data_access.dao.tq_fin_prgcfstatementnew_dao import TqfinPrgcfstatementnewDao
from data.db_access.yjt_data_access.dao.tq_bd_issueregister_dao import TqbdIssueregisterDao
from data.db_access.yjt_data_access.dao.tq_ns_regifinlease_dao import TqnsRegifinleaseDao
from data.db_access.yjt_data_access.dao.tq_bd_basicinfo_dao import TqbdBasicinfoDao
from data.db_access.yjt_data_access.dao.tq_cibd_newregifinaplat_dao import TqcibdNewregifinaplatDao
from analyze.private_apply import PrivateApply
from data.db_access.yjt_data_access.dao.tq_comp_info_dao import TqCompInfoDao
from data.db_access.yjt_data_access.dao.tq_comp_cboardmap_dao import TqCompCboardmapDao
from data.db_access.yjt_data_access.dao.tq_bd_relatedparty_dao import TqBdRelatedpartyDao
from data.db_access.yjt_data_access.dao.tq_bd_creditrtissue_dao import TqBdCreditrtissueDao

from utils.file_util import mkdir
from utils.addr_util import get_region_pca, get_min_region
from utils.log_utils import get_logger

logger = get_logger()


class DataDownload:
    def __init__(self, output_dir):
        self.output_dir = output_dir

    def to_excel(self, file_name, data):
        df = pd.DataFrame(data)

    def to_csv(self, company_name, file_name, data, year_mon=None):
        _file_name = f"{file_name}_{year_mon}"
        file_dir = f'{self.output_dir}/external_data'
        # 判断文件路径是否存在，不存在就创建
        mkdir(file_dir)
        if year_mon is None:
            _year_mon = datetime.today().strftime("%Y%m")
            _file_name = f"{file_name}_{_year_mon}"

        if data and isinstance(data, list) and all(isinstance(x, list) for x in data):
            df = pd.concat([pd.DataFrame(ele).set_index("指标名称") for ele in data], axis=1).reset_index()
        else:
            df = pd.DataFrame(data)
        df.to_csv(f'{file_dir}/{_file_name}.csv', encoding='gbk', index=False, errors='ignore')

    def convert_to_comment_value_dicts(self, result):
        if isinstance(result, list):
            return [ele.to_comment_dict() for ele in result]
        elif isinstance(result, dict) and result is not None:
            return [result]

    def transpose_comment_value_dicts(self, result):
        if isinstance(result, list):
            return [ele.transpose_comment_dict() for ele in result]

    def get_year_month(self, result, attr):
        _temp_year_month = None
        if isinstance(result, list) and len(result) > 0:
            _temp_year_month = str(max([getattr(_ele, attr) for _ele in result if getattr(_ele, attr) is not None]))
        elif isinstance(result, dict):
            _temp_year_month = result.get(attr)
        if _temp_year_month is not None:
            if re.search(r"\d{4}\d{1,2}\d{1,2}", _temp_year_month):
                _temp_year_month = datetime.strptime(_temp_year_month, '%Y%m%d').strftime("%Y%m")
            elif re.search(r"\d{4}-\d{1,2}-\d{1,2}", _temp_year_month):
                _temp_year_month = datetime.strptime(_temp_year_month, '%Y-%m-%d').strftime("%Y%m")
        return _temp_year_month

    def get_rel_region(self, region: list, controller: str):
        company_biz_messages = [
            {
                "role": "user",
                "content": f"""
                你是一个金融分析助手，请根据实际控制人，从公司区域信息列表选出一个出归属地信息。
                【实际控制人】
                {controller}
                【区域信息】
                {region}
                【输出】
                {{'BOARDCODE': '', 'KEYCODE': '', 'KEYNAME': ''}}
                """
            }
        ]
        llmClient = PrivateApply()
        return llmClient.generate(company_biz_messages)

    def get_rel_region_by_com(self, region: list, com: str):
        company_biz_messages = [
            {
                "role": "user",
                "content": f"""
                你是一个金融分析助手，请根据公司名称，从公司区域信息列表选出一个出归属地信息。
                【公司名称】
                {com}
                【区域信息】
                {region}
                【输出】
                {{'BOARDCODE': '', 'KEYCODE': '', 'KEYNAME': ''}}
                """
            }
        ]
        llmClient = PrivateApply()
        return llmClient.generate(company_biz_messages)

    def extract_data_bak(self, comName):
        try:
            # 获取公司编码
            tqcibdRegifinaplatDao = TqcibdRegifinaplatDao()
            comCode = tqcibdRegifinaplatDao.select_comcode(comName)
            logger.debug(f"获取公司编码成功：{comName}: {comCode}")

            # 发行平台所属区域
            region = tqcibdRegifinaplatDao.select_region(comName)
            region_dict = get_region_pca(region)
            region_dict.update({"公司名称": comName})
            self.to_csv(comName, "公司所属区域", [region_dict])

            # 主体评级
            tqcibdNewregifinaplatDao = TqcibdNewregifinaplatDao()
            region_creditrate = tqcibdNewregifinaplatDao.select_region_and_credit_rate(comCode)
            if region_creditrate is not None:
                self.to_csv(comName, "发行主体评级", self.convert_to_comment_value_dicts(region_creditrate))

            # 发行平台区域经济--全国
            tqcibdRegifinNewDao = TqcibdRegifinNewDao()
            national_province_eco = tqcibdRegifinNewDao.select_national_eco()
            if national_province_eco is not None:
                _year_month = self.get_year_month(national_province_eco, "endDate")
                self.to_csv(comName, "全国区域经济", self.convert_to_comment_value_dicts(national_province_eco), _year_month)

            # 发行平台区域经济--地方
            tqcibdRegifinNewDao = TqcibdRegifinNewDao()
            province = region_dict.get("省")
            national_local_eco = tqcibdRegifinNewDao.select_local_eco(province)
            if national_local_eco is not None:
                _year_month = self.get_year_month(national_local_eco, "endDate")
                self.to_csv(comName, "地方区域经济", self.convert_to_comment_value_dicts(national_local_eco), _year_month)

            # 发行平台区域城投平台
            region = get_min_region(region_dict.get("地址"))
            if "海门市" == region:
                region = "海门区"
            logger.debug(f'区域：{region_dict.get("地址")}, 最小区域：{region}')
            tqcibdRegifinaplatDao = TqcibdRegifinaplatDao()
            region_companys = tqcibdRegifinaplatDao.select_region_companys(region)
            if region_companys is not None:
                _year_month = self.get_year_month(region_companys, "reportdate")
                self.to_csv(comName, "区域发行平台", self.convert_to_comment_value_dicts(region_companys), _year_month)

            # 股权结构
            tqskShareholderDao = TqskShareholderDao()
            share_holders = tqskShareholderDao.select_share_holder(comCode)
            if share_holders is not None and len(share_holders) > 0:
                _year_month = self.get_year_month(share_holders, "enddate")
                self.to_csv(comName, "股权结构", self.convert_to_comment_value_dicts(share_holders), _year_month)

            # 有息债务
            tqfinInbeardebtDao = TqfinInbeardebtDao()
            inbeardebts = tqfinInbeardebtDao.select_inbeardebt(comCode)
            if inbeardebts is not None and len(inbeardebts) > 0:
                _year_month = self.get_year_month(inbeardebts, "reportdate")
                self.to_csv(comName, "有息负债", self.transpose_comment_value_dicts(inbeardebts), _year_month)

            # 应收账款
            tqfinFntop5acrecDao = TqfinFntop5acrecDao()
            ac_recs = tqfinFntop5acrecDao.select_ac_rec(comCode)
            if ac_recs is not None and len(ac_recs) > 0:
                _year_month = self.get_year_month(ac_recs, "enddate")
                self.to_csv(comName, "应收账款", self.convert_to_comment_value_dicts(ac_recs), _year_month)

            # 其他应收
            tqfinOtherreceDao = TqfinOtherreceDao()
            other_recs = tqfinOtherreceDao.select_other_rece(comCode)
            if other_recs is not None and len(other_recs) > 0:
                _year_month = self.get_year_month(other_recs, "enddate")
                self.to_csv(comName, "其他应收款", self.convert_to_comment_value_dicts(other_recs), _year_month)

            # 存续债券
            tqbdBasicinfoDao = TqbdBasicinfoDao()
            old_bond = tqbdBasicinfoDao.select_outstanding_bonds(comCode)
            if old_bond is not None and len(old_bond) > 0:
                _year_month = datetime.today().strftime("%Y%m")
                self.to_csv(comName, "存量债券", self.convert_to_comment_value_dicts(old_bond), _year_month)

            # 非标融资
            tqnsRegifinleaseDao = TqnsRegifinleaseDao()
            fin_leases = tqnsRegifinleaseDao.select_fin_lease(comCode)
            if fin_leases is not None and len(fin_leases) > 0:
                _year_month = self.get_year_month(fin_leases, "reportdate")
                self.to_csv(comName, "非标融资", self.convert_to_comment_value_dicts(fin_leases), _year_month)

            # DCM注册额度
            tqbdIssueregisterDao = TqbdIssueregisterDao()
            dcm_bonds = tqbdIssueregisterDao.select_dcm_bond(comCode)
            if dcm_bonds is not None and len(dcm_bonds) > 0:
                _year_month = datetime.today().strftime("%Y%m")
                self.to_csv(comName, "DCM注册额度", self.convert_to_comment_value_dicts(dcm_bonds), _year_month)

            # 授信额度
            tqbdCreditlinedetailsDao = TqbdCreditlinedetailsDao()
            credit_lines = tqbdCreditlinedetailsDao.select_credit_line(comCode)
            if credit_lines is not None and len(credit_lines) > 0:
                _year_month = self.get_year_month(credit_lines, "enddate")
                self.to_csv(comName, "授信情况", self.convert_to_comment_value_dicts(credit_lines), _year_month)

            # 资产负债
            tqfinPrgbalsheetnewDao = TqfinPrgbalsheetnewDao()
            prgbal_sheets = tqfinPrgbalsheetnewDao.select_sheet(comCode)
            if prgbal_sheets is not None and len(prgbal_sheets) > 0:
                _year_month = self.get_year_month(prgbal_sheets, "enddate")
                self.to_csv(comName, "资产负债", self.transpose_comment_value_dicts(prgbal_sheets), _year_month)

            # 现金流
            tqfinPrgcfstatementnewDao = TqfinPrgcfstatementnewDao()
            cash_flows = tqfinPrgcfstatementnewDao.select_cash_flow(comCode)
            if cash_flows is not None and len(cash_flows) > 0:
                _year_month = self.get_year_month(cash_flows, "enddate")
                self.to_csv(comName, "现金流", self.transpose_comment_value_dicts(cash_flows), _year_month)

        except Exception as e:
            logger.exception(e)
            raise Exception(f"数据库数据读取错误: {str(e)}")

    def extract_data(self, comName):
        try:
            # 获取公司编码
            tqCompInfoDao = TqCompInfoDao()
            comCode = tqCompInfoDao.select_comp_code(comName)
            if not comCode:
                raise Exception(f"未获取到预警通数据，无法生成。")
            logger.debug(f"获取公司编码成功：{comName}: {comCode}")

            # 实际控制人
            tqBdRelatedpartyDao = TqBdRelatedpartyDao()
            actual_controller = tqBdRelatedpartyDao.select_relatedparty(comCode)

            # 查询所属区域
            region_dict = {"省": "", "市": "", "区": ""}
            tqCompCboardmapDao = TqCompCboardmapDao()
            cboardmap = tqCompCboardmapDao.select_cboardmap_info(comCode)
            if cboardmap:
                for item in cboardmap:
                    if item.get("BOARDCODE", "") == "1101":
                        region_dict["省"] = item["KEYNAME"]
                    elif item.get("BOARDCODE", "") == "1102":
                        region_dict["市"] = item["KEYNAME"]
                    elif item.get("BOARDCODE", "") == "1103":
                        region_dict["区"] = item["KEYNAME"]

            # 发行平台区域经济--地方
            if region_dict["省"]:
                # 省
                province = region_dict["省"]
                tqcibdRegifinNewDao = TqcibdRegifinNewDao()
                national_local_eco = tqcibdRegifinNewDao.select_local_eco(province)
                if national_local_eco is not None:
                    _year_month = self.get_year_month(national_local_eco, "endDate")
                    self.to_csv(comName, "地方区域经济",self.convert_to_comment_value_dicts(national_local_eco), _year_month)

            # 发行平台区域城投平台,先查城投数据，没有则按照实际控制推测，最后按照公司名推测
            tqcibdRegifinaplatDao = TqcibdRegifinaplatDao()
            region = tqcibdRegifinaplatDao.select_region(comName)
            if region:
                if "海门市" == region:
                    region = "海门区"
                tqcibdRegifinaplatDao = TqcibdRegifinaplatDao()
                region_companys = tqcibdRegifinaplatDao.select_region_companys(region)
                if region_companys is not None:
                    _year_month = self.get_year_month(region_companys, "reportdate")
                    self.to_csv(comName, "区域发行平台",self.convert_to_comment_value_dicts(region_companys), _year_month)

            elif cboardmap and actual_controller:
                rel_region = self.get_rel_region(cboardmap, actual_controller)
                rel_region = json.loads(rel_region)
                region = rel_region.get("KEYNAME", "")
                if "海门市" == region:
                    region = "海门区"
                tqcibdRegifinaplatDao = TqcibdRegifinaplatDao()
                region_companys = tqcibdRegifinaplatDao.select_region_companys(region)
                if region_companys is not None:
                    _year_month = self.get_year_month(region_companys, "reportdate")
                    self.to_csv(comName, "区域发行平台",self.convert_to_comment_value_dicts(region_companys),
                                _year_month)

            else:
                rel_region = self.get_rel_region_by_com(cboardmap, comName)
                rel_region = json.loads(rel_region)
                region = rel_region.get("KEYNAME", "")
                if "海门市" == region:
                    region = "海门区"
                tqcibdRegifinaplatDao = TqcibdRegifinaplatDao()
                region_companys = tqcibdRegifinaplatDao.select_region_companys(region)
                if region_companys is not None:
                    _year_month = self.get_year_month(region_companys, "reportdate")
                    self.to_csv(comName, "区域发行平台",self.convert_to_comment_value_dicts(region_companys),
                                _year_month)

            # 主体评级
            tqBdCreditrtissueDao = TqBdCreditrtissueDao()
            credit_rate = tqBdCreditrtissueDao.select_credit_rate(comCode)
            credit_rate = {"主体评级": credit_rate}
            credit_rate.update({"公司名称": comName})
            credit_rate.update({"实际控制人": actual_controller})
            credit_rate.update(region_dict)
            if credit_rate:
                self.to_csv(comName, "发行主体评级", [credit_rate])

            # 发行平台区域经济--全国
            tqcibdRegifinNewDao = TqcibdRegifinNewDao()
            national_province_eco = tqcibdRegifinNewDao.select_national_eco()
            if national_province_eco is not None:
                _year_month = self.get_year_month(national_province_eco, "endDate")
                self.to_csv(comName, "全国区域经济", self.convert_to_comment_value_dicts(national_province_eco),
                            _year_month)

            # 股权结构
            tqskShareholderDao = TqskShareholderDao()
            share_holders = tqskShareholderDao.select_share_holder(comCode)
            if share_holders is not None and len(share_holders) > 0:
                _year_month = self.get_year_month(share_holders, "enddate")
                self.to_csv(comName, "股权结构", self.convert_to_comment_value_dicts(share_holders),
                            _year_month)

            # 有息债务
            tqfinInbeardebtDao = TqfinInbeardebtDao()
            inbeardebts = tqfinInbeardebtDao.select_inbeardebt(comCode)
            if inbeardebts is not None and len(inbeardebts) > 0:
                _year_month = self.get_year_month(inbeardebts, "reportdate")
                self.to_csv(comName, "有息负债", self.transpose_comment_value_dicts(inbeardebts), _year_month)

            # 应收账款
            tqfinFntop5acrecDao = TqfinFntop5acrecDao()
            ac_recs = tqfinFntop5acrecDao.select_ac_rec(comCode)
            if ac_recs is not None and len(ac_recs) > 0:
                _year_month = self.get_year_month(ac_recs, "enddate")
                self.to_csv(comName, "应收账款", self.convert_to_comment_value_dicts(ac_recs), _year_month)

            # 其他应收
            tqfinOtherreceDao = TqfinOtherreceDao()
            other_recs = tqfinOtherreceDao.select_other_rece(comCode)
            if other_recs is not None and len(other_recs) > 0:
                _year_month = self.get_year_month(other_recs, "enddate")
                self.to_csv(comName, "其他应收款", self.convert_to_comment_value_dicts(other_recs), _year_month)

            # 存续债券
            tqbdBasicinfoDao = TqbdBasicinfoDao()
            old_bond = tqbdBasicinfoDao.select_outstanding_bonds(comCode)
            if old_bond is not None and len(old_bond) > 0:
                _year_month = datetime.today().strftime("%Y%m")
                self.to_csv(comName, "存量债券", self.convert_to_comment_value_dicts(old_bond), _year_month)

            # 非标融资
            tqnsRegifinleaseDao = TqnsRegifinleaseDao()
            fin_leases = tqnsRegifinleaseDao.select_fin_lease(comCode)
            if fin_leases is not None and len(fin_leases) > 0:
                _year_month = self.get_year_month(fin_leases, "reportdate")
                self.to_csv(comName, "非标融资", self.convert_to_comment_value_dicts(fin_leases), _year_month)

            # DCM注册额度
            tqbdIssueregisterDao = TqbdIssueregisterDao()
            dcm_bonds = tqbdIssueregisterDao.select_dcm_bond(comCode)
            if dcm_bonds is not None and len(dcm_bonds) > 0:
                _year_month = datetime.today().strftime("%Y%m")
                self.to_csv(comName, "DCM注册额度", self.convert_to_comment_value_dicts(dcm_bonds), _year_month)

            # 授信额度
            tqbdCreditlinedetailsDao = TqbdCreditlinedetailsDao()
            credit_lines = tqbdCreditlinedetailsDao.select_credit_line(comCode)
            if credit_lines is not None and len(credit_lines) > 0:
                _year_month = self.get_year_month(credit_lines, "enddate")
                self.to_csv(comName, "授信情况", self.convert_to_comment_value_dicts(credit_lines), _year_month)

            # 资产负债
            tqfinPrgbalsheetnewDao = TqfinPrgbalsheetnewDao()
            prgbal_sheets = tqfinPrgbalsheetnewDao.select_sheet(comCode)
            if prgbal_sheets is not None and len(prgbal_sheets) > 0:
                _year_month = self.get_year_month(prgbal_sheets, "enddate")
                self.to_csv(comName, "资产负债", self.transpose_comment_value_dicts(prgbal_sheets), _year_month)

            # 现金流
            tqfinPrgcfstatementnewDao = TqfinPrgcfstatementnewDao()
            cash_flows = tqfinPrgcfstatementnewDao.select_cash_flow(comCode)
            if cash_flows is not None and len(cash_flows) > 0:
                _year_month = self.get_year_month(cash_flows, "enddate")
                self.to_csv(comName, "现金流", self.transpose_comment_value_dicts(cash_flows), _year_month)

        except Exception as e:
            logger.exception(e)
            raise Exception(f"数据库数据读取错误。{str(e)}")
        
if __name__ == "__main__":
    tqcibdRegifinaplatDao = TqcibdRegifinaplatDao()
    comCode = tqcibdRegifinaplatDao.select_comcode("义乌市双江湖开发集团有限公司")
    print(comCode)