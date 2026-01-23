import pandas as pd
import numpy as np
import os
from utils.string_util import *
import utils.file_util as file_util
from utils.log_utils import get_logger

logger = get_logger()

class YjtDataHandler():
    def __init__(self, company_name, root_dir):
        self.company_name = company_name.replace("(", "（").replace(")", "）")
        self.comm_data_dir = os.path.join(root_dir, "common_data")
        self.ext_data_dir = os.path.join(root_dir, f"company_data/{company_name}/external_data")

    def get_urban_invest_rank(self, urban_invest_path):
        """城投平台排名  下载情况解析"""
        logger.debug(urban_invest_path)
        """地区城投债排名"""
        urban_invest_df = pd.read_excel(urban_invest_path, skiprows=1)
        urban_invest_df.dropna(subset=['总资产（亿元）'], inplace=True)
        urban_invest_df['排名'] = urban_invest_df['总资产（亿元）'].rank(ascending=False).astype(int).astype(str) + "/" + str(len(urban_invest_df))
        # 处理公司名称带括号问题
        urban_invest_df['公司名称'] = urban_invest_df['公司名称'].apply(lambda x : x.replace("(", "（").replace(")", "）"))
        urban_invest_df = urban_invest_df[urban_invest_df['公司名称'] == self.company_name][['公司名称', '区域', '行政级别', '总资产（亿元）', '排名']]
        invest_rank_list = [urban_invest_df.columns.tolist()] + urban_invest_df.values.tolist()
        tab_date = "2023年"
        return {'name': '发债平台情况', 'date': tab_date, 'header_rows': 1, 'unit': '', 'data': invest_rank_list}

    def get_urban_invest_rank_crawl(self, urban_invest_path):
        """城投平台排名  下载情况解析"""
        """地区城投债排名"""
        logger.debug(f"地区城投债排名-数据路径：{urban_invest_path}")
        urban_invest_df = pd.read_csv(urban_invest_path, encoding='gbk')
        urban_invest_df = urban_invest_df[
            ~urban_invest_df.apply(lambda row: all(_cell == '-' for _cell in row), axis=1)]
        urban_invest_df.replace("-", np.nan, inplace=True)
        urban_invest_df.dropna(subset=['总资产(亿元)'], inplace=True)
        urban_invest_df['排名'] = urban_invest_df['总资产(亿元)'].astype(float).rank(ascending=False).astype(
            int).astype(str) + "/" + str(len(urban_invest_df))
        # 处理公司名称带括号问题
        urban_invest_df['公司名称'] = urban_invest_df['公司名称'].str.replace('简报分析', '').apply(
            lambda x: x.replace("(", "（").replace(")", "）"))
        urban_invest_df = urban_invest_df[urban_invest_df['公司名称'].str.replace('简报分析', '') == self.company_name][
            ['公司名称', '区域', '行政级别', '总资产(亿元)', '排名']]
        # urban_invest_dict_list = urban_invest_df.to_dict(orient='records')
        # if len(urban_invest_dict_list) > 0:
        #     return urban_invest_dict_list[0]
        invest_rank_list = [urban_invest_df.columns.tolist()] + urban_invest_df.values.tolist()
        tab_date = "2023年"
        return {'name': '发债平台情况', 'date': tab_date, 'header_rows': 1, 'unit': '', 'data': invest_rank_list}

    def get_table_unit(self, table_datas):
        for row_data in table_datas:
            for cell_data in row_data:
                if not isinstance(cell_data, str):
                    continue
                tmp_unit = extract_unit(cell_data)
                if tmp_unit is not None:
                    return tmp_unit

    def get_recent_date(self, table_datas):
        for row_data in table_datas:
            for cell_data in row_data:
                if not isinstance(cell_data, str):
                    continue
                tmp_date = extract_date(cell_data)
                if tmp_date is not None:
                    return tmp_date

    def get_area_rank(self, area_path):
        """区域排名
            返回结果为二维数组
        """
        # 获取公司的省市区
        all_company_region_df = pd.read_csv(os.path.join(self.comm_data_dir, '公司所属区域.csv'), encoding='gbk')
        company_region_df = all_company_region_df[all_company_region_df['公司名称'] == self.company_name]
        logger.debug(company_region_df, self.company_name)
        _province = company_region_df['省'].values[0]
        _city = company_region_df['市'].values[0]
        _town = company_region_df['区'].values[0]
        logger.debug(_province, _city, _town)

        # 省排名(全国)
        _nation_region_dir, _nation_region_file = file_util.find_file_path(self.comm_data_dir, '区域经济_全国')
        province_df = pd.read_excel(os.path.join(_nation_region_dir, _nation_region_file), skiprows=1)
        province_df.fillna(np.nan, inplace=True)
        new_province_df = province_df[
            ['地区名称', 'GDP(亿元)', '一般公共预算收入(亿元)', '政府性基金收入(亿元)', '债务率(宽口径)(%)',
             '财政自给率(%)']].copy()
        new_columns = ['地区', 'GDP', '一般公共预算收入', '政府性基金收入', '宽口径债务率', '财政自给率']
        new_province_df.rename(columns=dict(zip(new_province_df.columns, new_columns)), inplace=True)
        sort_cols = ['GDP', '一般公共预算收入', '政府性基金收入', '宽口径债务率', '财政自给率']
        for sort_col in sort_cols:
            if sort_col == "宽口径债务率":
                new_province_df.insert(new_province_df.columns.get_loc(sort_col) + 1, sort_col + '_排名',
                                       new_province_df[sort_col].rank(ascending=True).apply(
                                           lambda x: f"{int(x)}/{len(new_province_df)}" if not pd.isna(x) else '-'))
            else:
                new_province_df.insert(new_province_df.columns.get_loc(sort_col) + 1, sort_col + '_排名',
                                       new_province_df[sort_col].rank(ascending=False).apply(
                                           lambda x: f"{int(x)}/{len(new_province_df)}" if not pd.isna(x) else '-'))
        fix_province_df = new_province_df[new_province_df['地区'] == _province]
        fix_province_list = [[col_name.split("_")[1] if len(col_name.split("_")) > 1 else col_name for col_name in
                              fix_province_df.columns.tolist()]] + fix_province_df.values.tolist()
        # logger.debug(fix_province_list)

        # 市排名(省内)
        curr_province_df = pd.read_excel(area_path, skiprows=1)
        curr_province_df.fillna(np.nan, inplace=True)
        if "市辖区" != _city:
            city_df = curr_province_df[
                curr_province_df.apply(lambda r: pd.notna(r['地级市']) and pd.isna(r['区县']), axis=1)]
            new_city_df = city_df[
                ['地区名称', 'GDP(亿元)', '一般公共预算收入(亿元)', '政府性基金收入(亿元)', '债务率(宽口径)(%)',
                 '财政自给率(%)']].copy()
            new_columns = ['地区', 'GDP', '一般公共预算收入', '政府性基金收入', '宽口径债务率', '财政自给率']
            new_city_df.rename(columns=dict(zip(new_city_df.columns, new_columns)), inplace=True)
            sort_cols = ['GDP', '一般公共预算收入', '政府性基金收入', '宽口径债务率', '财政自给率']
            for sort_col in sort_cols:
                if sort_col == "宽口径债务率":
                    new_city_df.insert(new_city_df.columns.get_loc(sort_col) + 1, sort_col + '_排名',
                                       new_city_df[sort_col].rank(ascending=True).apply(
                                           lambda x: f"{int(x)}/{len(new_city_df)}" if not pd.isna(x) else '-'))
                    pass
                else:
                    new_city_df.insert(new_city_df.columns.get_loc(sort_col) + 1, sort_col + '_排名',
                                       new_city_df[sort_col].rank(ascending=False).apply(
                                           lambda x: f"{int(x)}/{len(new_city_df)}" if not pd.isna(x) else '-'))
                    pass
            fix_city_df = new_city_df[new_city_df['地区'] == _city]
            fix_province_list = fix_province_list + fix_city_df.values.tolist()
            # logger.debug(fix_province_list)

        # 区县排名(市内)
        if pd.notna(_town):
            town_df = curr_province_df[
                curr_province_df.apply(lambda r: r['地级市'] == _city and pd.notna(r['区县']), axis=1)]
            if _province == "重庆市":
                town_df = curr_province_df[curr_province_df.apply(lambda r: pd.notna(r['区县']), axis=1)]
            new_town_df = town_df[
                ['地区名称', 'GDP(亿元)', '一般公共预算收入(亿元)', '政府性基金收入(亿元)', '债务率(宽口径)(%)',
                 '财政自给率(%)']].copy()
            new_columns = ['地区', 'GDP', '一般公共预算收入', '政府性基金收入', '宽口径债务率', '财政自给率']
            new_town_df.rename(columns=dict(zip(new_town_df.columns, new_columns)), inplace=True)
            sort_cols = ['GDP', '一般公共预算收入', '政府性基金收入', '宽口径债务率', '财政自给率']
            for sort_col in sort_cols:
                if sort_col == "宽口径债务率":
                    new_town_df.insert(new_town_df.columns.get_loc(sort_col) + 1, sort_col + '_排名',
                                       new_town_df[sort_col].rank(ascending=True).apply(
                                           lambda x: f"{int(x)}/{len(new_town_df)}" if not pd.isna(x) else '-'))
                    pass
                else:
                    new_town_df.insert(new_town_df.columns.get_loc(sort_col) + 1, sort_col + '_排名',
                                       new_town_df[sort_col].rank(ascending=False).apply(
                                           lambda x: f"{int(x)}/{len(new_town_df)}" if not pd.isna(x) else '-'))
                    pass
            fix_town_df = new_town_df[new_town_df['地区'] == _town]
            fix_province_list = fix_province_list + fix_town_df.values.tolist()
            # logger.debug(fix_province_list)

        tab_date = uniform_date(os.path.basename(area_path), '%Y%m')
        tab_unit = self.get_table_unit([province_df.columns.tolist()])
        logger.debug(f'区域经济及债务: {fix_province_list}')
        return {'name': '区域经济及债务', 'date': tab_date, 'header_rows': 1, 'unit': tab_unit,
                'data': fix_province_list}

    def outstanding_bonds(self, bonds_path):
        """存量债券"""
        bonds_df = pd.read_excel(bonds_path, skiprows=1)
        bonds_df.dropna(subset=['发行规模（亿）'], inplace=True)
        bonds_df = bonds_df[
            ['债券代码', '债券简称', '债券一级类型', '债项评级', '债券余额（亿）', '剩余期限（年）', '发行规模（亿）',
             '发行日期', '募集方式', '票面利率（%）', '到期日期']].copy()
        bonds_df.fillna('', inplace=True)
        bonds_list = [bonds_df.columns.tolist()] + bonds_df.values.tolist()
        tab_unit = self.get_table_unit(bonds_list)
        _data_date = datetime.today().strftime("%Y%m")
        return {'name': '存量债券', 'date': _data_date, 'header_rows': 1, 'unit': tab_unit, 'data': bonds_list}

    def outstanding_bonds_crawl(self, bonds_path):
        """存量债券"""
        bonds_df = pd.read_csv(bonds_path, encoding='gbk')
        bonds_df = bonds_df[~bonds_df.apply(lambda row: all(_cell == '-' for _cell in row), axis=1)]
        bonds_df.dropna(subset=['发行规模(亿)'], inplace=True)
        bonds_df = bonds_df[
            ['债券代码', '债券简称', '债券类型', '债项评级', '债券余额(亿)', '剩余期限(年)', '发行规模(亿)', '发行日期', '募集方式', '票面利率(%)', '到期日期']].copy()
        bonds_df.fillna('', inplace=True)
        bonds_list = [bonds_df.columns.tolist()] + bonds_df.values.tolist()
        tab_unit = self.get_table_unit(bonds_list)
        _data_date = datetime.today().strftime("%Y%m")
        return {'name': '存量债券', 'date': _data_date, 'header_rows': 1, 'unit': tab_unit, 'data': bonds_list}

    def cash_flow(self, cash_path):
        cash_df = pd.read_excel(cash_path, skiprows=1)
        cash_data_list = [cash_df.columns.tolist()] + cash_df.values.tolist()
        tab_date = self.get_recent_date(cash_data_list)
        tab_unit = self.get_table_unit(cash_data_list)
        return {'name': '现金流', 'date': tab_date, 'header_rows': 3, 'unit': tab_unit, 'data': cash_data_list}

    def cash_flow_crawl(self, cash_path):
        cash_df = pd.read_csv(cash_path, encoding='gbk')
        cash_df = cash_df[~cash_df.apply(lambda row: all(_cell == '-' for _cell in row), axis=1)]
        cash_data_list = [cash_df.columns.tolist()] + cash_df.values.tolist()
        tab_date = self.get_recent_date(cash_data_list)
        tab_unit = self.get_table_unit(cash_data_list)
        if tab_unit is None:
            tab_unit = '万'
        return {'name': '现金流', 'date': tab_date, 'header_rows': 3, 'unit': tab_unit, 'data': cash_data_list}

    def assets(self, assets_path):
        assets_df = pd.read_excel(assets_path, skiprows=1)
        assets_data_list = [assets_df.columns.tolist()] + assets_df.values.tolist()
        tab_date = self.get_recent_date(assets_data_list)
        tab_unit = self.get_table_unit(assets_data_list)
        return {'name': '资产负债', 'date': tab_date, 'header_rows': 3, 'unit': tab_unit, 'data': assets_data_list}

    def assets_crawl(self, assets_path):
        assets_df = pd.read_csv(assets_path, encoding='gbk')
        assets_df = assets_df[~assets_df.apply(lambda row: all(_cell == '-' for _cell in row), axis=1)]
        assets_data_list = [assets_df.columns.tolist()] + assets_df.values.tolist()
        tab_date = self.get_recent_date(assets_data_list)
        tab_unit = self.get_table_unit(assets_data_list)
        if tab_unit is None:
            tab_unit = '万'
        return {'name': '资产负债', 'date': tab_date, 'header_rows': 3, 'unit': tab_unit, 'data': assets_data_list}

    def dcm_regist(self, dcm_path):
        dcm_df = pd.read_excel(dcm_path, skiprows=1)
        dcm_data_list = [dcm_df.columns.tolist()] + dcm_df.values.tolist()
        _data_date = datetime.today().strftime("%Y%m")
        tab_unit = self.get_table_unit(dcm_data_list)
        return {'name': '注册未发行债券', 'date': _data_date, 'header_rows': 3, 'unit': tab_unit, 'data': dcm_data_list}

    def get_debtor_amount(self, debtor_path):
        """被执行人金额"""
        debtor_df = pd.read_csv(debtor_path, encoding='gbk')
        debtor_data_list = [debtor_df.columns.tolist()] + debtor_df.values.tolist()
        _data_date = datetime.today().strftime("%Y%m")
        return {'name': '被执行金额', 'date': _data_date, 'header_rows': 1, 'unit': '', 'data': debtor_data_list}

    def get_no_standard_finance(self, no_standard_finance_path):
        """非标融资"""
        no_standard_finance_df = pd.read_csv(no_standard_finance_path, encoding='gbk')
        no_standard_finance_df = no_standard_finance_df[
            ~no_standard_finance_df.apply(lambda row: all(_cell == '-' for _cell in row), axis=1)
        ]
        no_standard_finance_list = [no_standard_finance_df.columns.tolist()] + no_standard_finance_df.values.tolist()
        _unit = self.get_table_unit(no_standard_finance_list)
        _data_date = uniform_date(os.path.basename(no_standard_finance_path), '%Y%m')
        return {'name': '非标融资', 'date': _data_date, 'header_rows': 1, 'unit': _unit,
                'data': no_standard_finance_list}

    def get_credit_limit(self, credit_limit_path):
        """授信额度"""
        credit_limit_df = pd.read_csv(credit_limit_path, encoding='gbk')
        credit_limit_df = credit_limit_df[~credit_limit_df.apply(lambda row: all(_cell == '-' for _cell in row), axis=1)]
        credit_limit_list = [credit_limit_df.columns.tolist()] + credit_limit_df.values.tolist()
        _unit = self.get_table_unit(credit_limit_list)
        _data_date = uniform_date(os.path.basename(credit_limit_path), '%Y%m')
        return {'name': '授信额度', 'date': _data_date, 'header_rows': 1, 'unit': _unit, 'data': credit_limit_list}

    def get_bear_debt(self, bear_debt_path):
        """有息负债"""
        bear_debt_df = pd.read_csv(bear_debt_path, encoding='gbk')
        bear_debt_df = bear_debt_df[~bear_debt_df.apply(lambda row: all(_cell == '-' for _cell in row), axis=1)]
        bear_debt_list = [bear_debt_df.columns.tolist()] + bear_debt_df.values.tolist()
        # _unit = self.get_table_unit(bear_debt_list)
        _data_date = self.get_recent_date(bear_debt_list)
        return {'name': '有息负债', 'date': _data_date, 'header_rows': 1, 'unit': '万', 'data': bear_debt_list}

    def get_method_by_keyword(self, file_name):
        methods = {
            "区域经济": self.get_area_rank,
            "债务公司": self.get_debtor_amount,
            "存量债券": self.outstanding_bonds_crawl,
            "非标融资": self.get_no_standard_finance,
            "授信额度": self.get_credit_limit,
            "同地区城投平台": self.get_urban_invest_rank_crawl,
            "DCM注册额度": self.dcm_regist,
            "现金流": self.cash_flow_crawl,
            "资产负债": self.assets_crawl
        }
        for key, method in methods.items():
            if key in file_name:
                return method

    def get_yjt_data(self):
        """获取预警通数据"""
        yjt_datas = []
        for root, dirs, files in os.walk(self.ext_data_dir):
            for file_name in files:
                method = self.get_method_by_keyword(file_name)
                logger.debug(file_name)
                if not method:
                    continue
                yjt_datas.append(method(os.path.join(root, file_name)))
        # logger.debug(yjt_datas)
        return yjt_datas

if __name__ == "__main__":
    company_name = "重庆市綦江区城市建设投资有限公司"
    data_dir = "D:/opt/城投债/20240620"
    handler = YjtDataHandler(company_name, data_dir)
    # yjt_datas = handler.get_yjt_data()
    # logger.debug(yjt_datas)

    _area_path = "D:/opt/城投债/20240620/company_data/重庆市綦江区城市建设投资有限公司/external_data/区域经济_重庆市_202312.xlsx"
    handler.get_area_rank(_area_path)