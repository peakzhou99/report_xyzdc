import pandas as pd
import numpy as np
import os
from utils.string_util import *
import utils.file_util as file_util
import utils.addr_util as addr_util
from utils.log_utils import get_logger

logger = get_logger()


class YjtDBDataHandler():
    def __init__(self, company_name, root_dir):
        self.company_name = company_name.replace("(", "（").replace(")", "）")
        self.ext_data_dir = os.path.join(root_dir, "external_data", )
        self.table_datas = []
        self.article_datas = {}

    def get_urban_invest_rank(self, urban_invest_path):
        """城投平台排名"""
        if not os.path.exists(urban_invest_path) or os.path.getsize(urban_invest_path) <= 1:
            logger.error(f"警告: 文件为空或不存在: {urban_invest_path}")
            return
        urban_invest_df = pd.read_csv(urban_invest_path, encoding='gbk')
        urban_invest_df = urban_invest_df[
            ~urban_invest_df.apply(lambda row: all(_cell == '-' for _cell in row), axis=1)]
        urban_invest_df.replace("-", np.nan, inplace=True)
        urban_invest_df.dropna(subset=['总资产'], inplace=True)
        urban_invest_df['排名'] = urban_invest_df['总资产'].astype(float).rank(ascending=False).astype(int).astype(
            str) + "/" + str(len(urban_invest_df))
        # 处理公司名称带括号问题
        urban_invest_df['公司名称'] = urban_invest_df['公司名称'].apply(lambda x: x.replace("(", "（").replace(")", "）"))
        _tab_date = str(urban_invest_df["截止时间"].max())[:6]
        _unit = urban_invest_df["单位"].max()
        if _unit and _unit == "亿":
            _unit = "亿元"
        urban_invest_df = urban_invest_df[urban_invest_df['公司名称'] == self.company_name][
            ['公司名称', '区域名称', '行政级别', '总资产', '排名']]

        if urban_invest_df.values.tolist():
            invest_rank_list = [urban_invest_df.columns.tolist()] + urban_invest_df.values.tolist()
            self.table_datas.append(
                {'name': '发债平台情况', 'date': _tab_date, 'header_rows': 1, 'unit': _unit, 'data': invest_rank_list})

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

    def get_max_date(self, table_datas):
        _max_date = None
        for row_data in table_datas:
            for cell_data in row_data:
                if not isinstance(cell_data, str):
                    continue
                tmp_date = extract_date(cell_data)
                if tmp_date is not None and _max_date is None:
                    _max_date = tmp_date
                elif tmp_date is not None and _max_date < tmp_date:
                    _max_date = tmp_date
        return _max_date

    def get_province_rank(self, province):
        province_df = pd.read_csv(file_util.find_path_by_name(self.ext_data_dir, '全国区域经济'), encoding='gbk')
        province_df.fillna(np.nan, inplace=True)
        new_province_df = province_df[
            ['区域名称', 'GDP', '一般公共预算收入', '政府性基金收入', '债务率（宽口径）', '财政自给率']].copy()
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
        fix_province_df = new_province_df[new_province_df['地区'] == province].copy()
        fix_province_df['地区'] = province
        return fix_province_df

    def get_city_rank(self, city):
        if city is None or pd.isna(city) or city == '市辖区' or city == '县':
            return None
        curr_province_df = pd.read_csv(file_util.find_path_by_name(self.ext_data_dir, '地方区域经济'), encoding='gbk')
        curr_province_df = curr_province_df[curr_province_df['区域编码'].astype(str).str[-4:] != '0000']  # 删除省记录
        city_df = curr_province_df[curr_province_df['区域编码'].astype(str).str[-2:] == '00']
        new_city_df = city_df[
            ['区域名称', 'GDP', '一般公共预算收入', '政府性基金收入', '债务率（宽口径）', '财政自给率']].copy()
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
        fix_city_df = new_city_df[new_city_df['地区'].str.endswith(city)].copy()
        fix_city_df['地区'] = city
        return fix_city_df

    def get_town_rank(self, province, city, town):
        if town is None or pd.isna(town):
            return None
        # 针对重庆市等单独处理
        if city is None or pd.isna(city) or city == 'nan':
            city = province
        data_file_path = file_util.find_path_by_name(self.ext_data_dir, '地方区域经济')
        if not os.path.exists(data_file_path) or os.path.getsize(data_file_path) <= 1:
            logger.error(f"警告: 文件为空或不存在: {data_file_path}")
            return
        curr_province_df = pd.read_csv(data_file_path, encoding='gbk')
        curr_province_df = curr_province_df[curr_province_df['区域编码'].astype(str).str[-4:] != '0000']  # 删除省记录
        town_df = curr_province_df[curr_province_df['区域编码'].astype(str).str[-2:] != '00']
        print(city, town)
        town_df = town_df[town_df['区域名称'].str.contains(city)]
        new_town_df = town_df[
            ['区域名称', 'GDP', '一般公共预算收入', '政府性基金收入', '债务率（宽口径）', '财政自给率']].copy()
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
        fix_town_df = new_town_df[new_town_df['地区'].str.endswith(town)].copy()
        fix_town_df['地区'] = town
        return fix_town_df

    def get_area_rank(self, area_path):
        data_file_path = file_util.find_path_by_name(self.ext_data_dir, '发行主体评级')
        if not os.path.exists(data_file_path) or os.path.getsize(data_file_path) <= 1:
            logger.error(f"警告: 文件为空或不存在: {data_file_path}")
            return
        company_region_df = pd.read_csv(data_file_path, encoding='gbk')
        _province = company_region_df['省'].values[0]
        _city = company_region_df['市'].values[0]
        _town = company_region_df['区'].values[0]
        print(_province, _city, _town)

        # 省排名
        fix_province_df = self.get_province_rank(_province)
        fix_province_list = [[col_name.split("_")[1] if len(col_name.split("_")) > 1 else col_name for col_name in
                              fix_province_df.columns.tolist()]] + fix_province_df.values.tolist()

        # 市排名
        fix_city_df = self.get_city_rank(_city)
        if fix_city_df is not None:
            fix_province_list = fix_province_list + fix_city_df.values.tolist()
        # 区排名
        fix_town_df = self.get_town_rank(_province, _city, _town)
        if fix_town_df is not None:
            fix_province_list = fix_province_list + fix_town_df.values.tolist()

        tab_date = uniform_date(os.path.basename(area_path), '%Y%m')
        tab_unit = "亿"
        self.table_datas.append(
            {'name': '区域经济及债务', 'date': tab_date, 'header_rows': 1, 'unit': tab_unit, 'data': fix_province_list})

    def outstanding_bonds(self, bonds_path):
        """存量债券"""
        bonds_df = pd.read_csv(bonds_path, encoding='gbk')
        bonds_df.replace(np.nan, "", inplace=True)
        bonds_df = bonds_df[~bonds_df.apply(lambda row: all(_cell == '-' for _cell in row), axis=1)]
        bonds_df.dropna(subset=['发行规模'], inplace=True)
        bonds_df = bonds_df[
            ['债券代码', '债券简称', '债券类型', '债券评级', '债券余额', '剩余期限', '发行规模', '发行日期', '募集方式',
             '债券期限', '票面利率', '到期日期']].copy()
        bonds_df.astype(str).fillna('', inplace=True)
        bonds_df = bonds_df.replace('', '-')
        bonds_list = [bonds_df.columns.tolist()] + bonds_df.values.tolist()
        tab_unit = "亿"
        _data_date = uniform_date(os.path.basename(bonds_path), '%Y%m')
        # print(tab_unit,_data_date)
        # print(bonds_list)
        self.table_datas.append(
            {'name': '存量债券', 'date': _data_date, 'header_rows': 1, 'unit': tab_unit, 'data': bonds_list})

    def outstanding_bonds_statis(self, bonds_path):
        """存量债券汇总"""
        bonds_df2 = pd.read_csv(bonds_path, encoding='gbk')
        # 私募债券汇总
        bonds_dict = {"存量规模": bonds_df2["债券余额"].sum()
            , "债券只数": bonds_df2["债券代码"].count()
            , "公募债券": bonds_df2[bonds_df2["募集方式"] == "公募债券"]["债券余额"].sum()
            , "公募债券占比": bonds_df2[bonds_df2["募集方式"] == "公募债券"]["债券余额"].sum() / bonds_df2[
                "债券余额"].sum()}
        bonds_static_df = pd.DataFrame([bonds_dict]).round(2)
        bonds_static_list = [bonds_static_df.columns.tolist()] + bonds_static_df.values.tolist()
        _tab_unit = "亿"
        _data_date = uniform_date(os.path.basename(bonds_path), '%Y%m')
        self.table_datas.append({'name': '存量债券统计', 'date': _data_date, 'header_rows': 1, 'unit': _tab_unit,
                                 'data': bonds_static_list})

    def cash_flow(self, cash_path):
        """单位亿元，已在SQL中做处理"""
        cash_df = pd.read_csv(cash_path, encoding='gbk')
        cash_df = cash_df[~cash_df.apply(lambda row: all(_cell == '-' for _cell in row), axis=1)]
        cash_data_list = [cash_df.columns.tolist()] + cash_df.values.tolist()
        tab_date = self.get_max_date([cash_df.columns.tolist()])
        tab_unit = "亿"
        self.table_datas.append(
            {'name': '现金流', 'date': tab_date, 'header_rows': 1, 'unit': tab_unit, 'data': cash_data_list})

    def assets(self, assets_path):
        assets_df = pd.read_csv(assets_path, encoding='gbk')
        assets_df = assets_df[~assets_df.apply(lambda row: all(_cell == '-' for _cell in row), axis=1)]
        zichan_cols = ["货币资金", "结算备付金", "拆出资金", "应收保证金", "应收利息", "应收票据", "应收账款"
            , "应收款项融资", "应收保费", "应收分保账款", "应收分保合同准备金", "应收出口退税", "应收补贴款"
            , "内部应收款", "预付款项", "其他应收款合计", "存货", "买入返售金融资产", "交易性金融资产","衍生金融资产"
            , "一年内到期的非流动资产", "待处理流动资产损益", "其他流动资产", "流动资产合计", "发放贷款及垫款",
                       "可供出售金融资产"
            , "划分为持有待售的资产", "以公允价值计量且其变动计入其他综合收益的金融资产", "以摊余成本计量的金融资产"
            , "债权投资", "其他债权投资", "其他权益工具投资", "其他非流动金融资产", "长期应收款", "长期股权投资", "待摊费用"
            , "其他长期投资", "投资性房地产", "固定资产及清理合计", "合同资产", "在建工程合计", "使用权资产", "工程物资"
            , "生产性生物资产", "公益性生物资产", "油气资产", "无形资产", "开发支出", "商誉", "长期待摊费用"
            , "股权分置流通权", "递延所得税资产", "其他非流动资产", "非流动资产合计", "资产总计"]

        fuzhai_cols = ["短期借款", "向中央银行借款", "吸收存款及同业存放", "拆入资金", "交易性金融负债"
            , "衍生金融负债", "卖出回购金融资产款", "应付手续费及佣金", "应付票据", "应付账款", "预收款项"
            , "合同负债", "应付职工薪酬", "应交税费", "应付利息", "应付股利", "其他应交款", "应付保证金"
            , "内部应付款", "其他应付款合计", "预提费用", "预计流动负债", "应付分保账款", "保险合同准备金"
            , "代理买卖证券款", "代理承销证券款", "国际票证结算", "国内票证结算", "一年内的递延收益", "应付短期债券"
            , "一年内到期的非流动负债", "其他流动负债", "流动负债合计", "长期借款", "长期应付职工薪酬", "应付债券"
            , "应付债券：优先股", "长期应付款合计", "预计非流动负债", "长期递延收益"
            , "递延所得税负债", "其他非流动负债", "租赁负债", "担保责任赔偿准备金", "划分为持有待售的负债",
                       "非流动负债合计", "负债合计"]
        zifu_cols = zichan_cols + fuzhai_cols
        # 获取实收资本
        capitals = assets_df[assets_df["指标名称"].str.contains("实收资本")].copy().iloc[:, -1].tolist()
        if capitals:
            self.article_datas["实收资本"] = [f"实收资本：{capitals[-1]}万元"]
        # 处理资产负债表
        zichan_df = assets_df[assets_df["指标名称"].isin(zifu_cols)].copy()
        zichan_df['指标名称'] = pd.Categorical(zichan_df['指标名称'], categories=zifu_cols, ordered=True)
        zichan_df_sorted = zichan_df.sort_values(by="指标名称")
        # 替换部分名称
        zichan_df_sorted["指标名称"] = zichan_df_sorted["指标名称"].replace(
            {"其他应收款合计": "其他应收款", "固定资产及清理合计": "固定资产", "在建工程合计": "在建工程",
            "流动资产合计": "流动资产", "非流动资产合计": "非流动资产", "其他应付款合计": "其他应付款",
            "长期应付款合计": "长期应付款", "流动负债合计": "流动负债", "非流动负债合计": "非流动负债"})
        zichan_df_sorted = zichan_df_sorted.iloc[:, [0, zichan_df_sorted.shape[-1] - 1]]
        zichan_df_sorted = zichan_df_sorted.dropna(subset=zichan_df_sorted.columns.difference(['指标名称']), how='all')
        zichan_df_sorted = zichan_df_sorted[zichan_df_sorted.iloc[:, 1] >= 10000]
        zichan_df_sorted.iloc[:, 1] = (zichan_df_sorted.iloc[:, 1] / 10000).round(2)
        zichan_data_list = [zichan_df_sorted.columns.tolist()] + zichan_df_sorted.values.tolist()
        tab_date = self.get_max_date(zichan_data_list)
        tab_unit = "亿"
        self.table_datas.append(
            {'name': '资产负债表', 'date': tab_date, 'header_rows': 1, 'unit': tab_unit, 'data': zichan_data_list})

    def assets_statis(self, assets_path):
        # 资产处理
        assets_df = pd.read_csv(assets_path, encoding='gbk')
        assets_df = assets_df[~assets_df.apply(lambda row: all(_cell == '-' for _cell in row), axis=1)]
        zichan_cols = ["货币资金", "结算备付金", "拆出资金", "应收保证金", "应收利息", "应收票据", "应收账款"
            , "应收款项融资", "应收保费", "应收分保账款", "应收分保合同准备金", "应收出口退税", "应收补贴款"
            , "内部应收款", "预付款项", "其他应收款合计", "存货", "买入返售金融资产", "交易性金融资产", "衍生金融资产"
            , "一年内到期的非流动资产", "待处理流动资产损益", "其他流动资产", "流动资产合计", "发放贷款及垫款",
                       "可供出售金融资产"
            , "划分为持有待售的资产", "以公允价值计量且其变动计入其他综合收益的金融资产", "以摊余成本计量的金融资产"
            , "债权投资", "其他债权投资", "其他权益工具投资", "其他非流动金融资产", "长期应收款", "长期股权投资", "待摊费用"
            , "其他长期投资", "投资性房地产", "固定资产及清理合计", "合同资产", "在建工程合计", "使用权资产", "工程物资"
            , "生产性生物资产", "公益性生物资产", "油气资产", "无形资产", "开发支出", "商誉", "长期待摊费用"
            , "股权分置流通权", "递延所得税资产", "其他非流动资产", "非流动资产合计", "资产总计"]
        zichan_df = assets_df[assets_df["指标名称"].isin(zichan_cols)].copy()
        zichan_df['指标名称'] = pd.Categorical(zichan_df['指标名称'], categories=zichan_cols, ordered=True)
        zichan_df_sorted = zichan_df.sort_values(by="指标名称")
        # 替换部分名称
        zichan_df_sorted["指标名称"] = zichan_df_sorted["指标名称"].replace(
            {"其他应收款合计": "其他应收款", "固定资产及清理合计": "固定资产", "在建工程合计": "在建工程",
             "流动资产合计": "流动资产", "非流动资产合计": "非流动资产"})
        zichan_df_sorted = zichan_df_sorted.iloc[:, [0, zichan_df_sorted.shape[-1] - 1]]
        zichan_df_sorted = zichan_df_sorted.dropna(subset=zichan_df_sorted.columns.difference(['指标名称']), how='all')
        zichan_df_sorted = zichan_df_sorted[zichan_df_sorted.iloc[:, 1] >= 10000]
        zichan_df_sorted.iloc[:, 1] = (zichan_df_sorted.iloc[:, 1] / 10000).round(2)
        zichan_zongji_df = zichan_df_sorted[zichan_df_sorted['指标名称'] == '资产总计']
        zichan_data_list = [zichan_df_sorted.columns.tolist()] + zichan_df_sorted.values.tolist()
        tab_date = self.get_max_date(zichan_data_list)
        tab_unit = "亿"
        self.table_datas.append(
            {'name': '资产统计', 'date': tab_date, 'header_rows': 1, 'unit': tab_unit, 'data': zichan_data_list})
        fuzhai_cols = ["短期借款", "向中央银行借款", "吸收存款及同业存放", "拆入资金", "交易性金融负债"
            , "衍生金融负债", "卖出回购金融资产款", "应付手续费及佣金", "应付票据", "应付账款", "预收款项"
            , "合同负债", "应付职工薪酬", "应交税费", "应付利息", "应付股利", "其他应交款", "应付保证金"
            , "内部应付款", "其他应付款合计", "预提费用", "预计流动负债", "应付分保账款", "保险合同准备金"
            , "代理买卖证券款", "代理承销证券款", "国际票证结算", "国内票证结算", "一年内的递延收益", "应付短期债券"
            , "一年内到期的非流动负债", "其他流动负债", "长期借款", "长期应付职工薪酬", "应付债券"
            , "应付债券：优先股", "长期应付款合计", "预计非流动负债", "长期递延收益"
            , "递延所得税负债", "其他非流动负债", "租赁负债", "担保责任赔偿准备金", "划分为持有待售的负债", "负债合计"]
        fuzhai_df = assets_df[assets_df["指标名称"].isin(fuzhai_cols)].copy()
        fuzhai_df["指标名称"] = pd.Categorical(fuzhai_df['指标名称'], categories=fuzhai_cols, ordered=True)
        fuzhai_df_sorted = fuzhai_df.sort_values(by="指标名称")

        fuzhai_df_sorted["指标名称"] = fuzhai_df_sorted["指标名称"].replace(
            {"其他应付款合计": "其他应付款", "长期应付款合计": "长期应付款"})
        fuzhai_df_sorted = fuzhai_df_sorted.iloc[:, [0, fuzhai_df_sorted.shape[-1] - 1]]
        fuzhai_df_sorted = fuzhai_df_sorted.dropna(subset=fuzhai_df_sorted.columns.difference(['指标名称']), how='all')
        fuzhai_df_sorted = fuzhai_df_sorted[fuzhai_df_sorted.iloc[:, 1] >= 10000]
        fuzhai_df_sorted.iloc[:, 1] = (fuzhai_df_sorted.iloc[:, 1] / 10000).round(2)
        fuzhai_df_sorted = pd.concat([fuzhai_df_sorted, zichan_zongji_df], ignore_index=False)
        fuzhai_data_list = [fuzhai_df_sorted.columns.tolist()] + fuzhai_df_sorted.values.tolist()
        self.table_datas.append(
            {'name': '负债统计', 'date': tab_date, 'header_rows': 1, 'unit': tab_unit, 'data': fuzhai_data_list})

    def dcm_regist(self, dcm_path):
        dcm_df = pd.read_csv(dcm_path, encoding='gbk')
        dcm_df.replace(np.nan, '', inplace=True)
        dcm_data_list = [dcm_df.columns.tolist()] + dcm_df.values.tolist()
        _data_date = uniform_date(os.path.basename(dcm_path), '%Y%m')
        _tab_unit = "亿"
        self.table_datas.append(
            {'name': '注册未发行债券', 'date': _data_date, 'header_rows': 1, 'unit': _tab_unit, 'data': dcm_data_list})

    def get_debtor_amount(self, debtor_path):
        """被执行人金额"""
        debtor_df = pd.read_csv(debtor_path, encoding='gbk')
        debtor_data_list = [debtor_df.columns.tolist()] + debtor_df.values.tolist()
        _data_date = datetime.today().strftime("%Y%m")
        self.table_datas.append(
            {'name': '被执行金额', 'date': _data_date, 'header_rows': 1, 'unit': '', 'data': debtor_data_list})

    def get_no_standard_finance(self, no_standard_finance_path):
        """非标融资"""
        no_standard_finance_df = pd.read_csv(no_standard_finance_path, encoding='gbk')
        no_standard_finance_df = no_standard_finance_df[
            ~no_standard_finance_df.apply(lambda row: all(_cell == '-' for _cell in row), axis=1)]
        no_standard_finance_df = no_standard_finance_df.drop("截止日期", axis=1)
        no_standard_finance_df.replace(19000101, "", inplace=True)
        no_standard_finance_df.replace(np.nan, "", inplace=True)
        no_standard_finance_list = [no_standard_finance_df.columns.tolist()] + no_standard_finance_df.values.tolist()
        _unit = self.get_table_unit(no_standard_finance_list)
        _data_date = uniform_date(os.path.basename(no_standard_finance_path), '%Y%m')
        self.table_datas.append(
            {'name': '非标融资', 'date': _data_date, 'header_rows': 1, 'unit': _unit, 'data': no_standard_finance_list})

    def get_credit_limit(self, credit_limit_path):
        """授信额度"""
        credit_limit_df = pd.read_csv(credit_limit_path, encoding='gbk')
        credit_limit_df = credit_limit_df[
            ~credit_limit_df.apply(lambda row: all(_cell == '-' for _cell in row), axis=1)]
        _unit = credit_limit_df["单位"].max()
        _data_date = uniform_date(os.path.basename(credit_limit_path), '%Y%m')
        credit_limit_df = credit_limit_df.drop(["截止时间", "单位"], axis=1)
        credit_limit_list = [credit_limit_df.columns.tolist()] + credit_limit_df.values.tolist()
        self.table_datas.append(
            {'name': '授信额度', 'date': _data_date, 'header_rows': 1, 'unit': _unit, 'data': credit_limit_list})

    def get_bear_debt(self, bear_debt_path):
        """有息负债"""
        bear_debt_df = pd.read_csv(bear_debt_path, encoding='gbk')
        bear_debt_df = bear_debt_df[~bear_debt_df.apply(lambda row: all(_cell == '-' for _cell in row), axis=1)]
        bear_debt_list = [bear_debt_df.columns.tolist()] + bear_debt_df.values.tolist()
        # _unit = self.get_table_unit(bear_debt_list)
        _data_date = self.get_recent_date(bear_debt_list)
        self.table_datas.append(
            {'name': '有息负债', 'date': _data_date, 'header_rows': 1, 'unit': '万', 'data': bear_debt_list})

    def yingshou_zhangkuan(self, yszk_path):
        """应收账款"""
        yszk_df = pd.read_csv(yszk_path, encoding='gbk')
        yszk_df = yszk_df[~yszk_df.apply(lambda row: all(_cell == '-' for _cell in row), axis=1)]
        yszk_df = yszk_df.drop("报表日期", axis=1)
        yszk_list = [yszk_df.columns.tolist()] + yszk_df.values.tolist()
        _unit = "万"
        _data_date = uniform_date(os.path.basename(yszk_path), '%Y%m')
        self.table_datas.append({'name': '应收账款', 'date': _data_date, 'header_rows': 1, 'unit': _unit, 'data': yszk_list})

    def qita_yingshou(self, qtys_path):
        """其他应收款"""
        qtys_df = pd.read_csv(qtys_path, encoding='gbk')
        qtys_df = qtys_df[~qtys_df.apply(lambda row: all(_cell == '-' for _cell in row), axis=1)]
        qtys_df = qtys_df.drop("截止日期", axis=1)
        qtys_list = [qtys_df.columns.tolist()] + qtys_df.values.tolist()
        _unit = "万"
        _data_date = uniform_date(os.path.basename(qtys_path), '%Y%m')
        self.table_datas.append(
            {'name': '其他应收款', 'date': _data_date, 'header_rows': 1, 'unit': _unit, 'data': qtys_list})

    def youxi_fuzhai(self, yxfz_path):
        """有息负债"""
        yxfz_df = pd.read_csv(yxfz_path, encoding='gbk')
        yxfz_df = yxfz_df[~yxfz_df.apply(lambda row: all(_cell == '-' for _cell in row), axis=1)]
        _subset = [col for col in yxfz_df.columns.tolist() if col != '指标名称']
        yxfz_df = yxfz_df.dropna(subset=_subset, how='all')
        yxfz_list = [yxfz_df.columns.tolist()] + yxfz_df.values.tolist()
        _unit = "万"
        _data_date = uniform_date(os.path.basename(yxfz_path), '%Y%m')
        self.table_datas.append(
            {'name': '有息负债', 'date': _data_date, 'header_rows': 1, 'unit': _unit, 'data': yxfz_list})

    def guquan_jiegou(self, gqjg_path):
        """股权结构"""
        gqjg_df = pd.read_csv(gqjg_path, encoding='gbk')
        gqjg_df = gqjg_df[["股东名称", "持股数量", "持股比例"]].copy().astype(str)
        gqjg_df['持股比例'] = gqjg_df['持股比例'].apply(lambda c: str(c) + "%")
        gqjg_list = [gqjg_df.columns.tolist()] + gqjg_df.values.tolist()
        _unit = ""
        _data_date = uniform_date(os.path.basename(gqjg_path), '%Y%m')
        self.table_datas.append(
            {'name': '股权结构', 'date': _data_date, 'header_rows': 1, 'unit': _unit, 'data': gqjg_list})

    def zhuti_pingji(self, ztpj_path):
        """主体评级"""
        try:
            ztpj_df_datas = pd.read_csv(ztpj_path, encoding='gbk')
            ztpj_df = ztpj_df_datas[["公司名称", "主体评级"]].copy().astype(str)
            ztpj_list = [ztpj_df.columns.tolist()] + ztpj_df.values.tolist()
            _unit = ""
            _data_date = uniform_date(os.path.basename(ztpj_path), '%Y%m')
            self.table_datas.append(
                {'name': '发行主体评级', 'date': _data_date, 'header_rows': 1, 'unit': _unit, 'data': ztpj_list})
            # 获取实际控制人
            actual_controller = ztpj_df_datas['实际控制人'].values[0]
            if actual_controller:
                self.article_datas["实际控制人"] = [actual_controller]
            else:
                logger.error("未获取到实际控制人")
        except pd.errors.EmptyDataError:
            print("主体评级数据为空")

    def get_method_by_keyword(self, file_name):

        methods = {
            "地方区域经济": self.get_area_rank,
            "债务公司": self.get_debtor_amount,
            "存量债券": self.outstanding_bonds,
            "非标融资": self.get_no_standard_finance,
            "授信情况": self.get_credit_limit,
            "区域发行平台": self.get_urban_invest_rank,
            "DCM注册额度": self.dcm_regist,
            "现金流": self.cash_flow,
            "资产负债": self.assets,
            "应收账款": self.yingshou_zhangkuan,
            "其他应收款": self.qita_yingshou,
            "股权结构": self.guquan_jiegou,
            "发行主体评级": self.zhuti_pingji,
        }

        for key, method in methods.items():
            if key in file_name:
                return method

    def get_statis_method_by_keyword(self, file_name):
        methods = {
            "存量债券统计": self.outstanding_bonds_statis,
            "资产负债统计": self.assets_statis,
        }

        for key, method in methods.items():
            if key.replace("统计", "") in file_name:
                return method


    def get_yjt_data(self):
        self.table_datas = []
        """获取预警通数据"""
        print(self.ext_data_dir)
        for root, dirs, files in os.walk(self.ext_data_dir):
            for file_name in files:
                method1 = self.get_method_by_keyword(file_name)
                if method1:
                    method1(os.path.join(root, file_name))
                method2 = self.get_statis_method_by_keyword(file_name)
                if method2:
                    method2(os.path.join(root, file_name))
        return self.table_datas

if __name__ == "__main__":
    data_dir = "D:/opt/城投债/20241127"
    company_name = "成都菁弘投资集团有限公司"
    handler = YjtDBDataHandler(company_name, data_dir)
    yjt_datas = handler.get_yjt_data()
    for yjt_data in yjt_datas:
        print(yjt_data)