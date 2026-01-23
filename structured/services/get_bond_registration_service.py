#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Description：公司债券注册分析服务，分析指定公司的债券注册情况

from pathlib import Path
import sys

project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from sqlmodel import Session, select, and_
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from decimal import Decimal

# 导入模型
from structured.models.tq_bd_projectparty_model import TqBdProjectParty
from structured.models.tq_bd_projectinfo_model import TqBdProjectInfo
from structured.models.tq_bd_pproesscorr_model import TqBdPproesscorr
from structured.models.tq_bd_bondissapp_model import TqBdBondissapp
from structured.models.tq_bd_issueregister_model import TqBdIssueregister
from structured.models.tq_bd_ndrcabondinfo_model import TqBdNdrcabondinfo

from utils.db_util import engine
from utils.logger_util import get_logger

logger = get_logger()

# 项目类别字典映射
PROJECTTYPE_DICT = {
    '1': '资产支持证券', '2': '小公募', '3': '私募', '4': 'MTN',
    '5': 'SCP', '6': 'CP', '7': 'DFI', '8': 'ABN',
    '9': 'PN', '10': 'PRN', '11': 'PB', '12': 'PB-MTN',
    '13': '大公募', '14': '企业债券', '15': 'PB-CP', '16': 'SMECN',
    '17': 'PB-RB', '18': 'TDFI', '19': 'PPN', '20': 'PB-DFI', '21': 'CB'
}

# 项目进展状况字典映射
PROCESSTYPE_DICT = {
    '1': '交易所已受理项目', '2': '交易所已反馈意见', '3': '交易所已接收反馈意见',
    '4': '项目主体已回复交易所意见', '5': '交易所通过项目', '6': '交易所未通过项目',
    '7': '交易所终止项目', '8': '交易所中止项目', '9': '交易商协会已受理',
    '10': '交易商协会预评中', '11': '交易商协会反馈中', '12': '交易商协会待上会',
    '13': '交易商协会已上会', '14': '交易商协会完成注册', '15': '提交注册（交易所）',
    '16': '注册生效（交易所）', '17': '不予注册（交易所）', '18': '中央结算公司已申报',
    '19': '中央结算公司受理反馈待答复', '20': '中央结算公司已受理', '21': '已注册（中央结算公司）',
    '22': '中央结算公司终止受理', '23': '中央结算公司已终止', '24': '中央结算已撤销申请',
    '25': '交易商协会待受理', '26': '交易商协会已撤回', '27': '交易商协会链接失效',
    '28': '终止注册（交易所）'
}


def get_register_amount(
        session: Session,
        regtable: Optional[str],
        regid: Optional[int],
        register_amt: Optional[Decimal],
        plan_amount: Optional[Decimal]
) -> Optional[float]:
    """
    获取注册额度

    :param session: 数据库会话
    :param regtable: 注册表名
    :param regid: 注册ID
    :param register_amt: 注册金额
    :param plan_amount: 计划金额
    :return: 注册额度（万元）
    """
    try:
        if regtable and regid:
            amount = None

            if regtable == 'TQ_BD_BONDISSAPP':
                result = session.exec(
                    select(TqBdBondissapp).where(TqBdBondissapp.BONDISSAPPID == regid)
                ).first()
                if result:
                    amount = result.SCALE
            elif regtable == 'TQ_BD_ISSUEREGISTER':
                result = session.exec(
                    select(TqBdIssueregister).where(TqBdIssueregister.BONDIRIID == regid)
                ).first()
                if result:
                    amount = result.REGISTERLIMIT
            elif regtable == 'TQ_BD_NDRCABONDINFO':
                result = session.exec(
                    select(TqBdNdrcabondinfo).where(TqBdNdrcabondinfo.NDRCABONDINFOID == regid)
                ).first()
                if result:
                    amount = result.AMOUNT

            if amount is not None:
                return float(amount)

        if register_amt is not None:
            return float(register_amt)

        if plan_amount is not None:
            return float(plan_amount)

        return None
    except Exception as e:
        logger.error(f"获取注册额度时发生错误: {str(e)}")
        raise


def get_register_enddate(
        session: Session,
        regtable: Optional[str],
        regid: Optional[int],
        register_enddate: Optional[str]
) -> Optional[str]:
    """
    获取注册有效期截止日

    :param session: 数据库会话
    :param regtable: 注册表名
    :param regid: 注册ID
    :param register_enddate: 注册截止日期
    :return: 注册有效期截止日
    """
    try:
        if register_enddate and register_enddate != '19000101':
            return register_enddate

        if regtable and regid:
            enddate = None

            if regtable == 'TQ_BD_BONDISSAPP':
                result = session.exec(
                    select(TqBdBondissapp).where(TqBdBondissapp.BONDISSAPPID == regid)
                ).first()
                if result:
                    enddate = result.REPENDDATE
            elif regtable == 'TQ_BD_ISSUEREGISTER':
                result = session.exec(
                    select(TqBdIssueregister).where(TqBdIssueregister.BONDIRIID == regid)
                ).first()
                if result:
                    enddate = result.REGISTERENDDATE
            elif regtable == 'TQ_BD_NDRCABONDINFO':
                result = session.exec(
                    select(TqBdNdrcabondinfo).where(TqBdNdrcabondinfo.NDRCABONDINFOID == regid)
                ).first()
                if result:
                    enddate = result.REGISTERENDDATE

            if enddate and enddate != '19000101':
                return enddate

        return None
    except Exception as e:
        logger.error(f"获取注册截止日期时发生错误: {str(e)}")
        raise


def get_approval_location(
        session: Session,
        regtable: Optional[str],
        regid: Optional[int]
) -> str:
    """
    获取批复场所

    :param session: 数据库会话
    :param regtable: 注册表名
    :param regid: 注册ID
    :return: 批复场所名称
    """
    try:
        if not regtable:
            return "交易所"

        if regtable == 'TQ_BD_NDRCABONDINFO':
            return "发改委"
        elif regtable == 'TQ_BD_ISSUEREGISTER':
            return "交易商协会"
        elif regtable == 'TQ_BD_BONDISSAPP':
            if regid:
                result = session.exec(
                    select(TqBdBondissapp).where(TqBdBondissapp.BONDISSAPPID == regid)
                ).first()
                if result and result.REPCOMPCODE:
                    if result.REPCOMPCODE == '80043190':
                        return "证监会"
                    elif result.REPCOMPCODE in ['82832192', '80062785', '80972143']:
                        return "金融监管总局"
            return "未知机构"
        else:
            return f"未知表({regtable})"
    except Exception as e:
        logger.error(f"获取批复场所时发生错误: {str(e)}")
        raise


def parse_date(date_str: str) -> Optional[datetime]:
    """
    解析日期字符串

    :param date_str: 日期字符串
    :return: datetime对象
    """
    if not date_str or date_str == '19000101':
        return None
    try:
        for fmt in ['%Y%m%d', '%Y-%m-%d', '%Y/%m/%d']:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        return None
    except Exception:
        return None


def calculate_validity_period(
        project_type: str,
        process_date: str,
        register_enddate: Optional[str] = None
) -> Optional[datetime]:
    """
    计算有效期截止日期

    :param project_type: 项目类型
    :param process_date: 进展日期
    :param register_enddate: 注册截止日期
    :return: 有效期截止日期
    """
    try:
        if register_enddate and register_enddate != '19000101':
            return parse_date(register_enddate)

        process_dt = parse_date(process_date)
        if not process_dt:
            return None

        # 根据项目类型确定有效期
        if project_type == '1':  # 资产支持证券
            return process_dt + timedelta(days=730)  # 24个月
        elif project_type == '14':  # 企业债券
            return process_dt + timedelta(days=730)  # 2年
        elif project_type == '3':  # 私募
            return process_dt + timedelta(days=365)  # 1年
        else:
            return process_dt + timedelta(days=365)  # 默认1年
    except Exception as e:
        logger.error(f"计算有效期时发生错误: {str(e)}")
        raise


def get_bond_registration_analysis(
        company_name: str,
        session: Session,
        analysis_date: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    获取指定公司在有效期内的债券注册项目

    :param company_name: 公司名称
    :param session: 数据库会话
    :param analysis_date: 分析日期（可选），格式为YYYYMMDD
    :return: 有效期内的项目列表
    """
    try:
        logger.info(f"开始分析公司 '{company_name}' 的债券注册情况")

        # 设置分析日期
        if analysis_date:
            current_date = parse_date(analysis_date)
            if not current_date:
                raise ValueError("无效的分析日期格式")
        else:
            current_date = datetime.now()

        # 定义允许的 PROCESSTYPE 值
        allowed_process_types = ['1', '10', '11', '12', '13', '18', '19', '2', '20', '25', '3', '4', '8', '9']

        # 查询公司项目信息
        statement = (
            select(
                TqBdProjectInfo.PROJECTTYPE,
                TqBdProjectInfo.PROCESSDATE,
                TqBdProjectInfo.PROCESSTYPE,
                TqBdProjectInfo.REGISTERAMT,
                TqBdProjectInfo.REGISTERENDDATE,
                TqBdProjectInfo.PROJECTID,
                TqBdProjectInfo.PLANAMOUNT,
                TqBdProjectInfo.PROJECTNAME,
                TqBdPproesscorr.REGTABLE,
                TqBdPproesscorr.REGID
            )
            .select_from(TqBdProjectParty)
            .join(
                TqBdProjectInfo,
                TqBdProjectParty.PROJECTCODE == TqBdProjectInfo.PROJECTCODE
            )
            .outerjoin(
                TqBdPproesscorr,
                and_(
                    TqBdProjectInfo.PROJECTID == TqBdPproesscorr.RECORDID,
                    TqBdPproesscorr.RECORDTABLE == 'TQ_BD_PROJECTINFO'
                )
            )
            .where(
                and_(
                    TqBdProjectParty.PARTYTYPE == '2',
                    TqBdProjectParty.PARTYNAME == company_name,
                    TqBdProjectInfo.PROCESSTYPE != '14',
                    TqBdProjectInfo.PROCESSTYPE.in_(allowed_process_types)
                )
            )
        )

        results = session.exec(statement).all()

        if not results:
            logger.info(f"未找到公司 '{company_name}' 的项目数据")
            return []

        # 处理查询结果，只返回有效期内的项目
        valid_projects = []

        for row in results:
            # 获取注册额度
            register_amount = get_register_amount(
                session, row[8], row[9], row[3], row[6]
            )

            # 获取有效截止日期
            valid_enddate = get_register_enddate(
                session, row[8], row[9], row[4]
            )

            # 计算实际截止日期
            actual_enddate = calculate_validity_period(
                row[0], row[1], valid_enddate
            )

            # 判断是否在有效期内
            if actual_enddate and current_date <= actual_enddate:
                # 获取批复场所
                approval_location = get_approval_location(
                    session, row[8], row[9]
                )

                # 获取中文描述
                project_type_name = PROJECTTYPE_DICT.get(str(row[0]), f"未知({row[0]})")
                process_type_name = PROCESSTYPE_DICT.get(str(row[2]), f"未知({row[2]})")

                # 构建响应字典
                project = {
                    "project_name": row[7],
                    "project_type": row[0],
                    "project_type_name": project_type_name,
                    "process_date": row[1],
                    "process_type": row[2],
                    "process_type_name": process_type_name,
                    "plan_amount": float(row[6]) if row[6] else None,
                    "register_amount": register_amount,
                    "register_enddate": valid_enddate,
                    "actual_enddate": actual_enddate.strftime('%Y-%m-%d') if actual_enddate else None,
                    "approval_location": approval_location,
                    "project_id": row[5]
                }

                valid_projects.append(project)

        # 按实际截止日期排序
        valid_projects.sort(key=lambda x: x['actual_enddate'] if x['actual_enddate'] else '9999-12-31')

        logger.info(f"查询成功，找到 {len(valid_projects)} 个有效期内项目")
        return valid_projects

    except Exception as e:
        logger.error(f"分析公司债券注册情况时出错: {e}")
        raise


if __name__ == "__main__":
    # 测试参数
    test_company = "泰安泰山城乡建设发展有限公司"

    with Session(engine) as session:
        try:
            print("=" * 80)
            print(f"查询公司 '{test_company}' 的债券注册项目")
            print("=" * 80)

            result = get_bond_registration_analysis(test_company, session)

            print(f"\n找到 {len(result)} 个有效期内项目:\n")
            for i, project in enumerate(result, 1):
                print(f"项目 {i}:")
                print(f"  项目名称: {project['project_name']}")
                print(f"  项目类型: {project['project_type_name']}")
                print(f"  进展状态: {project['process_type_name']}")
                print(f"  进展日期: {project['process_date']}")
                print(f"  拟发行金额: {project['plan_amount']} 万元")
                print(f"  注册额度: {project['register_amount']} 万元")
                print(f"  注册截止日: {project['register_enddate']}")
                print(f"  实际截止日: {project['actual_enddate']}")
                print(f"  批复场所: {project['approval_location']}")
                print(f"  项目ID: {project['project_id']}")
                print("-" * 80)

        except Exception as e:
            print(f"查询失败: {e}")
