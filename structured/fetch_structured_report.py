#!/usr/bin/env python
# -*- coding:utf-8 -*-
from pathlib import Path
import sys
import json
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))
from utils.logger_util import get_logger
from config.config import LLM_CONFIG
from utils.data_util import parse_json_response
from utils.llm_client2 import LLMClient
from typing import Any, Dict, List
from decimal import Decimal
from sqlmodel import Session
from utils.db_util import engine
from structured.services.get_comp_code_service import get_company_code
from structured.services.get_comp_info_service import get_company_info
from structured.services.get_shareholder_service import get_latest_shareholders
from structured.services.get_platform_scores_service import get_platform_score_by_company_code
from structured.services.get_platspreadstat_service import get_plat_spreads
from structured.services.get_creditrtissue_service import get_credit_ratings, get_latest_credit_rating
from structured.services.get_bond_info_service import get_bond_info
from structured.services.get_bond_registration_service import get_bond_registration_analysis
from structured.services.get_guaranteedetails_service import get_guarantors
from structured.services.get_economic_indicators_service import (
    get_latest_prgindicdata,
    get_latest_prgbalsheetnew,
    get_latest_regifin,
    get_latest_regifin_by_company
)
from structured.services.get_regifinaplat_service import get_regifinaplat_by_itcode, get_same_affiliation_platforms

logger = get_logger()

class YJTCompanyDataFetcher:
    """预警通数据解析"""
    def __init__(self, company_name: str = None, llm: LLMClient | None = None) -> None:
        self.llm = llm or LLMClient(LLM_CONFIG["llm"])
        self.company_name = company_name.strip() if company_name else ""
        self.company_code, self.unified_credit_code = self._get_company_code() # 公司代码、社会信用代码
        self.guarantors = self._get_guarantors() # 担保人信息
        self.credit_ratings = self._get_all_credit_ratings() # 主体所有信用评级信息
        self.bonds_info = self._get_bonds_info() # 主体债券信息

    def _get_company_code(self) -> tuple[str, str]:
        """获取公司代码和统一社会信用代码"""
        try:
            logger.info(f"获取公司【{self.company_name}】代码信息")
            with Session(engine) as session:
                from structured.services.get_comp_info_service import get_compcode_by_name
                comcode = get_compcode_by_name(self.company_name, session)
                logger.info(f"从机构资料表获取到代码: {comcode}")
                # 查询统一社会信用代码
                response = get_company_code(self.company_name, session)
                outcode = response.OUTCODE or ""
                logger.info(f"公司代码: {comcode}, 统一社会信用代码: {outcode or '无'}")
                return comcode, outcode
        except Exception as e:
            logger.error(f"获取公司代码失败: {e}")
            raise

    def _get_guarantors(self, company_code: str = None) -> List[Dict[str, Any]]:
        """获取担保人信息"""
        try:
            logger.info(f"获取【{self.company_name}】担保人信息")
            with Session(engine) as session:
                guarantors = get_guarantors(company_code or self.company_code, session)
                return [guarantor.model_dump(mode='json') for guarantor in guarantors] if guarantors else []
        except Exception as e:
            logger.error(f"获取担保人信息失败: {e}")
            return []

    def _get_all_credit_ratings(self, company_code: str = None) -> List[Dict[str, Any]]:
        """获取公司信用评级信息列表"""
        try:
            logger.info(f"获取【{self.company_name}】所有信用评级信息")
            with Session(engine) as session:
                ratings = get_credit_ratings(company_code or self.company_code, session)
                return [rating.model_dump(mode='json') for rating in ratings] if ratings else []
        except Exception as e:
            logger.error(f"获取公司所有信用评级信息失败: {e}")
            return []

    def _get_latest_credit_rating(self, company_code: str = None) -> Dict[str, Any] | None:
        """获取公司最新信用评级信息（单条记录）"""
        try:
            logger.info(f"获取【{self.company_name}】最新信用评级")
            with Session(engine) as session:
                rating = get_latest_credit_rating(company_code or self.company_code, session)
                return rating.model_dump(mode='json') if rating else None
        except Exception as e:
            logger.error(f"获取公司最新信用评级信息失败: {e}")
            return None

    def _get_bonds_info(self, company_code: str = None) -> Dict[str, Any]:
        """获取公司所有债券详细信息"""
        try:
            logger.info(f"获取【{self.company_name}】债券详细信息")
            with Session(engine) as session:
                bond_info = get_bond_info(company_code or self.company_code, session)
                return bond_info.model_dump(mode='json')
        except Exception as e:
            logger.error(f"获取债券详细信息失败: {e}")
            return {}

    def _get_rating_requirements1(self) -> Dict:
        """评级要求1:主体评级不低于AA+,或担保人不低于AA+,或债项评级(如有)不低于AA+"""
        logger.info(f"获取【{self.company_name}】公司评级要求1")
        high_ratings = ['AAA', 'AAA-', 'AA+']

        try:
            # 1. 检查主体评级
            subject_rating = ""
            if self.credit_ratings:
                for rating in self.credit_ratings:
                    subject_rating = rating.get("CREDITRATE", "")
                    if subject_rating:
                        if subject_rating in high_ratings:
                            logger.debug(f"主体评级 {subject_rating} 符合标准")
                            return {
                                "申请人指标说明": f"主体评级为{subject_rating}",
                                "符合/不符合": "符合"
                            }
                        break

            # 2. 检查担保人评级
            guarantor_rating = "无"
            guarantor_name = ""
            if self.guarantors:
                for guarantor in self.guarantors:
                    guar_code = guarantor.get("GUARCODE")
                    guar_name = guarantor.get("GUARNAME", "")

                    if guar_code:
                        # 查询担保人的评级信息
                        guar_rating_info = self._get_latest_credit_rating(guar_code)
                        if guar_rating_info:
                            rating = guar_rating_info.get("CREDITRATE", "")
                            if rating in high_ratings:
                                logger.debug(f"担保人 {guar_name} 评级 {rating} 符合标准")
                                return {
                                    "申请人指标说明": f"担保人{guar_name}评级为{rating}",
                                    "符合/不符合": "符合"
                                }
                            # 记录第一个有效担保人评级
                            if not guarantor_rating or guarantor_rating == "无":
                                guarantor_rating = rating
                                guarantor_name = guar_name

            # 3. 检查债项评级
            bond_rating = "无"
            bonds_list = self.bonds_info.get("bonds", [])

            if bonds_list:
                for bond in bonds_list:
                    bond_rating = bond.get("creditrate", "")
                    if bond_rating and bond_rating in high_ratings:
                        logger.debug(f"债项评级 {bond_rating} 符合标准")
                        return {
                            "申请人指标说明": f"债项评级为{bond_rating}",
                            "符合/不符合": "符合"
                        }
                    if bond_rating:
                        break

            # 4. 都不符合,返回不符合结果
            logger.debug("主体、担保人、债项评级均不符合AA+及以上标准")

            # 构建说明文本
            parts = []
            if subject_rating:
                parts.append(f"主体评级{subject_rating}")
            else:
                parts.append("主体评级无")

            if guarantor_name:
                parts.append(f"担保人{guarantor_name}评级{guarantor_rating}")
            else:
                parts.append("担保人评级无")

            if bond_rating and bond_rating != "无":
                parts.append(f"债项评级{bond_rating}")
            else:
                parts.append("债项评级无")

            explanation = "、".join(parts) + ",均不符合AA+及以上要求"

            return {
                "申请人指标说明": explanation,
                "符合/不符合": "不符合"
            }

        except Exception as e:
            logger.error(f"查询评级要求1时出错: {e}")
            return {
                "申请人指标说明": "",
                "符合/不符合": ""
            }

    def _get_regifinaplat_by_itcode(self, company_code: str = None) -> Dict[str, Any] | None:
        """获取当前公司的地方融资平台信息"""
        try:
            logger.info(f"获取【{self.company_name}】融资平台信息")
            with Session(engine) as session:
                platform = get_regifinaplat_by_itcode(company_code or self.company_code, session)
                if platform:
                    return {
                        "itcode": platform.ITCODE,
                        "itname": platform.ITNAME,
                        "finaffcode": platform.FINAFFCODE,
                        "finaffname": platform.FINAFFNAME,
                        "territorytype": platform.TERRITORYTYPE,
                        "bondbalance": platform.BONDBALANCE,
                        "reglanname_p": platform.REGLANNAME_P,
                        "reglanname_c": platform.REGLANNAME_C
                    }
                return None
        except Exception as e:
            logger.error(f"获取融资平台信息失败: {e}")
            return None

    def _get_rating_requirements2(self) -> Dict:
        """评级要求2：江苏省内13个地级市的市级平台，或江苏省外一般预算收入超150亿元的市级平台，主体、担保人或债项评级不低于AA"""
        logger.info(f"获取【{self.company_name}】公司评级要求2")
        high_ratings = ['AAA', 'AAA-', 'AA+', 'AA']

        try:
            # Step 1: 获取平台信息
            platform_info = self._get_regifinaplat_by_itcode()

            if not platform_info or all(value is None or value == "" for value in platform_info.values()):
                logger.warning(f"公司 {self.company_name} 的融资平台信息所有字段均为None")
                return {
                    "申请人指标说明": "",
                    "符合/不符合": ""
                }

            region_level = platform_info.get("territorytype") or "无"
            province = platform_info.get("reglanname_p") or "无"
            city = platform_info.get("reglanname_c") or "无"
            is_jiangsu = province == "江苏省"
            is_city_level = region_level == "地市级"

            # Step 2: 检查是否为江苏省内市级平台
            if is_jiangsu and is_city_level:
                logger.debug(f"公司 {self.company_name} 为江苏省内地市级平台")
                desc_str = f"{city}为江苏省内地市级平台"
                return {
                    "申请人指标说明": desc_str,
                    "符合/不符合": "符合"
                }

            # Step 3: 对于省外，检查是否为市级平台并获取预算收入
            budget_income = 0.0
            budget_unit = "亿元"
            if not is_jiangsu and is_city_level:
                finaffcode = platform_info.get("finaffcode", "")
                if finaffcode:
                    budget_records = self._get_general_budget_income(int(finaffcode))
                    if budget_records and budget_records[0].get("MVALUE", "N/A") != "N/A":
                        budget_income = float(budget_records[0].get("MVALUE"))
                        budget_unit = budget_records[0].get("DISPLAYCUNIT_VALUE", "亿元")
                logger.debug(
                    f"公司 {self.company_name} 注册地{province}{city}, 预算收入: {budget_income} {budget_unit}")

            # Step 4: 检查省外平台是否符合预算收入要求
            meets_platform_requirement = not is_jiangsu and is_city_level and budget_income > 150
            if not (is_jiangsu and is_city_level) and not meets_platform_requirement:
                desc_str = f"注册地{province}{city}"
                desc_str += f", 预算收入{budget_income:.2f}{budget_unit}" if budget_income else ""
                desc_str += f", 层级为{region_level}" if region_level else ""
                logger.debug(f"公司 {self.company_name} 不符合平台要求, 层级为{region_level}")
                return {
                    "申请人指标说明": desc_str,
                    "符合/不符合": "不符合"
                }

            # Step 5: 检查评级
            # 1. 检查主体评级
            subject_rating = self._get_latest_credit_rating()
            subject_rating_value = "无"
            if subject_rating:
                subject_rating_value = subject_rating.get("CREDITRATE") or "无"
                if subject_rating_value in high_ratings:
                    logger.debug(f"主体评级 {subject_rating_value} 符合标准")
                    desc_str = f"注册地{province}{city}, 预算收入{budget_income:.2f}{budget_unit}, 主体评级{subject_rating_value}"
                    return {
                        "申请人指标说明": desc_str,
                        "符合/不符合": "符合"
                    }

            # 2. 检查担保人评级
            guarantor_rating = "无"
            guarantor_name = "无"
            if self.guarantors:
                for guarantor in self.guarantors:
                    guar_code = guarantor.get("GUARCODE")
                    guarantor_name = guarantor.get("GUARNAME") or "无"
                    if guar_code:
                        rating_info = self._get_latest_credit_rating(guar_code)
                        if rating_info:
                            rating = rating_info.get("CREDITRATE") or "无"
                            if rating in high_ratings:
                                logger.debug(f"担保人 {guarantor_name} 评级 {rating} 符合标准")
                                desc_str = f"注册地{province}{city}, 预算收入{budget_income:.2f}{budget_unit}, 担保人{guarantor_name}评级{rating}"
                                return {
                                    "申请人指标说明": desc_str,
                                    "符合/不符合": "符合"
                                }
                            if guarantor_rating == "无":
                                guarantor_rating = rating

            # 3. 检查债项评级
            bond_rating = "无"
            bonds_list = self.bonds_info.get("bonds", [])

            if bonds_list:
                for bond in bonds_list:
                    bond_rating = bond.get("creditrate") or "无"
                    if bond_rating and bond_rating != "无":
                        if bond_rating in high_ratings:
                            logger.debug(f"债项评级 {bond_rating} 符合标准")
                            desc_str = f"注册地{province}{city}, 预算收入{budget_income:.2f}{budget_unit}, 债项评级{bond_rating}"
                            return {
                                "申请人指标说明": desc_str,
                                "符合/不符合": "符合"
                            }
                        break

            # 如果都不符合，输出评级信息
            logger.debug("主体、担保人、债项评级均不符合标准")
            desc_str = f"注册地{province}{city}, 预算收入{budget_income:.2f}{budget_unit}, 主体评级{subject_rating_value}, 担保人{guarantor_name}评级{guarantor_rating}, 债项评级{bond_rating}"
            return {
                "申请人指标说明": desc_str,
                "符合/不符合": "不符合"
            }

        except Exception as e:
            logger.error(f"检查评级要求2失败: {str(e)}")
            return {
                "申请人指标说明": "",
                "符合/不符合": ""
            }

    def _get_rating_requirements3(self) -> Dict:
        """评级要求3：区县级平台，且一般预算收入超过20亿元，主体含担保人及债项评级（如有）可放宽至AA"""
        logger.info(f"获取【{self.company_name}】公司评级要求3")
        high_ratings = ['AAA', 'AAA-', 'AA+', 'AA']

        try:
            # Step 1: 获取平台信息
            platform_info = self._get_regifinaplat_by_itcode()

            if not platform_info or all(value is None or value == "" for value in platform_info.values()):
                logger.warning(f"公司 {self.company_name} 的融资平台信息所有字段均为None")
                return {
                    "申请人指标说明": "",
                    "符合/不符合": ""
                }

            region_level = platform_info.get("territorytype") or "无"
            province = platform_info.get("reglanname_p") or "无"
            city = platform_info.get("reglanname_c") or "无"

            # Step 2: 检查是否为区县级平台
            if region_level != "县市级":
                logger.debug(f"公司 {self.company_name} 为{region_level}平台，非区县级")
                desc_str = f"注册地{province}{city}, 层级为{region_level}，非区县级平台"
                return {
                    "申请人指标说明": desc_str,
                    "符合/不符合": "不符合"
                }

            # Step 3: 查询一般公共预算收入
            budget_income = 0.0
            budget_unit = "亿元"
            finaffcode = platform_info.get("finaffcode", "")
            if finaffcode:
                budget_records = self._get_general_budget_income(int(finaffcode))
                if budget_records and budget_records[0].get("MVALUE", "N/A") != "N/A":
                    budget_income = float(budget_records[0].get("MVALUE"))
                    budget_unit = budget_records[0].get("DISPLAYCUNIT_VALUE", "亿元")
            logger.debug(f"公司 {self.company_name} 注册地{province}{city}, 预算收入: {budget_income} {budget_unit}")

            # Step 4: 检查预算收入是否满足要求
            meets_budget_requirement = budget_income >= 20
            if not meets_budget_requirement:
                desc_str = f"注册地{province}{city}, 预算收入{budget_income:.2f}{budget_unit}, 层级为{region_level}"
                logger.debug(f"公司 {self.company_name} 预算收入 {budget_income}{budget_unit}，不足20亿元")
                return {
                    "申请人指标说明": desc_str,
                    "符合/不符合": "不符合"
                }

            # Step 5: 检查主体评级
            subject_rating = self._get_latest_credit_rating()
            subject_rating_value = "无"
            if subject_rating:
                subject_rating_value = subject_rating.get("CREDITRATE") or "无"
                if subject_rating_value in high_ratings:
                    logger.debug(f"主体评级 {subject_rating_value} 符合标准")
                    desc_str = f"注册地{province}{city}, 预算收入{budget_income:.2f}{budget_unit}, 主体评级{subject_rating_value}"
                    return {
                        "申请人指标说明": desc_str,
                        "符合/不符合": "符合"
                    }

            # Step 6: 检查担保人评级
            guarantor_rating = "无"
            guarantor_name = "无"
            if self.guarantors:
                for guarantor in self.guarantors:
                    guar_code = guarantor.get("GUARCODE")
                    guarantor_name = guarantor.get("GUARNAME") or "无"
                    if guar_code:
                        rating_info = self._get_latest_credit_rating(guar_code)
                        if rating_info:
                            rating = rating_info.get("CREDITRATE") or "无"
                            if rating in high_ratings:
                                logger.debug(f"担保人 {guarantor_name} 评级 {rating} 符合标准")
                                desc_str = f"注册地{province}{city}, 预算收入{budget_income:.2f}{budget_unit}, 担保人{guarantor_name}评级{rating}"
                                return {
                                    "申请人指标说明": desc_str,
                                    "符合/不符合": "符合"
                                }
                            if guarantor_rating == "无":
                                guarantor_rating = rating

            # Step 7: 检查债项评级
            bond_rating = "无"
            bonds_list = self.bonds_info.get("bonds", [])

            if bonds_list:
                for bond in bonds_list:
                    bond_rating = bond.get("creditrate") or "无"
                    if bond_rating and bond_rating != "无":
                        if bond_rating in high_ratings:
                            logger.debug(f"债项评级 {bond_rating} 符合标准")
                            desc_str = f"注册地{province}{city}, 预算收入{budget_income:.2f}{budget_unit}, 债项评级{bond_rating}"
                            return {
                                "申请人指标说明": desc_str,
                                "符合/不符合": "符合"
                            }
                        break

            # Step 8: 如果都不符合，输出评级信息
            logger.debug("主体、担保人、债项评级均不符合标准")
            desc_str = f"注册地{province}{city}, 预算收入{budget_income:.2f}{budget_unit}, 主体评级{subject_rating_value}, 担保人{guarantor_name}评级{guarantor_rating}, 债项评级{bond_rating}"
            return {
                "申请人指标说明": desc_str,
                "符合/不符合": "不符合"
            }

        except Exception as e:
            logger.error(f"检查评级要求3失败: {str(e)}")
            return {
                "申请人指标说明": "",
                "符合/不符合": ""
            }

    def _get_rating_requirements4(self) -> Dict:
        """评级要求4：省级平台,发行人或担保人或债项评级（如有）不低于AA"""
        logger.info(f"获取【{self.company_name}】公司评级要求4")
        high_ratings = ['AAA', 'AAA-', 'AA+', 'AA']

        try:
            # Step 1: 获取平台信息
            platform_info = self._get_regifinaplat_by_itcode()

            if not platform_info or all(value is None or value == "" for value in platform_info.values()):
                logger.warning(f"公司 {self.company_name} 的融资平台信息所有字段均为None")
                return {
                    "申请人指标说明": "",
                    "符合/不符合": ""
                }

            region_level = platform_info.get("territorytype") or "无"
            province = platform_info.get("reglanname_p") or "无"
            city = platform_info.get("reglanname_c") or "无"

            # Step 2: 检查是否为省级平台
            if region_level != "省级":
                logger.debug(f"公司 {self.company_name} 为{region_level}平台，非省级")
                desc_str = f"注册地{province}{city}, 层级为{region_level}，非省级平台"
                return {
                    "申请人指标说明": desc_str,
                    "符合/不符合": "不符合"
                }

            # Step 3: 检查主体评级
            subject_rating = self._get_latest_credit_rating()
            subject_rating_value = "无"
            if subject_rating:
                subject_rating_value = subject_rating.get("CREDITRATE") or "无"
                if subject_rating_value in high_ratings:
                    logger.debug(f"主体评级 {subject_rating_value} 符合标准")
                    desc_str = f"注册地{province}{city}, 层级为{region_level}, 主体评级{subject_rating_value}"
                    return {
                        "申请人指标说明": desc_str,
                        "符合/不符合": "符合"
                    }

            # Step 4: 检查担保人评级
            guarantor_rating = "无"
            guarantor_name = "无"
            if self.guarantors:
                for guarantor in self.guarantors:
                    guar_code = guarantor.get("GUARCODE")
                    guarantor_name = guarantor.get("GUARNAME") or "无"
                    if guar_code:
                        rating_info = self._get_latest_credit_rating(guar_code)
                        if rating_info:
                            rating = rating_info.get("CREDITRATE") or "无"
                            if rating in high_ratings:
                                logger.debug(f"担保人 {guarantor_name} 评级 {rating} 符合标准")
                                desc_str = f"注册地{province}{city}, 层级为{region_level}, 担保人{guarantor_name}评级{rating}"
                                return {
                                    "申请人指标说明": desc_str,
                                    "符合/不符合": "符合"
                                }
                            if guarantor_rating == "无":
                                guarantor_rating = rating

            # Step 5: 检查债项评级
            bond_rating = "无"
            bonds_list = self.bonds_info.get("bonds", [])

            if bonds_list:
                for bond in bonds_list:
                    bond_rating = bond.get("creditrate") or "无"
                    if bond_rating and bond_rating != "无":
                        if bond_rating in high_ratings:
                            logger.debug(f"债项评级 {bond_rating} 符合标准")
                            desc_str = f"注册地{province}{city}, 层级为{region_level}, 债项评级{bond_rating}"
                            return {
                                "申请人指标说明": desc_str,
                                "符合/不符合": "符合"
                            }
                        break

            # Step 6: 如果都不符合，输出评级信息
            logger.debug("主体、担保人、债项评级均不符合标准")
            desc_str = f"注册地{province}{city}, 层级为{region_level}, 主体评级{subject_rating_value}, 担保人{guarantor_name}评级{guarantor_rating}, 债项评级{bond_rating}"
            return {
                "申请人指标说明": desc_str,
                "符合/不符合": "不符合"
            }

        except Exception as e:
            logger.error(f"检查评级要求4失败: {str(e)}")
            return {
                "申请人指标说明": "",
                "符合/不符合": ""
            }

    def _get_rating_requirements(self) -> Dict:
        """评级要求，依次检查评级要求1至4，满足其一即可，并指明是哪个要求"""
        try:
            # 按顺序检查评级要求
            for i, requirement_func in enumerate([
                self._get_rating_requirements1,
                self._get_rating_requirements2,
                self._get_rating_requirements3,
                self._get_rating_requirements4
            ], 1):
                result = requirement_func()
                if result.get("符合/不符合") == "符合":
                    logger.info(f"满足评级要求{i}: {result['申请人指标说明']}")
                    return {
                        f"评级要求{i}": {
                            "申请人指标说明": result["申请人指标说明"],
                            "符合/不符合": result["符合/不符合"]
                        }
                    }
            # 如果没有满足任何要求
            logger.info(f"公司 {self.company_name} 未满足任何评级要求")
            return {
                "评级要求": {
                    "申请人指标说明": "",
                    "符合/不符合": ""
                }
            }
        except Exception as e:
            logger.error(f"生成报告数据失败: {str(e)}")
            return {
                "评级要求": {
                    "申请人指标说明": "",
                    "符合/不符合": ""
                }
            }

    def _get_level_requirements(self) -> Dict:
        """层级要求：
        江苏省外，投资层级限于省级、地市级、市属区级、一般预算收入超过20亿元区县级。
        """
        logger.info(f"检查【{self.company_name}】层级要求")

        try:
            # Step 1: 获取平台信息
            platform_info = self._get_regifinaplat_by_itcode()

            if not platform_info or all(value is None or value == "" for value in platform_info.values()):
                logger.warning(f"公司 {self.company_name} 的融资平台信息所有字段均为None")
                return {
                    "申请人指标说明": "",
                    "符合/不符合": ""
                }

            province = platform_info.get("reglanname_p") or "无"
            region = platform_info.get("reglanname_c") or "无"
            region_level = platform_info.get("territorytype") or "无"
            finaffcode = platform_info.get("finaffcode", "")

            # Step 2: 检查是否为江苏省内平台
            if province == "江苏省":
                logger.info(f"公司 {self.company_name} 为江苏省内平台")
                return {
                    "申请人指标说明": "江苏省内平台",
                    "符合/不符合": "符合"
                }

            if region == "无":
                logger.warning(f"公司 {self.company_name} 无归属地信息")
                return {
                    "申请人指标说明": "",
                    "符合/不符合": ""
                }

            # Step 3: 检查层级
            if region_level in ["省级", "地市级", "省直辖市级"]:
                logger.info(f"公司 {self.company_name} 层级为{region_level}")
                return {
                    "申请人指标说明": f"层级为{region_level}",
                    "符合/不符合": "符合"
                }

            # Step 4: 查询一般公共预算收入（针对区县级）
            budget_income = 0.0
            budget_unit = "亿元"
            if finaffcode:
                budget_records = self._get_general_budget_income(int(finaffcode))
                if budget_records and budget_records[0].get("MVALUE", "N/A") != "N/A":
                    budget_income = float(budget_records[0].get("MVALUE"))
                    budget_unit = budget_records[0].get("DISPLAYCUNIT_VALUE", "亿元")

            logger.info(
                f"{self.company_name} 归属地属性: {region_level}, 预算收入 {budget_income} {budget_unit}")

            if budget_income >= 20:
                logger.info(
                    f"{self.company_name} 为区县级平台，预算收入 {budget_income} {budget_unit}，符合标准")
                return {
                    "申请人指标说明": f"区县级, 一般预算收入 {round(budget_income, 1)} {budget_unit}",
                    "符合/不符合": "符合"
                }

            logger.info(
                f"{self.company_name} 为区县级平台，预算收入 {budget_income} {budget_unit}，不足20亿元")
            return {
                "申请人指标说明": f"区县级, 一般预算收入 {round(budget_income, 1)} {budget_unit}",
                "符合/不符合": "不符合"
            }

        except Exception as e:
            logger.error(f"检查非江苏省平台标准失败: {str(e)}")
            return {
                "申请人指标说明": "",
                "符合/不符合": ""
            }

    def _get_subject_qualifications1(self) -> Dict:
        """检查发行人或担保人资产规模是否不低于100亿元"""
        logger.info(f"检查【{self.company_name}】资产规模要求")

        try:
            # Step 1: 获取发行人最新资产规模（单位：元，转换为亿元）及截止日期
            latest_balance = self._get_latest_balance_sheet()
            if not latest_balance:
                return {
                    "申请人指标说明": "",
                    "符合/不符合": ""
                }

            latest_balance = latest_balance[0]
            issuer_asset = float(latest_balance.get("TOTASSET", 0)) / 1e8
            deadline_date = latest_balance.get("ENDDATE") or "无"

            if issuer_asset >= 100:
                logger.info(
                    f"发行人 {self.company_name} 资产规模 {issuer_asset:.2f} 亿元，符合标准，截止日期 {deadline_date}")
                return {
                    "申请人指标说明": f"截止{deadline_date}，发行人资产规模{issuer_asset:.2f}亿元",
                    "符合/不符合": "符合"
                }

            # Step 2: 检查担保人资产规模
            if self.guarantors:
                for guarantor in self.guarantors:
                    guarantor_name = guarantor.get("GUARNAME") or "无"
                    guarantor_code = guarantor.get("GUARCODE")

                    if guarantor_code:
                        guarantor_balance = self._get_latest_balance_sheet(guarantor_code)
                        if not guarantor_balance:
                            continue

                        guarantor_balance = guarantor_balance[0]
                        guarantor_asset = float(guarantor_balance.get("TOTASSET", 0)) / 1e8
                        guarantor_deadline_date = guarantor_balance.get("ENDDATE") or "无"

                        if guarantor_asset >= 100:
                            logger.info(
                                f"担保人 {guarantor_name} 资产规模 {guarantor_asset:.2f} 亿元，符合标准，截止日期 {guarantor_deadline_date}")
                            return {
                                "申请人指标说明": f"截止{guarantor_deadline_date}，担保人{guarantor_name}资产规模{guarantor_asset:.2f}亿元",
                                "符合/不符合": "符合"
                            }

            # 如果发行人和担保人均不符合
            logger.info(
                f"发行人资产规模 {issuer_asset:.2f} 亿元，所有担保人资产规模均不足100亿元，截止日期 {deadline_date}")
            return {
                "申请人指标说明": f"截止{deadline_date}，发行人资产规模{issuer_asset:.2f}亿元，所有担保人资产规模均不足100亿元",
                "符合/不符合": "不符合"
            }

        except Exception as e:
            logger.error(f"检查资产规模标准失败: {str(e)}")
            return {
                "申请人指标说明": "",
                "符合/不符合": ""
            }

    def _get_subject_qualifications2(self) -> Dict:
        """检查发行人及担保人资产负债率是否不高于80%"""
        logger.info(f"检查【{self.company_name}】资产负债率要求")

        try:
            # Step 1: 获取发行人最新资产负债率及截止日期
            latest_indic = self._get_latest_indicdata()
            if not latest_indic:
                return {
                    "申请人指标说明": "",
                    "符合/不符合": ""
                }

            latest_indic = latest_indic[0]
            issuer_liab_ratio = float(latest_indic.get("ASSLIABRT", 0.0))
            deadline_date = latest_indic.get("ENDDATE") or "无"

            issuer_meets = issuer_liab_ratio <= 80
            if issuer_meets:
                logger.info(
                    f"发行人 {self.company_name} 资产负债率 {issuer_liab_ratio:.2f}%，符合标准，截止日期 {deadline_date}")
            else:
                logger.info(
                    f"发行人 {self.company_name} 资产负债率 {issuer_liab_ratio:.2f}%，超过80%，截止日期 {deadline_date}")

            # Step 2: 检查担保人资产负债率
            guarantor_results = []
            all_guarantors_meet = True

            if self.guarantors:
                for guarantor in self.guarantors:
                    guarantor_name = guarantor.get("GUARNAME") or "无"
                    guarantor_code = guarantor.get("GUARCODE")

                    if guarantor_code:
                        guarantor_indic = self._get_latest_indicdata(guarantor_code)
                        if not guarantor_indic:
                            # 没有信息，跳过
                            continue

                        guarantor_indic = guarantor_indic[0]
                        guarantor_liab_ratio = float(guarantor_indic.get("ASSLIABRT", 0.0))
                        guarantor_deadline_date = guarantor_indic.get("ENDDATE") or "无"

                        if guarantor_liab_ratio <= 80:
                            logger.info(
                                f"担保人 {guarantor_name} 资产负债率 {guarantor_liab_ratio:.2f}%，符合标准，截止日期 {guarantor_deadline_date}")
                        else:
                            logger.info(
                                f"担保人 {guarantor_name} 资产负债率 {guarantor_liab_ratio:.2f}%，超过80%，截止日期 {guarantor_deadline_date}")
                            all_guarantors_meet = False

                        guarantor_results.append(
                            f"截至{guarantor_deadline_date}，担保人{guarantor_name}资产负债率{guarantor_liab_ratio:.2f}%")
            else:
                logger.info(f"无担保人数据，跳过担保人资产负债率检查")

            # 判断是否符合要求：发行人符合且所有担保人符合（无担保人时视为符合）
            explanation = f"截至{deadline_date}，发行人资产负债率{issuer_liab_ratio:.2f}%"
            if guarantor_results:
                explanation += f"，{', '.join(guarantor_results)}"

            if issuer_meets and all_guarantors_meet:
                return {
                    "申请人指标说明": explanation,
                    "符合/不符合": "符合"
                }

            logger.info(f"发行人或担保人资产负债率不符合标准")
            return {
                "申请人指标说明": explanation,
                "符合/不符合": "不符合"
            }

        except Exception as e:
            logger.error(f"检查资产负债率标准失败: {str(e)}")
            return {
                "申请人指标说明": "",
                "符合/不符合": ""
            }

    def _get_same_affiliation_platforms(self) -> List[Dict]:
        """
        获取与当前公司同一融资归属地的所有平台信息
        """
        try:
            with Session(engine) as session:
                platforms = get_same_affiliation_platforms(self.company_code, session)
                if not platforms:
                    logger.warning(f"未找到公司 {self.company_name} 同一归属地的平台信息")
                    return []

                # 转换为字典列表
                result = []
                for platform in platforms:
                    result.append({
                        "ITCODE": platform.ITCODE,
                        "ITNAME": platform.ITNAME,
                        "FINAFFCODE": platform.FINAFFCODE,
                        "FINAFFNAME": platform.FINAFFNAME,
                        "TERRITORYTYPE": platform.TERRITORYTYPE,
                        "REGLANNAME_P": platform.REGLANNAME_P,
                        "REGLANNAME_C": platform.REGLANNAME_C,
                        "BONDBALANCE": platform.BONDBALANCE
                    })

                logger.info(f"查询到 {len(result)} 个同一归属地的平台")
                return result

        except Exception as e:
            logger.error(f"获取同一归属地平台信息失败: {str(e)}")
            return []

    def _get_subject_qualifications3(self) -> Dict:
        """
        检查主体资质3：如发行人及担保人均为市属区级、县级平台，
        且主体评级均为AA级的，省外发行人或担保人须在本级财政所属区域内净资产规模排名前五，
        前五包括AA及以上平台；省内不限。
        """
        high_ratings = ['AAA', 'AAA-', 'AA+', 'AA']
        above_levels = ["地市级", "省直辖市级", "县市级"]

        try:
            # Step 1: 获取平台信息
            platform_info = self._get_regifinaplat_by_itcode()

            if not platform_info or all(v is None or v == "" for v in platform_info.values()):
                logger.warning(f"未找到公司 {self.company_name} 的融资平台信息")
                return {
                    "申请人指标说明": "",
                    "符合/不符合": ""
                }

            issuer_region_level = platform_info.get("territorytype") or "无"
            province = platform_info.get("reglanname_p") or "无"
            city = platform_info.get("reglanname_c") or "无"
            is_jiangsu = province == "江苏省"
            issuer_region_name = platform_info.get("finaffname") or "无"

            # Step 2: 检查是否为江苏省内平台
            if is_jiangsu:
                logger.debug(f"公司 {self.company_name} 为江苏省内平台，直接符合")
                return {
                    "申请人指标说明": f"江苏省内平台，所属区域{issuer_region_name}",
                    "符合/不符合": "符合"
                }

            # Step 3: 检查是否为市属区级或县级以上平台
            if issuer_region_level not in above_levels:
                logger.debug(f"省外发行人平台类型为{issuer_region_level}，非市属区级、县级平台以上的")
                return {
                    "申请人指标说明": f"注册地{province}{city}，平台类型为{issuer_region_level}，非市属区级、县级平台以上的",
                    "符合/不符合": "不符合"
                }

            # Step 4: 检查主体评级
            issuer_qualify = False
            subject_rating = self._get_latest_credit_rating()
            subject_rating_value = "无"

            if subject_rating:
                subject_rating_value = subject_rating.get("CREDITRATE") or "无"
                issuer_qualify = subject_rating_value in high_ratings

            issuer_explanation = f"发行人评级为{subject_rating_value}，所属区域{issuer_region_name}"

            # Step 5: 检查担保人评级和平台层级
            all_guar_qualify = True
            guar_explanation = ""

            if self.guarantors:
                guar_explanation = "，担保人"
                for guarantor in self.guarantors:
                    guar_name = guarantor.get("GUARNAME") or "无"
                    guar_code = guarantor.get("GUARCODE")

                    if guar_code:
                        guar_platform_info = self._get_regifinaplat_by_itcode(guar_code)
                        if not guar_platform_info or all(v is None or v == "" for v in guar_platform_info.values()):
                            all_guar_qualify = False
                            break

                        guar_region_level = guar_platform_info.get("territorytype") or "无"
                        guar_region_name = guar_platform_info.get("finaffname") or "无"

                        if guar_region_level not in above_levels:
                            all_guar_qualify = False
                            break

                        guar_rating_info = self._get_latest_credit_rating(guar_code)
                        guar_rating = guar_rating_info.get("CREDITRATE") or "无" if guar_rating_info else "无"

                        if guar_rating not in high_ratings:
                            all_guar_qualify = False
                            break

                        guar_explanation += f"{guar_name}评级为{guar_rating}，所属区域{guar_region_name}"
            else:
                guar_explanation = "，无担保人"

            # Step 6: 检查是否满足评级和层级要求
            if issuer_qualify and all_guar_qualify:
                logger.debug(f"省外发行人及担保人均市属区级/县级平台以上的且评级>=AA级，符合标准")
                return {
                    "申请人指标说明": issuer_explanation + guar_explanation,
                    "符合/不符合": "符合"
                }

            # Step 7: 检查区域内净资产排名
            region_platforms = self._get_same_affiliation_platforms()

            if not region_platforms:
                logger.warning(f"未找到公司 {self.company_name} 同归属地的平台信息")
                return {
                    "申请人指标说明": issuer_explanation + guar_explanation + "，无法获取区域平台信息",
                    "符合/不符合": "不符合"
                }

            # 统计AA及以上评级平台的净资产
            net_assets = []

            for p in region_platforms:
                p_code = p.get("ITCODE", "")
                if not p_code:
                    continue

                # 获取平台评级
                p_rating_info = self._get_latest_credit_rating(p_code)
                p_rating = p_rating_info.get("CREDITRATE") or "无" if p_rating_info else "无"

                # 只统计AA及以上评级的平台
                if p_rating in high_ratings:
                    balance = self._get_latest_balance_sheet(p_code)
                    if balance and len(balance) > 0:
                        net_asset_value = float(balance[0].get("RIGHAGGR", 0)) / 1e8
                        net_assets.append((p_code, net_asset_value))

            # 按净资产降序排序
            net_assets.sort(key=lambda x: x[1], reverse=True)

            logger.debug(f"同归属地内AA及以上平台共{len(net_assets)}个")

            # 检查发行人排名
            issuer_rank = next((i + 1 for i, (code, _) in enumerate(net_assets[:5]) if code == self.company_code), None)
            if issuer_rank:
                logger.debug(f"省外发行人净资产排名第{issuer_rank}")
                return {
                    "申请人指标说明": issuer_explanation + guar_explanation + f"，发行人净资产排名第{issuer_rank}",
                    "符合/不符合": "符合"
                }

            # 检查担保人排名
            if self.guarantors:
                for guar in self.guarantors:
                    guar_code = guar.get("GUARCODE")
                    if guar_code:
                        guar_rank = next((i + 1 for i, (code, _) in enumerate(net_assets[:5]) if code == guar_code),
                                         None)
                        if guar_rank:
                            guar_name = guar.get("GUARNAME") or "无"
                            logger.debug(f"担保人{guar_name}净资产排名第{guar_rank}")
                            return {
                                "申请人指标说明": issuer_explanation + guar_explanation + f"，担保人净资产排名第{guar_rank}",
                                "符合/不符合": "符合"
                            }

            # Step 8: 如果都不符合
            logger.debug(f"省外发行人或担保人净资产未进入区域内AA及以上平台前五")
            return {
                "申请人指标说明": issuer_explanation + guar_explanation + "，发行人或担保人净资产未进入区域内AA及以上平台前五",
                "符合/不符合": "不符合"
            }

        except Exception as e:
            logger.error(f"检查主体资质3失败: {str(e)}")
            return {
                "申请人指标说明": "",
                "符合/不符合": ""
            }

    def _get_restricted_regions(self) -> Dict:
        """检查禁入区域准入状态"""
        try:
            # 获取平台信息
            platform_info = self._get_regifinaplat_by_itcode()

            if not platform_info or all(v is None or v == "" for v in platform_info.values()):
                logger.warning(f"未找到公司 {self.company_name} 的融资平台信息")
                return {
                    "申请人指标说明": "",
                    "符合/不符合": ""
                }

            region_name = platform_info.get("reglanname_p") or "无"

            messages = [{
                "role": "user",
                "content": f"""/no_think
                                你是金融文档分析专家，请根据以下规则判断注册地的准入状态：
                                规则：
                                1. 江苏省内直接允许准入，返回"符合"。
                                2. 江苏省外，天津市、辽宁省、吉林省、黑龙江省、海南省、贵州省、云南省、甘肃省、青海省、内蒙古自治区、广西壮族自治区、西藏自治区、宁夏回族自治区、新疆维吾尔自治区不予准入，返回"不符合"。
                                3. 重庆市审慎控制业务余额，返回"审慎控制业务余额"。
                                4. 不在上述区域的其他地区允许准入，返回"符合"。
                                5. 如果注册地信息缺失或无法判断，返回空值。
                                注册地: {region_name}
                                【结果输出】
                                返回JSON格式：
                                {{"申请人指标说明":"注册地为{region_name}","符合/不符合":"符合" 或 "不符合" 或 "审慎控制业务余额"}}
                                如果无法判断，返回：
                                {{"申请人指标说明":"","符合/不符合":""}}
                                """
            }]
            logger.debug(messages)
            result = self.llm.generate_stream_complete(messages)
            return json.loads(result)

        except Exception as e:
            logger.error(f"检查禁入区域失败: {str(e)}")
            return {
                "申请人指标说明": "",
                "符合/不符合": ""
            }

    def _get_rating_restrictions(self) -> Dict:
        """检查其他禁入条件1：主体及担保人评级和债项评级（如有）近一年未被下调评级，且展望不得为负面"""
        logger.info(f"获取【{self.company_name}】评级禁入条件")
        qualified = True
        explanation = []

        try:
            # 获取主体评级信息
            main_credit = ""
            main_outlook_desc = ""
            if self.credit_ratings:
                sorted_ratings = sorted(
                    self.credit_ratings,
                    key=lambda x: x.get("PUBLISHDATE") or "0000-00-00",
                    reverse=True
                )
                latest_rating = sorted_ratings[0]
                main_credit = latest_rating.get("CREDITRATE", "")
                main_outlook = latest_rating.get("EXPTRATING", "")
                main_outlook_desc = latest_rating.get("EXPTRATING_value", "")
                # 检查主体展望
                if main_outlook in ['3', '5']:
                    qualified = False
                    explanation.append(f"主体评级展望为{main_outlook_desc}")

            # 获取担保人评级信息
            guar_credit = ""
            guar_outlook_desc = ""
            if self.guarantors and qualified:
                guar_explanations = []
                for guarantor in self.guarantors:
                    guar_name = guarantor.get('GUARNAME', '')  # 字段名为GUARNAME
                    guar_code = guarantor.get('GUARCODE')

                    if guar_code:
                        # 主动查询担保人评级
                        guar_rating_info = self._get_latest_credit_rating(guar_code)
                        if guar_rating_info:
                            guar_credit = guar_rating_info.get("CREDITRATE", "")
                            guar_outlook = guar_rating_info.get("EXPTRATING", "")
                            guar_outlook_desc = guar_rating_info.get("EXPTRATING_value", "")
                            # 检查担保人展望
                            if guar_outlook in ['3', '5']:
                                qualified = False
                                guar_explanations.append(f"担保人{guar_name}评级展望为{guar_outlook_desc}")
                if guar_explanations:
                    explanation.append("; ".join(guar_explanations))

            # 获取债项评级信息
            bond_credit = ""
            bond_outlook_desc = ""
            bonds_list = self.bonds_info.get("bonds", [])

            if bonds_list and qualified:
                bond_explanations = []
                sorted_bonds = sorted(
                    bonds_list,
                    key=lambda x: x.get("rating_publishdate") or "0000-00-00",
                    reverse=True
                )
                for bond in sorted_bonds:
                    secode = bond.get("secode")
                    bond_credit = bond.get("creditrate", "")
                    bond_outlook = bond.get("exptrating", "")
                    bond_outlook_desc = bond.get("exptrating", "")  # 展望描述
                    # 检查债项评级下调
                    bond_downgrade = bond.get("radjustdir", "") == '3'
                    if bond_downgrade:
                        qualified = False
                        bond_explanations.append(f"债项{secode}评级近一年内被下调")
                    # 检查债项展望
                    if bond_outlook in ['3', '5']:
                        qualified = False
                        bond_explanations.append(f"债项{secode}评级展望为{bond_outlook_desc}")
                    # 仅保存第一个非空债项信息
                    if bond_credit and not bond_explanations:
                        break
                if bond_explanations:
                    explanation.append("; ".join(bond_explanations))

            # 如果没有找到债项评级，获取第一个非空评级
            if not bond_credit and bonds_list:
                for bond in bonds_list:
                    bond_credit = bond.get("creditrate", "")
                    if bond_credit:
                        break

            # 构造返回说明
            result_explanation = ""
            if not main_credit and not guar_credit and not bond_credit:
                return {
                    "申请人指标说明": "",
                    "符合/不符合": ""
                }
            elif qualified:
                result_explanation = f"主体评级{main_credit or '无'}（展望{main_outlook_desc or '无'}），担保人评级{guar_credit or '无'}（展望{guar_outlook_desc or '无'}），债项评级{bond_credit or '无'}（展望{bond_outlook_desc or '无'}），近一年评级未下调且展望非负面"
            else:
                result_explanation = "; ".join(explanation)

            logger.info(f"公司{self.company_name}评级检查: {result_explanation}")
            return {
                "申请人指标说明": result_explanation,
                "符合/不符合": "符合" if qualified else "不符合"
            }

        except Exception as e:
            logger.error(f"检查评级禁入条件失败: {str(e)}")
            return {
                "申请人指标说明": "",
                "符合/不符合": ""
            }

    def _get_company_info(self) -> Dict[str, Any]:
        """获取公司基本信息"""
        try:
            logger.info(f"获取【{self.company_name}】公司基本信息")

            with Session(engine) as session:
                company_info = get_company_info(self.company_code, session)
                result = company_info.model_dump(mode='json')
                result['unified_credit_code'] = self.unified_credit_code
                return result
        except Exception as e:
            logger.error(f"获取公司信息失败: {e}")
            return {}

    def _get_platform_scores(self, company_code: str = None) -> List[Dict[str, Any]]:
        """获取区域排名信息"""
        try:
            logger.info(f"获取【{self.company_name}】区域排名信息")

            with Session(engine) as session:
                scores = get_platform_score_by_company_code(company_code or self.company_code, session)
                return [score.model_dump(mode='json') for score in scores] if scores else []
        except Exception as e:
            logger.error(f"获取区域排名信息失败: {e}")
            return []

    def _get_shareholders(self, company_code: str = None) -> List[Dict[str, Any]]:
        """获取最新股东名单信息"""
        try:
            logger.info(f"获取【{self.company_name}】股东信息")

            with Session(engine) as session:
                shareholders = get_latest_shareholders(company_code or self.company_code, session)
                return [shareholder.model_dump(mode='json') for shareholder in shareholders] if shareholders else []
        except Exception as e:
            logger.error(f"获取股东信息失败: {e}")
            return []

    def _get_bond_spreads(self, company_code: str = None) -> List[Dict[str, Any]]:
        """获取利差信息列表"""
        try:
            logger.info(f"获取【{self.company_name}】利差信息")

            with Session(engine) as session:
                spreads = get_plat_spreads(company_code or self.company_code, session)
                return [spread.model_dump(mode='json') for spread in spreads] if spreads else []
        except Exception as e:
            logger.error(f"获取利差信息失败: {e}")
            return []

    def _get_bond_registration_info(self, company_name: str = None) -> List[Dict[str, Any]]:
        """获取公司债券注册批复全景信息"""
        try:
            logger.info(f"获取【{company_name or self.company_name}】债券注册批复信息")

            with Session(engine) as session:
                registrations = get_bond_registration_analysis(company_name or self.company_name, session)
                return registrations if registrations else []
        except Exception as e:
            logger.error(f"获取债券注册批复信息失败: {e}")
            return []

    def _get_general_budget_income(self, regioncode: int = None) -> List[Dict[str, Any]]:
        """获取一般公共预算收入经济数据"""
        logger.info(f"获取【{self.company_name}】一般公共预算收入经济数据")
        try:
            with Session(engine) as session:
                # 如果提供了regioncode，使用get_latest_regifin
                if regioncode is not None:
                    results = get_latest_regifin(regioncode, session)
                else:
                    # 否则使用公司代码查询
                    results = get_latest_regifin_by_company(self.company_code, session)

                return [result.model_dump(mode='json') for result in results] if results else []
        except Exception as e:
            logger.error(f"获取一般公共预算收入经济数据失败: {e}")
            return []

    def _get_latest_balance_sheet(self, company_code: str = None) -> List[Dict[str, Any]]:
        """获取最新一般企业资产负债表数据"""
        logger.info(f"获取【{self.company_name}】最新一般企业资产负债表数据")
        try:
            with Session(engine) as session:
                results = get_latest_prgbalsheetnew(company_code or self.company_code, session)
                return [result.model_dump(mode='json') for result in results] if results else []
        except Exception as e:
            logger.error(f"获取最新一般企业资产负债表数据失败: {e}")
            return []

    def _get_latest_indicdata(self, company_code: str = None) -> List[Dict[str, Any]]:
        """获取最新资产负债率"""
        logger.info(f"获取【{self.company_name}】最新资产负债率")
        try:
            with Session(engine) as session:
                results = get_latest_prgindicdata(company_code or self.company_code, session)
                return [result.model_dump(mode='json') for result in results] if results else []
        except Exception as e:
            logger.error(f"获取最新资产负债率失败: {e}")
            return []



    def get_report_data(self) -> Dict[str, Any]:
        """生成完整的报告数据"""
        logger.info(f"开始生成【{self.company_name}】的报告数据")

        data = {
            "评级要求": self._get_rating_requirements(),
            "层级要求": self._get_level_requirements(),
            "主体资质1": self._get_subject_qualifications1(),
            "主体资质2": self._get_subject_qualifications2(),
            "主体资质3": self._get_subject_qualifications3(),
            "禁入区域": self._get_restricted_regions(),
            "其他禁入条件": self._get_rating_restrictions(),
            "利差信息": self._get_bond_spreads(),
            "公司基本信息": self._get_company_info(),
            "股东信息": self._get_shareholders(),
            "主体评级列表": self.credit_ratings,
            "区域排名信息": self._get_platform_scores(),
            "注册批复全景": self._get_bond_registration_info()
        }

        return self._process_values(data)

    @staticmethod
    def _process_values(obj: Any) -> Any:
        """递归处理数据：替换None为""，转换Decimal为float"""
        if obj is None:
            return ""
        elif isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, dict):
            return {k: YJTCompanyDataFetcher._process_values(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [YJTCompanyDataFetcher._process_values(item) for item in obj]
        else:
            return obj


def main(company_name: str = None):
    """主函数：测试数据抓取功能"""
    try:
        fetcher = YJTCompanyDataFetcher(company_name=company_name)
        report = fetcher.get_report_data()
        print(parse_json_response(report))
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        raise


if __name__ == "__main__":
    main(company_name="吉安市工业投资有限公司")
