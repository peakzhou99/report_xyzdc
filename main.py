import sys
import os
import json
import time

from utils.file_util import convert_com_name
from data.db_data.ctz_data_download import DataDownload
from data.doc_data.organize_doc import OrganizeDoc
from generate.generate_report import UrbanReport
from utils.log_utils import get_logger

logger = get_logger()

def main(task_dir):
    logger.info(f"xin yong zhai diao cha report process start.source path: {task_dir}")
    start_time  = time.time()
    request_file = os.path.join(task_dir, "request.json")
    response = {"resCode": 1,"resMsg": "succeed","resFiles": []}

    # 参数校验
    with open(request_file,"r") as f:
        request = json.load(f)
        if request is None or request.get("custName") is None:
            response["resCode"]=0
            response["resMsg"]="文件request.json中客户名称为空"


    # 生成报告
    try:
        # 1. 文件转换 doc-> docx | docx-> pdf
        od = OrganizeDoc(task_dir)
        company_name = od.organize()
        company_name = request.get("custName") or company_name

        # 2. 数据库数据下载
        dd = DataDownload(task_dir)
        dd.extract_data(convert_com_name(company_name))
        # 3. 报告生成
        report = UrbanReport(request, company_name, task_dir)
        output_file = report.gen_report()
        # 处理结果
        report_name = os.path.basename(output_file)
        response["resFiles"] = [report_name]
    except Exception as e:
        logger.exception(e)
        response["resCode"] = 0
        response["resMsg"] = f"文件生成失败！{str(e)}"
    finally:
        logger.info(f"xin yong zhai diao cha report process end.cost time: {time.time() - start_time}")

    # 结果处理
    response_file = os.path.join(task_dir, "response.json")
    with open(response_file, "w") as f:
        json.dump(response, f, ensure_ascii=False)

if __name__ == '__main__':
    task_dir = sys.argv[1]
    main(task_dir)