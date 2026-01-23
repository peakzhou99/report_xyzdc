import time
import os
import logging
import json

from data.doc_data.organize_doc import OrganizeDoc
from data.db_data.ctz_data_download import DataDownload
from generate.generate_report import UrbanReport
from utils.file_util import convert_com_name, rmdir
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

executor = ThreadPoolExecutor(max_workers=1)

def run(source_path):
    try:
        with open(os.path.join(source_path, "request.json")) as f:
            req = f.read()
        req_json = json.loads(req)
        od = OrganizeDoc(source_path)
        com = od.organize()
        com = req_json.get("custName") or com
        print(f"com: {com}")
        external_data_path = os.path.join(source_path, "external_data")
        # rmdir(external_data_path)
        # DataDownload(source_path).extract_data(convert_com_name(com))

        # com = "淮安市淮阴区城市资产经营有限公司"
        report = UrbanReport(req_json, com, source_path)
        report.gen_report()
    except Exception as e:
        logging.exception(e)

def get_ctz_file_lists():
    res = []
    custName_list = []
    files_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "files")
    date_list = os.listdir(files_path)
    date_list.sort()
    # print(date_list)
    if not date_list:
        return
    for date_dir in date_list:
        date_path = os.path.join(files_path, date_dir)
        files_list = os.listdir(date_path)
        files_list.sort()
        for file_dir in files_list:
            file_path = os.path.join(date_path, file_dir)
            for root, dirs, files in os.walk(file_path):
                if "response.json" in files:
                    with open(os.path.join(root, "response.json"), "r") as f:
                        data = f.read()
                    try:
                        data = json.loads(data)
                    except:
                        print("error", root)
                        continue
                    if "succeed" != data.get("resMsg", None):
                        continue
                    with open(os.path.join(root, "request.json"), "r") as f:
                        request_data = f.read()
                    request_data = json.loads(request_data)
                    cust_name = request_data.get("custName")
                    if cust_name and cust_name in custName_list:
                        continue
                    if request_data.get("reportType") == "05":
                        res.append(root)
    return res

if __name__ == "__main__":
    # 城投债
    run("/home/datagov/INSTANCE/projects/report_xyzdc/files/test1") # 都江堰兴市集团有限责任公司
    try:
        print(1)

    finally:
        print("end.................")
        executor.shutdown(wait=True)