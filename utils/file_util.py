import os
import subprocess

import shutil
import pandas as pd
import re


def mkdir(dir):
    """创建目录"""
    if not os.path.exists(dir):
        os.makedirs(dir)


def rmdir(dir):
    if os.path.exists(dir):
        shutil.rmtree(dir)
        print("目录/文件删除成功")


def copy(src_path, dest_path):
    try:
        desc_dir = os.path.dirname(dest_path)
        if not os.path.exists(desc_dir):
            os.makedirs(desc_dir)
        shutil.copy(src_path, dest_path)
        print("文件拷贝成功")
    except FileNotFoundError:
        print("文件不存在，请检查路径")
    except PermissionError:
        print("没有权限复制文件")
    except Exception as e:
        print("发生错误：", e)


def convert_file(input_path, output_format, output_dir=None):
    if not os.path.isfile(input_path):
        raise FileNotFoundError(f"输入文件不存在: {input_path}")

    if output_dir is None:
        output_dir = os.path.dirname(input_path)
    else:
        os.makedirs(output_dir, exist_ok=True)

    command = [
        'libreoffice25.2',
        '--headless',         # 无界面模式
        '--convert-to', output_format,
        '--outdir', output_dir,
        input_path
    ]

    try:
        print(" ".join(command))
        subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output_filename = os.path.splitext(os.path.basename(input_path))[0] + '.' + output_format
        output_path = os.path.join(output_dir, output_filename)
        return output_path
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"转换失败: {e.stderr.decode()}") from e


def convert_doc_to_docx(doc_path, docx_path):
    convert_file(doc_path, "docx", os.path.dirname(docx_path))


def rename(dir, old_file, new_file):
    old_file_path = os.path.join(dir, old_file)
    new_file_path = os.path.join(dir, new_file)
    if os.path.isfile(old_file_path) and not os.path.exists(new_file_path):
        os.rename(old_file_path, new_file_path)


def find_file_path(dir, file_name):
    if os.path.isfile(dir):
        return
    for root, dirs, files in os.walk(dir):
        for file in files:
            if re.search(rf"{file_name}", file) and '~$' not in file:
                return (root, file)


def find_path_by_name(dir, file_name):
    if os.path.isfile(dir):
        return
    for root, dirs, files in os.walk(dir):
        for file in files:
            if re.search(rf"{file_name}", file) and '~$' not in file:
                return os.path.join(root, file)


def convert_com_name(name):
    if "（" in name or "）" in name:
        return name.replace("（", "(").replace("）", ")")
    return name

