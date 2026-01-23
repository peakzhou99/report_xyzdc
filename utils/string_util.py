import re
from datetime import datetime,timedelta


def exists_keyword(string,keywords):
    if any(re.search(rf"{keyword}",string) for keyword in keywords):
        return True
    else:
        return False


def uniform_date(str_date,date_format):
    try:
        if "年末" in str_date:
            return datetime.strptime(str_date,'%Y年末').replace(month=12).strftime(date_format)
        elif "年度" in str_date:
            return datetime.strptime(str_date,'%Y年度').replace(month=12).strftime(date_format)
        elif re.search(r"\d{4}年\d{1,2}月", str_date):
            return datetime.strptime(str_date, '%Y年%m月').strftime(date_format)
        elif re.search(r"\d{4}年\d{1,2}-\d{1,2}月", str_date):
            date_arr = re.split(r"年|-", str_date)
            date_arr.pop(1)
            return datetime.strptime("年".join(date_arr), '%Y年%m月').strftime(date_format)
        elif re.search(r"(\d{4}-\d{1,2})", str_date):
            dt = datetime.strptime(re.search(r"(\d{4}-\d{1,2})", str_date).group(0), '%Y-%m')
            if date_format == "%Y年%m月":
                return f"{dt.year}年{dt.month}月"
            return dt.strftime(date_format)
        elif re.search(r"(\d{4}\d{1,2})", str_date):
            dt = datetime.strptime(re.search(r"(\d{4}\d{1,2})", str_date).group(0), '%Y%m')
            if date_format == "%Y年%m月":
                return f"{dt.year}年{dt.month}月末"
            return dt.strftime(date_format)
        elif re.search(r"\d{4}",str_date):
            return datetime.strptime(re.search(r"\d{4}", str_date).group(0), '%Y').strftime(date_format)
    except Exception:
        return None


def custom_date(str_date):
    _str_date = uniform_date(str_date,"%Y%m")
    if _str_date is None:
        return "目前"
    elif _str_date.endswith("12"):
        return uniform_date(str_date, "%Y年末")
    else:
        return uniform_date(str_date, "%Y年%m月")

def extract_date(text):
    pattern = r"(\d{4}\s*年度)|(\d{4}\s*年末)|(\d{4}\s*年\s*\d{1,2}\s*月)|(\d{4}\s*年\s*\d{1,2}\s*-\s*\d{1,2}\s*月)|(\d{4}-\d{1,2})|(\d{4}\d{2})"
    match = re.findall(pattern, text)
    if match:
        date_res = []
        for date_list in re.findall(pattern, text):
            for d in date_list:
                if d and uniform_date(d,'%Y%m'):
                    date_res.append(uniform_date(d,'%Y%m'))
        if date_res:
            return max(date_res)
    elif re.search(r"\d{4}年$", text):
        return uniform_date(re.search(r"\d{4}年$", text).group(0), '%Y12')
    else:
        return None

def extract_unit(text):
    pattern = r"(万元、亩|亿元、亩|亿元、%|万元|亿元|单位：人|次/年|亿|万|元)"
    # match = re.search(pattern, text)
    match = re.findall(pattern, text)
    if match:
        if "人" in match[-1]:
            return "人"
        else:
            return match[-1]
    else:
        return None


def convert_val(txt):
    res = re.findall(r"[\d,.]*万元", txt)
    for val_txt in res:
        val = val_txt.strip("万元")
        val = float(val.replace(",",""))
        conv = round(val / 10000, 2)
        txt = txt.replace(val_txt, str(conv)+"亿元", 1)
    return txt
