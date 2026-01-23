import cpca

def get_region_pca(region):
    """区域转为省市区"""
    _region = [region]
    _area_df = cpca.transform(_region)
    _area_df['区'] = _area_df.apply(lambda r: r['地址'] if r['区'] is None and r['地址'] is not None and r['地址'] !='' else r['区'],axis=1)
    _area_df['地址'] = _region
    _area_df = _area_df.map(lambda e: None if e in ['市辖区','县'] else e)
    area_dict_list = _area_df[['地址', '省', '市', '区']].to_dict(orient='records')
    if isinstance(area_dict_list,list) and len(area_dict_list) > 0:
        return area_dict_list[0]


def get_min_region(region):
    area_dict = get_region_pca(region)
    if area_dict is None:
        return
    elif area_dict.get("区") is not None:
        return area_dict.get("区")
    elif area_dict.get("市") is not None:
        return area_dict.get("市")
    elif area_dict.get("省") is not None:
        return area_dict.get("省")

