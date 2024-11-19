def replace_null_values(item_list):
    return [{k: (v if v not in["null", None] else "") for k, v in item.items()} for item in item_list]
