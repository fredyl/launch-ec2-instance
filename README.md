def replace_null_values_df(data_key):
    if isinstance (data_key, dict):
        return {k: replace_null_values_df(v) if v is not None else " " for k, v in data_key.items()}
    elif isinstance (data_key, list):
        return [replace_null_values_df(item) if item is not None else " " for item in data_key]
    else:
        return data_key if data_key is not None else " "
