def replace_null_values(data_key, default_int=0, default_str="", default_list=[]):
    """
    Replaces None with type-appropriate defaults based on context.
    """
    if isinstance(data_key, dict):
        return {k: replace_null_values(v, default_int, default_str, default_list) for k, v in data_key.items()}
    elif isinstance(data_key, list):
        return [replace_null_values(item, default_int, default_str, default_list) for item in data_key]
    elif isinstance(data_key, int):
        return data_key if data_key is not None else default_int
    elif isinstance(data_key, str):
        return data_key if data_key is not None else default_str
    elif isinstance(data_key, list):
        return data_key if data_key is not None else default_list
    else:
        return data_key if data_key is not None else None 
