 Some of types cannot be determined after inferring.


def replace_null_values(data_key):
    #Replacing Null Values with empty string
    if isinstance (data_key, dict):
        return {k: replace_null_values(v) if v is not None else None for k, v in data_key.items()}
    elif isinstance (data_key, list):
        return [replace_null_values(item) if item is not None else None for item in data_key]
    else:
        return data_key if data_key is not None else None
