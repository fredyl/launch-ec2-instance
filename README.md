def replace_null_values(item):
    return [
        {key: (value if value is not None else "") for key, value in item.items()}
        for item in item
    ]
