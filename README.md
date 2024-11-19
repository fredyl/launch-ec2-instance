def clean_null_values(data):
    if isinstance(data, dict):
        # Process each key-value pair in the dictionary
        return {k: clean_null_values(v) if v is not None and v != "" else None for k, v in data.items()}
    elif isinstance(data, list):
        # Process each item in the list
        return [clean_null_values(item) for item in data]
    else:
        # Return the value as-is, unless it is None or empty string
        return data if data is not None and data != "" else None
