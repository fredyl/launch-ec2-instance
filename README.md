if len(headers) != len(rows[0]):
        raise ValueError(f"The number of headers ({len(headers)}) does not match the number of values in the first row ({len(rows[0])}).")
    
