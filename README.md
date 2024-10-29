# Check if data is available for the given type and key before generating URLs
def check_data_availability(token, data_type, code_key, code_value):
    endpoint = f"{data_type}?{code_key}={code_value}"
    _, response_data = get_holman_api_response(token, endpoint)
    
    if response_data:
        total_records = response_data.get("totalRecords", 0)
        print(f"Total records available: {total_records} for {data_type} with {code_key}={code_value}")
        return total_records > 0
    else:
        print(f"No data available for {data_type} with {code_key}={code_value}")
        return False
