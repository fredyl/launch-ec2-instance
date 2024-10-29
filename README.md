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


for code_value in range(1, 4):  # Looping through code_key values 1 to 3
        print(f"\nChecking data for {data_type} with code key {code_key} = {code_value}")
        
        # Preliminary check to see if data is available
        if not check_data_availability("your_token", data_type, code_key, code_value):
            print(f"No data found for {data_type} with code key {code_key} = {code_value}. Skipping...")
            continue  # Skip to the next code_value if no data is available
