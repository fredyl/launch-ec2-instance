Exception: ('Failed:', 400, '{"type":"https://tools.ietf.org/html/rfc7231#section-6.5.1","title":"One or more validation errors occurred.","status":400,"traceId":"80000300-0000-ea00-b63f-84710c7967bb","errors":{"billingTypeCode":["The value \'\' is invalid."]}}')
File <command-1723439101937380>, line 19
     14 primary_key = endpoint_config["primary_key"]
     16 for code_value in range(1, 4):  # Looping through code_key values 1 to 3
     17 
     18      # Preliminary check to see if data is available
---> 19     if not check_data_availability(token, data_type, code_key, code_value):
     20         print(f"No data found for {data_type} with code key {code_key} = {code_value}. Skipping...")
     21         continue  # Skip to the next code_value if no data is available
File <command-1723439101937694>, line 4, in check_data_availability(token, data_type, code_key, code_value)
      2 def check_data_availability(token, data_type, code_key, code_value): 
      3     endpoint = f"{data_type}?{code_key}" 
----> 4     _, response_data = get_holman_api_response(token, endpoint)
      6     if response_data:
      7         total_records = response_data.get("totalRecords", 0)
File <command-4434650908684363>, line 21, in get_holman_api_response(token, endpoint, param_pagination)
     19     return get_holman_api_response(token, endpoint)
     20 else:
---> 21     raise Exception("Failed:", response.status_code, response.text)
