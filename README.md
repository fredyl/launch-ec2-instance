Exception: ('Failed:', 404, '')
File <command-1930516570011813>, line 15
     13 #iterate through the endpoints to fetch and upsert data
     14 for data_type, (data_key, primary_key) in holman_endpoints.items():
---> 15     data_list = get_holman_data(token=token, data_type=data_type, data_key=data_key, use_delta=use_delta)
     16     data_list = replace_null_values(data_list)
     18     if data_list:
File <command-1930516570011811>, line 11, in get_holman_data(token, data_type, data_key, use_delta)
      9 while True:
     10     endpoint = f"{data_type}?pageNumber={page}"
---> 11     response_data = get_holman_api_response(token, endpoint, 3, use_delta)
     12     batch_data = response_data.get(data_key, [])
     13     if len(batch_data) == 0:
File <command-1930516570011806>, line 23, in get_holman_api_response(token, endpoint, retries, use_delta)
     21         time.sleep(5)
     22     else:
---> 23         raise Exception("Failed:", response.status_code, response.text)
     24 raise Exception("Max retries reached for token refresh")
