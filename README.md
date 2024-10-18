Exception: ('Failed:', 400, '{"type":"https://tools.ietf.org/html/rfc7231#section-6.5.1","title":"One or more validation errors occurred.","status":400,"traceId":"80003f12-0000-cf00-b63f-84710c7967bb","errors":{"pageNumber":["The value \'{}\' is not valid."]}}')
File <command-3094085711592474>, line 39
     37 # for code in range(1,4):
     38 while True:
---> 39     data_list = fetch_holman_code_batch_data(data_type, code_key, data_key,code,batch_size=50)
     40     clean_data_list = replace_null_values(data_list)
     41     # print(json.dumps(clean_data_list, indent=4))
File <command-310280906944989>, line 16, in get_holman_api_response(token, endpoint)
     14     return response, response_data
     15 else:
---> 16     raise Exception("Failed:", response.status_code, response.text)
