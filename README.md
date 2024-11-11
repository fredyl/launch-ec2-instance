Exception: ('Failed:', 404, '')
File <command-1930516570011813>, line 16
     13 print(f"Processing {data_type}")
     15 if upd_endpoints and upd_endpoints.get("run_all"):
---> 16     data_list = get_holman_data(token, data_type=data_type, data_key=data_key, url_ext=delta_url)
     17 elif upd_endpoints and "delta_url" in upd_endpoints:
     18     delta_url = upd_endpoints["delta_url"]
File <command-3260448887818671>, line 21, in get_holman_api_response(token, endpoint, retries, use_delta)
     19         time.sleep(5)
     20     else:
---> 21         raise Exception("Failed:", response.status_code, response.text)
     22 raise Exception("Max retries reached for token refresh")
