data_type = "billing"
code_key = "billingTypeCode"
code = 2
page = 1
Holman_Upsert_data(data_type="billing", code_key="billingTypeCode", data_key="billing",table_name=None, primary_key="vehicleNumber")

def Holman_Upsert_data(data_type, code_key, data_key, table_name, primary_key=None):
    date_list = fetch_Holman_code_data(data_type, code_key, data_key,code)
    print(json.dumps(date_list, indent=4))
    endpoint = f"{data_type}?{code_key}={code}&pageNumber={page}"
    table_name = f"bronze.holman_{data_type}"
    print(table_name)
    df = spark.createDataFrame(date_list)
    display(df)


def fetch_Holman_code_data(data_type, code_key, data_key,code):
    data_list = []
    # for code in range(1,3):
    # print(f"Fetching data from endpoint code={code}")
    page = 1
    while True:
        print(f"Fetching page {page} of data from endpoint code={code}")
        endpoint = f"{data_type}?{code_key}={code}&pageNumber={page}"
        response, response_data = get_holman_api_response(token, endpoint)
        if not response_data or data_key not in response_data or not response_data[data_key]:
            print(f"No more records on page {page}, Stopping Pagination")
            break
        if response.status_code == 200:
            fetched_data = response_data[data_key]
            data_list.extend(fetched_data)
        if int(response_data['pageNumber']) >= int(response_data['totalPages']):
            break
        page +=1
    return data_list

# fetch_Holman_code_data(data_type="billing", code_key="billingTypeCode", data_key="billing",code=2)


[CANNOT_DETERMINE_TYPE] Some of types cannot be determined after inferring.
File <command-310280906946865>, line 6
      4 code = 2
      5 page = 1
----> 6 Holman_Upsert_data(data_type="billing", code_key="billingTypeCode", data_key="billing",table_name=None, primary_key="vehicleNumber")
File <command-3234459011987704>, line 7, in Holman_Upsert_data(data_type, code_key, data_key, table_name, primary_key)
      5 table_name = f"bronze.holman_{data_type}"
      6 print(table_name)
----> 7 df = spark.createDataFrame(date_list)
      8 display(df)
