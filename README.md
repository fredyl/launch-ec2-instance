from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import floor, col
params = {
        "data_type": "fuels",
        "code_key": "transDateCode",
        "data_key": "us",
        "primary_key": "usRecordID"
    }

def get_end_data(token, data_type, endpoint, params, batch_size = 200):
    total_pages = get_all_pages(token, data_type, endpoint)

    data_array = []
    for page in range(1, total_pages + 1):
        param_pagination = {**params, "pageNumber": page}
        data_array.append({"url":f"https://customer-experience-api.arifleet.com/v1/{data_type}", "params":param_pagination})

    schema = StructType([
        StructField("pageNumber", IntegerType(), nullable=True),
        StructField("data", StringType(), nullable=True)
    ])

    full_query_df = spark.createDataFrame(data_array, schema)
    full_query_df = full_query_df.withColumn('part', floor(col('pageNumber') / 100)).repartition(col('part'))
    display(full_query_df)

get_end_data(token, "fuels", "https://customer-experience-api.arifleet.com/v1/fuels", params, batch_size=200)



def get_all_pages(token, data_type,endpoint):
    response, response_data  = get_holman_api_response(token, endpoint, param_pagination = None)
    if response.status_code == 200:
        response_data = response.json()
        total_pages = int(response_data.get("totalPages", 1))
        return total_pages
    else:
        raise Exception("Failed:", response.status_code, response.text)


def get_holman_api_response(token, endpoint, param_pagination = None):
    base_url = "https://customer-experience-api.arifleet.com/v1/"
    url = base_url + endpoint
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f"Bearer {token}"
    }
    # headers['Authorization'] = f"Bearer {token}"
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 204:
        print(f"Received 204, No Content from the API.")
        return response, None
    elif response.status_code == 200:
        response_data = response.json()
        return response, response_data
    elif response.status_code == 401:
        token = get_token()
        time.sleep(5)
        return get_holman_api_response(token, endpoint)
    else:
        raise Exception("Failed:", response.status_code, response.text)


def get_token():
    url = "https://customer-experience-api.arifleet.com/v1/users/authenticate"
    payload = {
        "Username" : "9AD0ED33263920833DC6E47924C3A80BCECD4D86",
        "Password" : "Ap4PGA9f"
    }
    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    if response.status_code == 200:
        token = response.json().get("token")
        return token
    else:
        print(f"Authentication failed: {response.status_code}, {response.text}")

token = get_token()
