based on this result it shows all the urls are stored in different volumes with same name.
how can insert all the urls into volumes with different names or different the volumes as per 
url name or number allocated 

Checking data for violation with code key violationDateCode = 2
Received 200, Success from the API.
Total Pages : 81
 num_batches : 1
len 81
Total URLs : 81
['/Volumes/dev/bronze_vendor/holman/data.json', '/Volumes/dev/bronze_vendor/holman/data.json', '/Volumes/dev/bronze_vendor/holman/data.json', '/Volumes/dev/bronze_vendor/holman/data.json', '/Volumes/dev/bronze_vendor/holman/data.json', '/Volumes/dev/bronze_vendor/holman/data.json', '/Volumes/dev/bronze_vendor/holman/data.json', '/Volumes/dev/bronze_vendor/holman/data.json', 


def fetch_data_from_url(url):
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f"Bearer {token}"
    }
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        return f"Error: {response.status_code}"
    
    chk_path = '/Volumes/dev/bronze_vendor/holman/data.json'
    with open(chk_path, 'w') as f:
        f.write(response.text)
    return chk_path



# Generates paginated URLs based on the total number of pages
def generate_paginated_urls(data_type,data_key, code_key, code_value,total_pages,batch_size=200):
    base_url = "https://customer-experience-api.arifleet.com/v1/"
    pagination_urls = []
    num_batches = (total_pages // batch_size) + 1
    print(f" num_batches : {num_batches}")
    for i in range(num_batches):
        for page in range(i * batch_size + 1, min((i + 1) * batch_size + 1, total_pages +1)):
            url = f"{base_url}{data_type}?{code_key}={code_value}&pageNumber={page}"
            pagination_urls.append({"pageNumber": page, "url": url})
            # print(pagination_urls)
    print('len', len(pagination_urls))
    return pagination_urls, num_batches

def get_total_pages(data_type, code_key, code_value):
    endpoint = f"{data_type}?{code_key}={code_value}"
    response_data  = get_holman_api_response(token, endpoint)
    if response_data:
        return int(response_data.get("totalPages", 1))
    return 0

fetch_data_udf = udf(fetch_data_from_url, StringType())


holman_coded_endpoints = [
   
    # {"data_type": "fuels", "code_key": "transDateCode", "data_key": "can", "primary_key": "clientVehicleNumber"},
   
    {"data_type": "violation", "code_key": "violationDateCode", "data_key": "violations", "primary_key": "record_id"},
    # {"data_type": "billing", "code_key": "billingTypeCode", "data_key": "billing", "primary_key": "vehicleNumber"},
    #  {"data_type": "fuels", "code_key": "transDateCode", "data_key": "us", "primary_key": "usRecordID"},
]

# Main execution loop for fetching and processing data
for endpoint_config in holman_coded_endpoints:
    data_type = endpoint_config["data_type"]
    code_key = endpoint_config["code_key"]
    data_key = endpoint_config["data_key"]
    primary_key = endpoint_config["primary_key"]

    for code_value in range(1, 4):  # Looping through code_key values 1 to 3
        print(f"\nChecking data for {data_type} with code key {code_key} = {code_value}")

        # Retrieve total pages and skip if no pages are available
        total_pages = get_total_pages(data_type, code_key, code_value)
        print(f"Total Pages : {total_pages}")
        if total_pages == 0:
            print(f"No pages available for {data_type} with code key {code_key} = {code_value}. Skipping...")
            continue
        
        paginated_urls, num_batches = generate_paginated_urls(data_type,data_key, code_key, code_value,total_pages)
        print(f"Total URLs : {len(paginated_urls)}")
        batch_df = spark.createDataFrame(paginated_urls)
        batch_df = batch_df.withColumn("part", floor(col("pageNumber") / 100)).repartition(num_batches, "part")
        result_df = batch_df.withColumn("data", fetch_data_udf(col("url"))).cache()
        
        batch_df = spark.read.json(f"{chk_path}")
        file_path = [row.data for row in result_df.select("data").collect()]
        print(file_path)
    
