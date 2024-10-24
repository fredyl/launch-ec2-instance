def get_holman_code_data_partition_batch(iterator, data_type, code_key, data_key, code, token, batch_size=200):
    
    data_list = []
    endpoint_key = f"Holman_{data_type}_{code_key}_{code}"
    page = get_last_checkpoint(endpoint_key)
    pages_fetched = 0
    
    for _ in iterator:
        while pages_fetched < batch_size:
            print(f"Fetching page {page} of data from {endpoint_key}")
            endpoint = f"{data_type}?{code_key}={code}&pageNumber={page}"
            response, response_data = get_holman_api_response(token, endpoint)
            
            if response.status_code != 200 or not response_data.get(data_key):
                print(f"No more recodes on page {page}, Stopping Pagination")
                break


            fetched_data = response_data[data_key]
            total_pages = int(response_data.get('total_pages', 1))
            print(f"total_pages : {total_pages}")

            if len(response_data) == 0 or page > total_pages:
                break

            data_list.extend(fetched_data)
            print(f"Processing page {page}, with {len(fetched_data)} records")

            page += 1
            pages_fetched += 1

            save_checkpoint(endpoint_key, page)

        if pages_fetched >= batch_size:
            print(F"Fetch batch_size {batch_size}, stopping for batch")
        break

    return iter(data_list)


def parallelize_holman_data_fetch(token, data_type,code_key, data_key, primary_key, partition_range=100, num_partitions=10):
    """
    Function to parallelize data fetching using mapPartitions.
    This function handles the partitioning and API call distribution.
    """
    # Define an RDD to simulate partitioning over a range
    rdd = spark.sparkContext.parallelize(range(0, partition_range), numSlices=num_partitions)

    # Use mapPartitions to fetch data in parallel across partitions
    partitioned_data = rdd.mapPartitions(
        lambda iterator: get_holman_code_data_partition_batch(iterator, data_type, code, data_key, code, token, batch_size=200)
    )

    # Collect the fetched data
    data_list = partitioned_data.collect()
    return data_list


# Define the set of endpoints with corresponding data keys
#define a set of endpoints with corresponding data keys
holman_coded_endpoints =[
    # {
    #     "data_type" : "billing",
    #     "code_key": "billingTypeCode",
    #     "data_key": "billing",
    #     "primary_key": "invoiceNumber"
    #  },
    # {
    #     "data_type": "fuels",
    #     "code_key": "transDateCode",
    #     "data_key": "can",
    #     "primary_key": "clientVehicleNumber"
    # },
    {
        "data_type": "fuels",
        "code_key": "transDateCode",
        "data_key": "us",
        "primary_key": "usRecordID"
    },
    # {
    #     "data_type" : "violation",
    #     "code_key": "violationDateCode",
    #     "data_key": "violations",
    #     "primary_key": "record_id"
    # }
]
   

#iterate through the endpoints to fetch and upsert data
for endpoint_config in holman_coded_endpoints:
    data_type = endpoint_config["data_type"]
    code_key = endpoint_config["code_key"]
    data_key = endpoint_config["data_key"]
    primary_key = endpoint_config["primary_key"]
    code = 2
    page =1
    # for code in range(1,4):
    while True:
        data_list = parallelize_holman_data_fetch(token, data_type,code, data_key, primary_key)
        clean_data_list = replace_null_values(data_list)
        # print(json.dumps(data_list, indent=4))
        if data_list:
            # Clean and upsert the data after it is fetched
            clean_data_list = replace_null_values(data_list)
            Holman_Upsert_data(data_type, data_key, clean_data_list, primary_key)
        else:
            print(f"No data found for {data_type}_{data_key}")
            break



why am I getting only the below response from the output. How will I have all the print statement from def get_holman_code_data_partition_batch(iterator, data_type, code_key, data_key, code, token, batch_size=200):


bronze.holman_fuels_us
Table bronze.holman_fuels_us exists. Performing upsert (merge)...
Upsert completed for bronze.holman_fuels_us
bronze.holman_fuels_us
Table bronze.holman_fuels_us exists. Performing upsert (merge)...
Upsert completed for bronze.holman_fuels_us
bronze.holman_fuels_us
Table bronze.holman_fuels_us exists. Performing upsert (merge)...
Upsert completed for bronze.holman_fuels_us
bronze.holman_fuels_us
Table bronze.holman_fuels_us exists. Performing upsert (merge)...

