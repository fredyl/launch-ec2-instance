from pyspark.sql.functions import current_timestamp, col, expr
from delta.tables import DeltaTable

# Function to fetch Holman API data within a partition
def get_holman_data_partition(iterator, token, data_type, data_key, primary_key):
    limit = 200  # Define a limit if necessary for the API calls
    data_list = []
    
    for _ in iterator:
        page = 1  # Start pagination from page 1
        while True:
            endpoint = f"{data_type}?pageNumber={page}"
            response, response_data = get_holman_api_response(token, endpoint)
            if response.status_code == 200 and response_data:
                batch_data = response_data.get(data_key, [])
                total_pages = int(response_data.get('totalPages', 1))  # Dynamically calculate totalPages
                if len(batch_data) == 0 or page > total_pages:
                    print(f"No more records on page {page}, stopping pagination")
                    break
                data_list.extend(batch_data)
                print(f"Processing page {page}, with {len(batch_data)} records")
                page += 1
            else:
                break

    return iter(data_list)  # Returning an iterator for the partition

# Function to parallelize data fetching using mapPartitions
def parallelize_holman_data_fetch(token, data_type, data_key, primary_key, partition_range=100, num_partitions=10):
    """
    Function to parallelize data fetching using mapPartitions.
    This function handles the partitioning and API call distribution.
    """
    # Define an RDD to simulate partitioning over a range
    rdd = spark.sparkContext.parallelize(range(0, partition_range), numSlices=num_partitions)

    # Use mapPartitions to fetch data in parallel across partitions
    partitioned_data = rdd.mapPartitions(
        lambda iterator: get_holman_data_partition(iterator, token, data_type, data_key, primary_key)
    )

    # Collect the fetched data
    data_list = partitioned_data.collect()
    return data_list

# Function to replace null values in the list of dictionaries
def replace_null_values(item_list):
    for item in item_list:
        for key, value in item.items():
            if value == "null" or value is None:
                item[key] = ""
    return item_list

# Generic Upsert function for Holman API
def Holman_Upsert_data(data_type, data_key, data_list, primary_key=None):
    table_name = f"bronze.holman_{data_type}_{data_key}"
    print(f"Upserting data to table: {table_name}")

    # Create DataFrame from the data_list
    df = spark.createDataFrame(data_list)
    current_time = current_timestamp()
    df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)

    # Check for duplicates based on primary key
    dup_check_df = df.groupBy(primary_key).count().filter(col("count") > 1)
    dup_check_df.display()
    duplicate_id = [row[primary_key] for row in dup_check_df.collect()]
    df = df.filter(~col(primary_key).isin(duplicate_id))

    if spark.catalog.tableExists(table_name):
        print(f"Table {table_name} exists. Performing upsert (merge)...")
        delta_table = DeltaTable.forName(spark, table_name)
        delta_table.alias("existing_data") \
            .merge(
                df.alias("new_data"), expr(f"new_data.{primary_key} = existing_data.{primary_key}")
            ) \
            .whenMatchedUpdate(
                set={col_name: col(f"new_data.{col_name}") for col_name in df.columns if col_name != primary_key and col_name != "tg_inserted"}
            ) \
            .whenNotMatchedInsert(
                values={col_name: col(f"new_data.{col_name}") for col_name in df.columns}
            ) \
            .execute()
        print(f"Upsert completed for {table_name}")
    else:
        print(f"Table does not exist, creating new table {table_name}")
        df.write.format("delta").saveAsTable(table_name)

# Dynamically calculate totalPages based on API response
def fetch_holman_code_batch_data(data_type, code_key, data_key, code, token, batch_size=200):
    data_list = []
    endpoint_key = f"Holman_{data_type}_{code_key}_{code}"
    page = 1
    pages_fetched = 0
    
    while True:
        print(f"Fetching page {page} of data from {endpoint_key}")
        endpoint = f"{data_type}?{code_key}={code}&pageNumber={page}"
        response, response_data = get_holman_api_response(token, endpoint)
        
        if not response_data or data_key not in response_data or not response_data[data_key]:
            print(f"No more records on page {page}, stopping pagination")
            break

        if response.status_code == 200:
            fetched_data = response_data[data_key]
            data_list.extend(fetched_data)
            total_pages = int(response_data.get('totalPages', 1))  # Dynamically calculate totalPages
            
            if page >= total_pages:
                break
            page += 1
            pages_fetched += 1
        else:
            print(f"Error: {response.status_code} {response.text}")
            break
    
    return data_list


# Define the set of endpoints with corresponding data keys
holman_endpoints = {
    "accidents": ("accident", "record_id"),
    "Odometer": ("odometerHistory", "vin"),
    "orders": ("order", "order_id"),
    "persons": ("person", "contactEmployeeId")
}

# Iterate through the endpoints to fetch and upsert data using partitions
for data_type, (data_key, primary_key) in holman_endpoints.items():
    print(f"Fetching and upserting data for {data_type}_{data_key}")
    
    # Parallelize the fetching process
    data_list = parallelize_holman_data_fetch(token, data_type, data_key, primary_key)
    
    if data_list:
        # Clean and upsert the data after it is fetched
        clean_data_list = replace_null_values(data_list)
        Holman_Upsert_data(data_type, data_key, clean_data_list, primary_key)
    else:
        print(f"No data found for {data_type}_{data_key}")
