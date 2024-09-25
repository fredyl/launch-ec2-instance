when I use the functions statuses_api_response the data is populated as below. but then when trying to create a dataframe while using the function below process_api_data_to_delta i get _corrupt_record.
Why the dataframes comes as corrupted record and how can I fix the issue. since will need to use the dataframe to create a delta table

def statuses_api_response():
    endpoint = "/vehicles/all"   
    response_data = lytx_get_repoonse_from_event_api(endpoint)
    return response_data
    print(response_data)
statuses_api_response()


def process_api_data_to_delta(endpoint, table_name):
    # Fetch the API response
    response_data = lytx_get_repoonse_from_event_api(endpoint)
    
    # Convert JSON response to DataFrame
    if isinstance(response_data, list):
        df = spark.createDataFrame(response_data)  # Handle if the response is a list
    else:
        rdd = spark.sparkContext.parallelize([response_data])
        df = spark.read.json(rdd)
    
    # Add insert_time and update_time columns
    current_time = F.current_timestamp()
    df = df.withColumn("insert_time", current_time).withColumn("update_time", current_time)
    df.display()

    
from pyspark.sql.functions import explode

# Explode the devices array into individual rows
df_flattened = df.withColumn("device", explode("devices")) \
                 .select("id", "name", "status", "device.serialNumber", "device.views")

# Show the flattened DataFrame
df_flattened.display()
