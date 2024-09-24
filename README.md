import requests
import json
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from delta.tables import DeltaTable  # Import DeltaTable for upsert operations

# Initialize Spark session
spark = SparkSession.builder.appName("API to Delta").getOrCreate()

# Function to fetch data from the API and handle errors
def lytx_get_response_from_event_api(endpoint, api_key):
    base_url = "https://lytx-api.prod5.ph.lytx.com/video"
    url = base_url + endpoint
    headers = {
        'x-apikey': api_key
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)
        return response.json()  # Return the JSON data
    except requests.exceptions.HTTPError as err:
        raise Exception(f"HTTP error occurred: {err}")
    except Exception as err:
        raise Exception(f"An error occurred: {err}")

# Function to print the API response
def generic_api_response(endpoint, api_key):
    # Get the API response
    response_data = lytx_get_response_from_event_api(endpoint, api_key)
    
    # Print a header indicating which endpoint this response belongs to
    print(f"\nResponse for endpoint: {endpoint}")
    print("=" * (len(f"Response for endpoint: {endpoint}")))
    
    # Print the response data
    print(json.dumps(response_data, indent=4))
    
    # Return the response data if needed elsewhere
    return response_data

# Function to process API response, convert to DataFrame, and perform upsert (merge) to Delta table
def process_api_data_to_delta(endpoint, api_key, table_name, database_name, primary_key):
    # Fetch the API response
    response_data = lytx_get_response_from_event_api(endpoint, api_key)
    
    # Convert JSON response to DataFrame
    if isinstance(response_data, list):
        df = spark.createDataFrame(response_data)  # Handle if the response is a list
    else:
        rdd = spark.sparkContext.parallelize([response_data])
        df = spark.read.json(rdd)
    
    # Add insert_time and update_time columns
    current_time = F.current_timestamp()
    df = df.withColumn("insert_time", current_time).withColumn("update_time", current_time)
    
    # Fully qualified table name (database and table)
    full_table_name = f"{database_name}.{table_name}"
    
    # If the Delta table exists, perform an upsert (merge)
    if spark.catalog.tableExists(full_table_name):
        print(f"Table {full_table_name} exists. Performing upsert (merge)...")
        
        # Load the Delta table from the database
        delta_table = DeltaTable.forName(spark, full_table_name)  # Reference the Delta table in the database

        # Perform the upsert (merge) operation
        delta_table.alias("existing_data") \
          .merge(df.alias("new_data"), F.expr(f"new_data.{primary_key} = existing_data.{primary_key}")) \
          .whenMatchedUpdate(set={
              "update_time": current_time, 
              **{col: F.col(f"new_data.{col}") for col in df.columns if col != primary_key}
          }) \
          .whenNotMatchedInsert(values={
              **{col: F.col(f"new_data.{col}") for col in df.columns}
          }) \
          .execute()

        print(f"Upsert (merge) completed successfully for {full_table_name}.")
    
    # If the Delta table does not exist, create a new table
    else:
        print(f"Table {full_table_name} does not exist. Creating new table...")
        df.write.format("delta").mode("overwrite").saveAsTable(full_table_name)
        print(f"Table {full_table_name} created successfully with new data.")

# Example usage for different endpoints
api_key = "jhdglwxuhqo6y499luevjabs7dbkjdbLxy"
endpoints = [
    "/safety/events/behaviors",
    "/safety/events/triggersubtypes",
    "/safety/events/triggers"
]
database_name = "your_database_name"  # Replace with your database name

# Dictionary mapping each endpoint to its primary key
endpoint_primary_key_map = {
    "/safety/events/behaviors": "behavior_id",      # Example primary key for this endpoint
    "/safety/events/triggersubtypes": "subtype_id", # Example primary key for this endpoint
    "/safety/events/triggers": "trigger_id"         # Example primary key for this endpoint
}

# Iterate over endpoints, print response, and save to Delta in the database
for endpoint in endpoints:
    try:
        # Print the API response
        generic_api_response(endpoint, api_key)
        
        # Automatically generate table names based on endpoint
        table_name = f"delta_{endpoint.split('/')[-1]}_table"
        
        # Get the correct primary key for the current endpoint from the dictionary
        primary_key = endpoint_primary_key_map.get(endpoint)
        
        if primary_key is None:
            raise Exception(f"No primary key found for endpoint: {endpoint}")
        
        # Process the response and save/update Delta table in the database
        process_api_data_to_delta(endpoint, api_key, table_name, database_name, primary_key)
    except Exception as e:
        print(f"Error processing endpoint {endpoint}: {e}")
