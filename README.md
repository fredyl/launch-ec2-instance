import requests
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

# Initialize Spark session
spark = SparkSession.builder.appName("lytx_data_ingestion").getOrCreate()

# Define the endpoint URL with dynamic dates (using ISO format)
today = datetime.date.today().isoformat()  # Get today in ISO format (YYYY-MM-DD)
two_years_ago = (datetime.date.today() - datetime.timedelta(days=2*365)).isoformat()  # Two years ago in ISO format

# Construct the API URL with ISO formatted dates
api_url = f"https://lytx-api.prod5.ph.lytx.com/video/events?EndDate={today}&StartDate={two_years_ago}"

# Fetch data from the Lytx API
try:
    response = requests.get(api_url)
    response.raise_for_status()  # Raises an error if the request fails
    data = response.json()  # Assuming the API returns JSON data
except requests.exceptions.HTTPError as err:
    print(f"HTTP error occurred: {err}")
    data = None
except Exception as err:
    print(f"An error occurred: {err}")
    data = None

# Check if data is available before proceeding
if data:
    # Convert JSON data to a Spark DataFrame
    df = spark.read.json(spark.sparkContext.parallelize([data]))

    # Add a timestamp for when the data was last modified
    df = df.withColumn("LastModified", current_timestamp())

    # Define Delta table path and name
    table_name = "bronze.lytx_video_events"

    # Check if the Delta table exists
    if DeltaTable.isDeltaTable(spark, table_name):
        delta_table = DeltaTable.forName(spark, table_name)

        # Perform upsert (merge) operation
        delta_table.alias("tgt").merge(
            df.alias("src"),
            "tgt.id = src.id"  # Assuming 'id' is the unique identifier for records
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        print(f"Data successfully upserted into {table_name}.")
    else:
        # If the table does not exist, create it by writing the DataFrame to Delta
        df.write.format("delta").saveAsTable(table_name)
        print(f"New Delta table {table_name} created and data inserted.")
else:
    print("No data fetched to upsert.")
