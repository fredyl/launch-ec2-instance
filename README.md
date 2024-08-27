from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def create_or_update_table(spark: SparkSession, dataframe, table_name, primary_key):
    # Step 1: Check if the table exists
    if spark._jsparkSession.catalog().tableExists(table_name):
        print(f"Table {table_name} exists. Performing an update.")

        # Step 2: Create a temporary view from the DataFrame
        dataframe.createOrReplaceTempView("temp_view")

        # Step 3: Perform a merge operation
        merge_query = f"""
        MERGE INTO {table_name} AS target
        USING temp_view AS source
        ON target.{primary_key} = source.{primary_key}
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
        """
        spark.sql(merge_query)
    else:
        print(f"Table {table_name} does not exist. Creating a new table.")

        # Step 4: Write the DataFrame to a new table
        dataframe.write.mode("overwrite").saveAsTable(table_name)

# Example usage:
# Assuming 'df' is your DataFrame, and you want to use 'id' as the primary key
create_or_update_table(spark, df, "my_table_name", "id")
