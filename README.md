def create_or_update_table(headers, rows, table_name,primary_key_columns):

    schema = StructType([StructField(header, StringType(), True) for header in headers])
    df = spark.createDataFrame([tuple(row.values()) for row in rows], schema)

    if spark._jsparkSession.catalog().tableExists(table_name):
        print(f"Table {table_name} exists, updating")

        existing_df = spark.table(table_name)

        df.createOrReplaceGlobalTempView('new_data')
        
        merge_condition = ""
        for col in primary_key_columns:
            if merge_condition != "":
                merge_condition += " AND "
            merge_condition += "t." + col + " = n." + col

        merge_query = f"""
        MERGE INTO {table_name} AS t
        USING new_data AS n
        ON {merge_condition}
        WHEN MATCHED THEN 
        UPDATE SET *
        WHEN NOT MATCHED
         THEN INSERT *
        """
    else: print(f"Table {table_name} does not exist. Creating a new table.")

    # If the table does not exist, create a new Delta table
    df.write.format("delta").saveAsTable(table_name)

# Verify that the table was created or updated successfully
if not spark._jsparkSession.catalog().tableExists(table_name):
    raise Exception(f"Failed to create or update the Delta table: {table_name}")


def create_or_update_table(headers, rows, table_name,primary_key_columns):

    schema = StructType([StructField(header, StringType(), True) for header in headers])
    df = spark.createDataFrame([tuple(row.values()) for row in rows], schema)

    if spark._jsparkSession.catalog().tableExists(table_name):
        print(f"Table {table_name} exists, updating")

        existing_df = spark.table(table_name)

        df.createOrReplaceGlobalTempView('new_data')
        
        merge_condition = ""
        for col in primary_key_columns:
            if merge_condition != "":
                merge_condition += " AND "
            merge_condition += "t." + col + " = n." + col

        merge_query = f"""
        MERGE INTO {table_name} AS t
        USING new_data AS n
        ON {merge_condition}
        WHEN MATCHED THEN 
        UPDATE SET *
        WHEN NOT MATCHED
         THEN INSERT *
        """
    else: print(f"Table {table_name} does not exist. Creating a new table.")

    # If the table does not exist, create a new Delta table
    df.write.format("delta").saveAsTable(table_name)

# Verify that the table was created or updated successfully
if not spark._jsparkSession.catalog().tableExists(table_name):
    raise Exception(f"Failed to create or update the Delta table: {table_name}")
