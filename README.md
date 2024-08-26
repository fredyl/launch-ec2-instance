else:
        print(f"Table {table_name} does not exist. Creating a new table.")
        
        # If the table does not exist, create a new Delta table
        df.write.format("delta").saveAsTable(table_name)

    # Verify that the table was created or updated successfully
    if not spark._jsparkSession.catalog().tableExists(table_name):
        raise Exception(f"Failed to create or update the Delta table: {table_name}")
