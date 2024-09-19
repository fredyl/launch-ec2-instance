# Create the UPDATE and MERGE condition statements, handling special characters like '@'
        update_columns = ", ".join([f"t.`{col}` = n.`{col}`" if "@" in col else f"t.{col} = n.{col}" for col in table_columns if col != "InsertTime"])

        merge_condition = " AND ".join([f"t.`{col}` = n.`{col}`" if "@" in col else f"t.{col} = n.{col}" for col in primary_key])

        # Dynamically create the INSERT columns and values
        insert_columns = ", ".join([f"`{col}`" if "@" in col else col for col in table_columns])
        insert_values = ", ".join([f"n.`{col}`" if "@" in col else f"n.{col}" for col in table_columns])
