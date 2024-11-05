delta_table.alias("existing_data") \
            .merge(df.alias("new_data"), expr(f"new_data.{primary_key} = existing_data.{primary_key}")) \
            .whenMatchedUpdate(
                set={col_name: col(f"new_data.{col_name}") for col_name in df.columns if col_name not in [primary_key, "tg_inserted"]}
            ) \
            .whenNotMatchedInsertAll() \
            .execute()
