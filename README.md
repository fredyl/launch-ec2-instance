joined_df = existing_data.alias("existing_data").join(
        spark_df.alias("new_data"), 
        F.expr(f"existing_data.{primary_key} = new_data.{primary_key}"),
        how="inner"
    )

    # Check for column mismatches
    mismatch_conditions = [
        F.col(f"existing_data.{col}") != F.col(f"new_data.{col}")
        for col in spark_df.columns if col != primary_key
    ]
    
    mismatches = joined_df.filter(F.coalesce(*mismatch_conditions)).select("existing_data.*", "new_data.*")

    if mismatches.count() == 0:
        print("No column mismatches found between existing and new data.")
    else:
        print(f"Found mismatches between existing and new data in the following rows:")
        mismatches.show(truncate=False)
