df = df.filter(~col(primary_key).isin(duplicate_id) | (col("some_other_column") != "some_value"))
