def simple_replace_nulls(df):
    # Replace `NULL` and `"null"` values in all columns
    cleaned_df = df.select([
        when(col(c).isNull() | (col(c) == "null"), lit(" ")).otherwise(col(c)).alias(c)
        for c in df.columns
    ])
    return cleaned_df
