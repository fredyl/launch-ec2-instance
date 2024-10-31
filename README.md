def replace_nulls_in_dataframe(df: DataFrame):
    return df.select([when(col(c).isNull() | (col(c) == "null"), lit(" ")).otherwise(col(c)).alias(c) for c in df.columns])
