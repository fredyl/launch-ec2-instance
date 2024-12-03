def get_data_size_with_spark(path):
    data_df = spark.read.format("delta").load(path)  # Adjust format as needed (e.g., "csv", "parquet")
    data_size = data_df.rdd.mapPartitions(lambda iter: [sum(len(bytes(str(row))) for row in iter)]).sum()
    return data_size

# Example usage:
data_path = "/Volumes/dev/bronze_vendor/nightlyfiles/view_name_202411"
total_size_bytes = get_data_size_with_spark(data_path)
