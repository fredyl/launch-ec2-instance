# Create a DataFrame with a single URL for testing
test_df = spark.createDataFrame([(single_url,)], ["url"])

# Define the UDF to fetch data
fetch_data_udf = udf(lambda url: fetch_data_from_url(url), StringType())

# Apply the UDF to the DataFrame and show the result
test_df = test_df.withColumn("data", fetch_data_udf(col("url")))
test_df.show(truncate=False)


# Test a single URL directly
single_url = "https://customer-experience-api.arifleet.com/v1/billing?billingTypeCode=1&pageNumber=1"
data = fetch_data_from_url(single_url)
print("Data from single URL test:", data)
