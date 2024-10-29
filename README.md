why having the below output after running the udf function as below

for code_value in range(1, 4):  # Loop through code_key values 1 to 3
        print(f"\nChecking data for {data_type} with code key {code_key} = {code_value}")
        
        # Retrieve total pages and skip if no pages are available
        total_pages = get_all_pages(token, data_type, code_key, code_value)
        print(f"Total pages for {code_key}_{code_value}: {total_pages}")
        if total_pages == 0:
            print(f"No pages available for {data_type} with code key {code_key} = {code_value}. Skipping...")
            continue

        batch_urls, loops =generate_paginated_urls(token, data_key, code_key, code_value, batch_size=200)
        batch_df = spark.createDataFrame(batch_urls)
        batch_df = batch_df.withColumn("part", floor(col("pageNumber") / 100)).repartition(loops, "part")
        # batch_df.show()

        # Apply the UDF to fetch data for each URL
        result_df = batch_df.withColumn("data", fetch_data_udf(col("url")))
        result_df.show()

Checking data for billing with code key billingTypeCode = 1
Received 200, Success from the API.
Total pages: 1133
Total pages for billingTypeCode_1: 1133
Received 200, Success from the API.
Total pages: 1133
 loops : 6
+----------+--------------------+----+----+
|pageNumber|                 url|part|data|
+----------+--------------------+----+----+
|         1|https://customer-...|   0|NULL|
|         2|https://customer-...|   0|NULL|
|         3|https://customer-...|   0|NULL|
|         4|https://customer-...|   0|NULL|
|         5|https://customer-...|   0|NULL|
|         6|https://customer-...|   0|NULL|
+----------+--------------------+----+----+


Checking data for billing with code key billingTypeCode = 2
Received 200, Success from the API.
Total pages: 0
Total pages for billingTypeCode_2: 0
No pages available for billing with code key billingTypeCode = 2. Skipping...

Checking data for billing with code key billingTypeCode = 3
Received 200, Success from the API.
Total pages: 0
Total pages for billingTypeCode_3: 0
No pages available for billing with code key billingTypeCode = 3. Skipping...
