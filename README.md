def generate_paginated_urls(token, data_type, code_key, code_value, total_pages, batch_size=200):
    base_url = "https://customer-experience-api.arifleet.com/v1/"
    pagination_urls = [
        {"pageNumber": page, "url": f"{base_url}{data_type}?{code_key}={code_value}&page={page}&pageSize={batch_size}"}
        for page in range(1, total_pages + 1)
    ]
    return pagination_urls, len(pagination_urls)


for endpoint_config in holman_coded_endpoints:
    data_type, code_key, data_key, primary_key = endpoint_config.values()
    for code_value in range(1, 4):
        total_pages = get_all_pages(token, data_type, code_key, code_value)
        if total_pages == 0:
            logging.info(f"No data for {data_type} with {code_key}={code_value}")
            continue
        
        batch_urls, loops = generate_paginated_urls(token, data_type, code_key, code_value, total_pages)
        batch_df = spark.createDataFrame(batch_urls).withColumn("part", floor(col("pageNumber") / 100)).repartition(loops, "part")

        result_df = batch_df.withColumn("DataOutput", fetch_data_udf(col("url"))).cache()
        file_paths = [row.DataOutput for row in result_df.collect() if row.DataOutput]  # Avoid empty paths
        output_df = spark.read.json(file_paths).select(explode(col(data_key)).alias(data_key)).select(f"{data_key}.*")

        if output_df.count() > 0:
            Holman_Upsert_data(data_type, data_key, output_df.collect(), primary_key)
            logging.info(f"Data upserted for {data_type} with {code_key}={code_value}")
        else:
            logging.info(f"No data to upsert for {data_type} with {code_key}={code_value}")
