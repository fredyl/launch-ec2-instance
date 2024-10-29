 batch_urls, loops =generate_paginated_urls(token, data_key, code_key, code_value, batch_size=200)
 batch_df = spark.createDataFrame(batch_urls)
 batch_df = batch_df.withColumn("part", floor(col("pageNumber") / 10)).repartition(loops, "part")
