def get_billing_data():
    data_type = "billing"
    data_key = "billing"
    primary_key = "vehicleNumber"
    code_key = "billingTypeCode"
    key_code = "invoiceDateCode"
    data_list = []

    d_path = f'/Volumes/{env}/bronze_vendor/holman/{data_type}'
    dbutils.fs.mkdirs(d_path)
    
    for code_value in range(1, 4):
        key_value = 1
        print(f"\nChecking data for {data_type} with code key {code_key} = {code_value} and {key_code}={key_value}")

        total_pages = get_total_pages(data_type, code_key, code_value, key_code, key_value)
        if total_pages == 0:
            print(f"No pages available for {data_type} with code key {code_key} = {code_value}. Skipping...")
            continue
        print(f"Total Pages : {total_pages}")

        paginated_urls, num_batches = generate_paginated_urls(
            data_type, data_key, code_key, code_value, total_pages, key_value, key_code
        )
        print(f"Total URLs : {len(paginated_urls)}")

        batch_df = spark.createDataFrame(paginated_urls)
        batch_df = batch_df.withColumn("part", floor(col("pageNumber") / 100)).repartition(num_batches, "part")
        result_df = batch_df.withColumn(
            "data", fetch_data_udf(col("url"), col("pageNumber"), lit(data_type), lit(code_value), lit(d_path), lit(key_value))
        ).cache()

        file_path = [row.data for row in result_df.select("data").collect()]
        if not file_path:
            print(f"No Files found for {data_type} with code key {code_key} = {code_value} and key code {key_code}={key_value}.Skipping....")
            continue

        o_df = spark.read.json(file_path)
        if o_df.rdd.isEmpty():
            print(f"No data found for {data_type} with code key {code_key} = {code_value} and key code {key_code} = {key_value}")

        json_data = o_df.select(data_key).toJSON().collect()
        cleaned_data = [replace_null_values(json.loads(row)) for row in json_data]
        o_df = spark.createDataFrame([Row(**item) for item in cleaned_data])

        data_list = []
        for row in o_df.collect():
            if data_key in row and isinstance(row[data_key], list):
                data_list.extend(row[data_key])
        
        print(f"Total data fetched for {data_type} with code key {code_key} = {code_value} and key code {key_code} = {key_value} is {len(data_list)} ")
        print(f"Total records accumulated is {len(data_list)} ")

        if data_list:
            Holman_Upsert_data(data_type, data_key, data_list, primary_key)
            print(f"Data upserted for data type {data_type} with code value {code_value}")
        else:
            print(f"No data fetched")

    dbutils.fs.rm(d_path, True) #deleting the directory after processing the data



get_billing_data()


Py4JJavaError: An error occurred while calling z:org.apache.spark.sql.functions.col.
: java.lang.NullPointerException
	at org.apache.spark.sql.Column$$anonfun$$lessinit$greater$1.apply(Column.scala:165)
	at org.apache.spark.sql.Column$$anonfun$$lessinit$greater$1.apply(Column.scala:163)
	at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(origin.scala:85)
	at org.apache.spark.sql.package$.withOrigin(package.scala:118)
	at org.apache.spark.sql.Column.<init>(Column.scala:163)
	at org.apache.spark.sql.Column$.apply(Column.scala:42)
	at org.apache.spark.sql.functions$.col(functions.scala:115)
	at org.apache.spark.sql.functions.col(functions.scala)
	at sun.reflect.GeneratedMethodAccessor445.invoke(Unknown Source)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:498)
	at py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)
	at py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:397)
	at py4j.Gateway.invoke(Gateway.java:306)
	at py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
	at py4j.commands.CallCommand.execute(CallCommand.java:79)
	at py4j.ClientServerConnection.waitForCommands(ClientServerConnection.java:199)
	at py4j.ClientServerConnection.run(ClientServerConnection.java:119)
	at java.lang.Thread.run(Thread.java:750)
File <command-2690687946717719>, line 64
     58             print(f"No data fetched")
     60     dbutils.fs.rm(d_path, True) #deleting the directory after processing the data
---> 64 get_billing_data()
File /databricks/spark/python/lib/py4j-0.10.9.7-src.zip/py4j/protocol.py:326, in get_return_value(answer, gateway_client, target_id, name)
    324 value = OUTPUT_CONVERTER[type](answer[2:], gateway_client)
    325 if answer[1] == REFERENCE_TYPE:
--> 326     raise Py4JJavaError(
    327         "An error occurred while calling {0}{1}{2}.\n".
    328         format(target_id, ".", name), value)
    329 else:
    330     raise Py4JError(
    331         "An error occurred while calling {0}{1}{2}. Trace:\n{3}\n".
    332         format(target_id, ".", name, value))    
