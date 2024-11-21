'''
Getting Billing data from Holman API and processing the data
'''
data_type = "billing"
data_key = "billing"
primary_key = "vehicleNumber"
code_key = "billingTypeCode"
key_code = "invoiceDateCode"
data_list = []

# Retrieve endpoint options to get key values to get most recent data
endpoint_options = update_endpoints(data_type)
if endpoint_options is None:
    print(f"Skipping {data_type} endpoint is not defined")

#Retrieving key values from endpoint options for looping through the data
key_values =[endpoint_options["key_value"]] if "key_value" in endpoint_options else range(1,4)

d_path = f'/Volumes/{env}/bronze_vendor/holman/{data_type}'
dbutils.fs.mkdirs(d_path)

for code_value in range(1,4):
    for key_value in key_values:
    # key_value = 3
        print(f"\nChecking data for {data_type} with code key {code_key} = {code_value} and {key_code}={key_value}")

        # Retrieve total pages and skip if no pages are available
        total_pages = get_total_pages(data_type, code_key, code_value, key_code, key_value)
        if total_pages == 0:
            print(f"No pages available for {data_type} with code key {code_key} = {code_value}. Skipping...")
            continue
        print(f"Total Pages : {total_pages}")

        #Generating pagination urls for the data
        paginated_urls, num_batches = generate_paginated_urls(data_type,data_key, code_key, code_value,total_pages,key_value, key_code)
        print(f"Total URLs : {len(paginated_urls)}")

       
        #creating url datafarmes and batching the urls. 
        #Add part column to partition the pages into groups of 100
        #repartition the df based on num_batches and cache the df for faster processing
        #use udf to fetch the data from the url
        batch_df = spark.createDataFrame(paginated_urls)
        batch_df = batch_df.withColumn("part", floor(col("pageNumber") / 100)).repartition(num_batches, "part")
        result_df = batch_df.withColumn("data", fetch_data_udf(col("url"), col("pageNumber"), lit(data_type), lit(code_value), lit(d_path),lit(key_value))).cache()

        #extract data from result df and append to list
        data_list.extend(convert_holman_data(result_df, data_key))
        
        if data_list:
            #creating dataframe from data_list and doing an upsert
            Holman_Upsert_data(data_type,data_list, primary_key)
            print(f"Data upserted for data type {data_type} with code value {code_value}")
        

dbutils.fs.rm(d_path, True) #deleting the directory after processing the data



def convert_holman_data(result_df, data_key):
    #collect all file paths from result_df and convert to json then dataframe
        file_path = [row.data for row in result_df.select("data").collect()]
        if not file_path:
            print("No file found ... Skipping")
            return[]
        
        #reading the json file
        o_df = spark.read.json(file_path)
        if o_df.rdd.isEmpty():
            print(f"No data found in json")
            return[]

        #get the data from the json dataframe and removing null values and creating a dataframe
        json_data = o_df.select(data_key).toJSON().collect()
        json_data  = [json.loads(row) for row in json_data]
        o_df = spark.createDataFrame([Row(**item) for item in json_data])


        #fetching the data from the json dataframe o_df and converting to list
        data_list = []
        for row in o_df.collect():
            if data_key in row and isinstance(row[data_key], list):
                data_list.extend(row[data_key])
        print(f"Total records accumulated is {len(data_list)} ")
        return data_list



      
