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
        # json_data  = [(json.loads(row)) for row in json_data]
        o_df = spark.createDataFrame([Row(**item) for item in json_data])


        #fetching the data from the json dataframe o_df and converting to list
        data_list = []
        for row in o_df.collect():
            if data_key in row and isinstance(row[data_key], list):
                data_list.extend(row[data_key])
        print(f"Total records accumulated is {len(data_list)} ")
        return data_list
