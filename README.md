def update_endpoints(data_type):
    '''
    endpoint to use in case data type delta table exists
    '''
    #check if table exists
    table_name = f"bronze.holman_{data_type}"

    #if table exists, check for the data type and return the endpoint for delta url and key value or code value. Used to get the most recent data updated
    if spark.catalog.tableExists(table_name):
        if data_type in ["maintenance","violation", "fuels"]:
            return {"code_value": 1}
        elif data_type in ["vehicles", "accidents", "odometer"]:
            return {"delta_url": "delta"}
        elif data_type == "billing":
            return {"key_value": 1}
        elif data_type in ["persons", "orders"]:
            return {"run_all": True}
        else:
            #If table does not exists and data type is given allow data to be loaded, so table will be craated at the upsert level
            return {"run_all": True}
