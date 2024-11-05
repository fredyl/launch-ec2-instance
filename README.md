
holman_coded_endpoints = [
   
    # {"data_type": "fuels", "code_key": "transDateCode", "data_key": "can", "primary_key": "clientVehicleNumber"},
   
    {"data_type": "violation", "code_key": "violationDateCode", "data_key": "violations", "primary_key": "record_id"},
    {"data_type": "billing", "code_key": "billingTypeCode", "data_key": "billing", "primary_key": "vehicleNumber"},
     {"data_type": "fuels", "code_key": "transDateCode", "data_key": "us", "primary_key": "usRecordID"},
]

# Main execution loop for fetching and processing data
for endpoint_config in holman_coded_endpoints:
    data_type = endpoint_config["data_type"]
    code_key = endpoint_config["code_key"]
    data_key = endpoint_config["data_key"]
    primary_key = endpoint_config["primary_key"]


    for code_value in range(1, 4):  # Looping through code_key values 1 to 3
    # code_value = 2
        print(f"\nChecking data for {data_type} with code key {code_key} = {code_value}")

        # Retrieve total pages and skip if no pages are available
        total_pages = get_total_pages(data_type, code_key, code_value)
        print(f"Total Pages : {total_pages}")
        
        if total_pages == 0:
            print(f"No pages available for {data_type} with code key {code_key} = {code_value}. Skipping...")
            continue
        
        paginated_urls, num_batches = generate_paginated_urls(data_type,data_key, code_key, code_value,total_pages)
        print(f"Total URLs : {len(paginated_urls)}")
        batch_df = spark.createDataFrame(paginated_urls)
        batch_df = batch_df.withColumn("part", floor(col("pageNumber") / 100)).repartition(num_batches, "part")
        # result_df = batch_df.withColumn("data", fetch_data_udf(col("url"), col("pageNumber"))).cache()
        result_df = batch_df.withColumn("data", fetch_data_udf(col("url"), col("pageNumber"), lit(data_type), lit(code_value))).cache()
        display(result_df)
        
        batch_df = spark.read.json(f"{chk_path}")
        file_path = [row.data for row in result_df.select("data").collect()]
        print(len(file_path))
        output_df = spark.read.json(file_path)


        data_list = []
        for row in output_df.collect():
            if data_key in row and isinstance(row[data_key], list):
                data_list.extend([record.asDict() for record in row[data_key]])
                #  print(json.dumps(data_list, indent=2  ))
            
        if data_list:
        # cleaned_data = replace_null_values(data_list)
            cleaned_data = replace_null_values(data_list)
            # print(json.dumps(cleaned_data, indent=2  ))
            Holman_Upsert_data(data_type, data_key,cleaned_data, primary_key)
        #     print(f"Data upserted for data type {data_type} with code value {code_value}")

        for f in dbutils.fs.ls(chk_path):
            dbutils.fs.rm(f.path, True)
            





pageNumber	url	part	data
1	https://customer-experience-api.arifleet.com/v1/violation?violationDateCode=2&pageNumber=1	0	[{"lesseeCode": "0D11", "ariVehicleNumber": "13489", "clientVehicleNumber": "113489", "vin": "1FTBF3A61DEA70708", "licensePlate": "BW07CB", "licenseState": "FL", "violationState": "FL", "violationNo": "1117255704", "violationsSequenceNo": "99", "violationDate": "9/30/2024 12:00:00 AM", "violationTime": "07:25", "violationAmountBilled": "0.89", "feeBilled": "0.13", "totalAmount": "1.02", "paidDate": "10/5/2024 6:08:38 PM", "violationType": "TOLLS", "plazaID": "INOV 528 (OFF)", "violationLocation": "Florida Turnpike Enterpri", "violationStatus": "PAID", "violationCategory": "TOLLS", "record_id": "55664648"}, {"lesseeCode": "0D11", "ariVehicleNumber": "124467", "clientVehicleNumber": "124467", "vin": "54DC4W1DXRS214934", "licensePlate": "RMMT96", "licenseState": "FL", "violationState": "FL", "violationNo": "1117256378", "violationsSequenceNo": "99", "violationDate": "10/1/2024 12:00:00 AM", "violationTime": "14:54", "violationAmountBilled": "0.46", "feeBilled": "0.07", "totalAmount": "0.53", "paidDate": "10/5/2024 6:08:40 PM", "violationType": "TOLLS", "plazaID": "SR91 BELVEDERE RD MAIN SB MP98", "violationLocation": "Florida Turnpike Enterpri", "violationStatus": "PAID", "violationCategory": "TOLLS", "record_id": "55664675"}, {"lesseeCode": "0D11", "ariVehicleNumber": "13489", "clientVehicleNumber": "113489", "vin": "1FTBF3A61DEA70708", "licensePlate": "BW07CB", "licenseState": "FL", "violationState": "FL", "violationNo": "1117256697", "violationsSequenceNo": "99", "violationDate": "9/30/2024 12:00:00 AM", "violationTime": "07:23", "violationAmountBilled": "3.67", "feeBilled": "0.55", "totalAmount": "4.22", "paidDate": "10/5/2024 6:08:41 PM", "violationType": "TOLLS", "plazaID": "INOV 528 (ON)", "violationLocation": "Florida Turnpike Enterpri", "violationStatus": "PAID", "violationCategory": "TOLLS", "record_id": "55664681"}, {"lesseeCode": "0D11", "ariVehicleNumber": "13489", "clientVehicleNumber": "113489", "vin": "1FTBF3A61DEA70708", "licensePlate": "BW07CB", "licenseState": "FL", 
