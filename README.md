chk_path = '/Volumes/dev/bronze_vendor/holman/data.txt'


# Generates paginated URLs based on the total number of pages
def generate_paginated_urls(data_type,data_key, code_key, code_value,total_pages,batch_size=200):
    base_url = "https://customer-experience-api.arifleet.com/v1/"
    pagination_urls = []
    num_batches = (total_pages // batch_size) + 1
    print(f" num_batches : {num_batches}")
    for i in range(num_batches):
        for page in range(i * batch_size + 1, min((i + 1) * batch_size + 1, total_pages +1)):
            url = f"{base_url}{data_type}?{code_key}={code_value}&pageNumber={page}"
            pagination_urls.append({"pageNumber": page, "url": url})
            # print(pagination_urls)
    print('len', len(pagination_urls))
    return pagination_urls, num_batches


def fetch_data_from_url(url):
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f"Bearer {token}"
    }
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        return f"Error: {response.status_code}"
    
    chk_path = '/Volumes/dev/bronze_vendor/holman/data.txt'
    with open(chk_path, 'w') as f:
        f.write(response.text)
    return chk_path



holman_coded_endpoints = [
   
    # {"data_type": "fuels", "code_key": "transDateCode", "data_key": "can", "primary_key": "clientVehicleNumber"},
   
    {"data_type": "violation", "code_key": "violationDateCode", "data_key": "violations", "primary_key": "record_id"},
    # {"data_type": "billing", "code_key": "billingTypeCode", "data_key": "billing", "primary_key": "vehicleNumber"},
    #  {"data_type": "fuels", "code_key": "transDateCode", "data_key": "us", "primary_key": "usRecordID"},
]

# Main execution loop for fetching and processing data
for endpoint_config in holman_coded_endpoints:
    data_type = endpoint_config["data_type"]
    code_key = endpoint_config["code_key"]
    data_key = endpoint_config["data_key"]
    primary_key = endpoint_config["primary_key"]

    for code_value in range(1, 4):  # Looping through code_key values 1 to 3
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
        result_df = batch_df.withColumn("data", fetch_data_udf(col("url"))).cache()
        batch_df = spark.read.json(f"{chk_path}")
        file_path = [row.data for row in result_df.select("data").collect()]
        output_df = spark.read.json(file_path)
        display(output_df)
        # cleaned_data =replace_nulls_in_dataframe(batch_df)


pageNumber	totalPages	violations
4	4	[{"ariVehicleNumber":"124344","clientVehicleNumber":"124344","feeBilled":"3","lesseeCode":"0D11","licensePlate":"8GH7210","licenseState":"MD","paidDate":"11/2/2024 6:30:36 PM","plazaID":"ICC/MD200 ","record_id":"56446065","totalAmount":"5.37","vin":"54DC4W1D7RS222649","violationAmountBilled":"2.37","violationCategory":"TOLLS","violationDate":"10/25/2024 12:00:00 AM","violationLocation":"Maryland Transportation A","violationNo":"1129939206","violationState":"MD","violationStatus":"PAID","violationTime":"06:52","violationType":"TOLLS","violationsSequenceNo":"99"},{"ariVehicleNumber":"B15512","clientVehicleNumber":"115512","feeBilled":"0.08","lesseeCode":"0D11","licensePlate":"EGTA48","licenseState":"FL","paidDate":"11/2/2024 6:30:37 PM","plazaID":"I-95 FL 814 EXLN NB MP36","record_id":"56446081","totalAmount":"0.63","vin":"1FTEX1C84FKD13395","violationAmountBilled":"0.55","violationCategory":"TOLLS","violationDate":"10/29/2024 12:00:00 AM","violationLocation":"Florida Turnpike Enterpri","violationNo":"1129941324","violationState":"FL","violationStatus":"PAID","violationTime":"10:29","violationType":"TOLLS","violationsSequenceNo":"99"},{"ariVehicleNumber":"124010","clientVehicleNumber":"124010","feeBilled":"0.08","lesseeCode":"0D11","licensePlate":"TN7584","licenseState":"NC","paidDate":"11/2/2024 6:30:37 PM","plazaID":"540 SB, US1/Veridea to NC55BYP","record_id":"56446086","totalAmount":"0.59","vin":"1FTBF2AA4PED48195","violationAmountBilled":"0.51","violationCategory":"TOLLS","violationDate":"10/29/2024 12:00:00 AM","violationLocation":"North Carolina Turnpike A","violationNo":"1129941541","violationState":"NC","violationStatus":"PAID","violationTime":"09:20","violationType":"TOLLS","violationsSequenceNo":"99"},{"ariVehicleNumber":"124271","clientVehicleNumber":"124271","feeBilled":"0.25","lesseeCode":"0D11","licensePlate":"RRYH31","licenseState":"FL","paidDate":"11/2/2024 6:30:38 PM","plazaID":"MIDPOINT","record_id":"56446284","totalAmount":"1.89","vin":"54DC4W1D7RS210095","violationAmountBilled":"1.64","violationCategory":"TOLLS","violationDate":"10/28/2024 12:00:00 AM","violationLocation":"Florida Turnpike Enterpri","violationNo":"1129943489","violationState":"FL","violationStatus":"PAID","violationTime":"06:51","violationType":"TOLLS","violationsSequenceNo":"99"},
