when I use the functions statuses_api_response the data is populated as below. but then when trying to create a dataframe while using the function below process_api_data_to_delta i get _corrupt_record.
Why the dataframes comes as corrupted record and how can I fix the issue. since will need to use the dataframe to create a delta table

def statuses_api_response():
    endpoint = "/vehicles/all"   
    response_data = lytx_get_repoonse_from_event_api(endpoint)
    return response_data
    print(response_data)
statuses_api_response()


def process_api_data_to_delta(endpoint, table_name):
    # Fetch the API response
    response_data = lytx_get_repoonse_from_event_api(endpoint)
    
    # Convert JSON response to DataFrame
    if isinstance(response_data, list):
        df = spark.createDataFrame(response_data)  # Handle if the response is a list
    else:
        rdd = spark.sparkContext.parallelize([response_data])
        df = spark.read.json(rdd)
    
    # Add insert_time and update_time columns
    current_time = F.current_timestamp()
    df = df.withColumn("insert_time", current_time).withColumn("update_time", current_time)
    df.display()

    

'vehicles': [{'id': '9100fffa3e15b0000',
   'name': '107763',
   'type': 23,
   'status': 2,
   'licensePlateNumber': '3167',
   'countrySubdivision': 'US-IL',
   'lastConnected': '2022-03-25T17:58:33.2549447Z',
   'driverId': '000000000000',
   'groupId': '5100f5b0000',
   'vin': '1FTMF1CW9AKC35573',
   'seatbeltType': 1,
   'make': 'FORD',
   'model': 'F-150',
   'year': 2010,
   'deviceId': '3500f0a3e15b0000'},
  {'id': '9100ffff-48e15b0000',
   'name': '204633',
   'type': 23,
   'status': 1,
   'licensePlateNumber': None,
   'countrySubdivision': None,
   'lastConnected': '2022-03-30T21:43:48.222166Z',
   'driverId': '000451d_et5300000',
   'groupId': '5100f3e15b0000',
   'vin': '1GCHC24U76E198459',
   'seatbeltType': 1,
   'make': 'CHEVROLET',
   'model': 'Silverado',
   'year': 2006,
   'deviceId': '00000000-0000-0000-0000-000000000000'},
