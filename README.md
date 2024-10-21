 <command-310280906947063>, line 11
      9 table_name = "bronze.lytx_vehicles_vehicle_id"
     10 endpoint = f"/vehicles/all?limit={limit}&page={page}&includeSubgroups=true"
---> 11 all_vehicles = get_lytx_paging_data(endpoint, page, limit)
     12 df = spark.createDataFrame(all_vehicles)
     14 #create dataframe and get current time stamp and add the columns tg_updated and tg_inserted to the dataframe
     15 #drop duplicates if exists and do upsert

File <command-310280906947130>, line 15, in get_lytx_paging_data(endpoint, page, limit)
     13     raise Exception(f"Invalid endpoint: {endpoint}")
     14 response = lytx_get_repoonse_from_event_api(page_endpoint)
---> 15 response_data = response.json()
     16 status_code = response.status_code
     17 if response_data is None or status_code ==204: #response is empty or status code is 204 stop paging

AttributeError: 'tuple' object has no attribute 'json'
