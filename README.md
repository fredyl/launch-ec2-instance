def get_access_token(client_id, client_secret,tenant_id, scope):
    # authority = f"https://login.microsoftonline.com/{tenant_id}"
    app = msal.ConfidentialClientApplication(
        client_id,
        authority=f"https://login.microsoftonline.com/{tenant_id}",
        client_credential=client_secret
    )
    token_response = app.acquire_token_for_client(scopes=['https://analysis.windows.net/powerbi/api/.default'])
    return token_response['access_token']

    def call_powerbi_api(access_token, endpoint, params=None):
    url = 'https://api.powerbi.com/v1.0/myorg/'
    url = url + endpoint

    # print(f"calling url")
    headers ={ 'Authorization': f'Bearer {access_token}',
              'Content-Type': 'application/json'
    }
    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        data = response.json()
        # print(json.dumps(data, indent=2))
        return data['value'] if 'value' in data else data
    elif response.status_code == 429:
        retry_after = int(response.headers.get('Retry-After', 8))
        # print("Rate limit exceeded, retrying after 8 seconds")
        time.sleep(retry_after)
    else:
        raise Exception(f"Request failed with status {response.status_code},Response: {response.text}")


        def get_all_groups_ids_1(access_token):
    groups_data = call_powerbi_api(access_token, 'groups')

    #extrac the group id's
    group_ids = [groups_data['id'] for groups_data in groups_data]

    #print the groups id's
    # print("Groups ids:")
    for group_id in group_ids:
        # print(group_id)
        return group_ids


def call_powerbi_api_all_groups(access_token, group_ids, api_endpoint):
    for group_id in group_ids:
        # print(f"calling api_endpoint {api_endpoint} for group {group_id}")
        endpoint=f"groups/{group_id}/{api_endpoint}"
        data = call_powerbi_api(access_token, endpoint)
        # print(json.dumps(data, indent=2)) # print responds in formated way


def get_Item_ids_for_all_groups(access_token, group_ids, api_name, id_key):
    """
    Retrieves all Specific item id's (e.g dataset,dataflow) for each group id
    and creates a dictionatory that maps  group IDs to a list of item IDs ( e.g Dataset and Dataflow).
    """
    all_items_id = {}

    for group_id in group_ids:
        # print(f"fetching {api_name}  IDs for group ID: {group_id}")
        endpoint = f"groups/{group_id}/{api_name}"
        items = call_powerbi_api(access_token, endpoint)

        if not isinstance(items, list):
            raise Exception("API response must be a list, but got type {type(items)} for group id {group_id}")

        #Extract the item IDs and store in a dictionary
        # print(json.dumps(items, indent=2))
        items_ids = [item.get(id_key) for item in items]
        # for item in items:
        #     item_id = item.get('id_key')
        #     if item_id is not None:
        #         items_ids.append(item_id)
        #     else:
        #         print(f"Skipping item with no id for group id {group_id}")

        # [item.get(id_key) for item in items]

        all_items_id[group_id] = items_ids

    
    # print(json.dumps(all_items_id, indent=2))
    return all_items_id


    def get_Item_ids_for_all_groups(access_token, group_ids, api_name, id_key):
    """
    Retrieves all Specific item id's (e.g dataset,dataflow) for each group id
    and creates a dictionatory that maps  group IDs to a list of item IDs ( e.g Dataset and Dataflow).
    """
    all_items_id = {}

    for group_id in group_ids:
        # print(f"fetching {api_name}  IDs for group ID: {group_id}")
        endpoint = f"groups/{group_id}/{api_name}"
        items = call_powerbi_api(access_token, endpoint)

        if not isinstance(items, list):
            raise Exception("API response must be a list, but got type {type(items)} for group id {group_id}")

        #Extract the item IDs and store in a dictionary
        # print(json.dumps(items, indent=2))
        items_ids = [item.get(id_key) for item in items]
        # for item in items:
        #     item_id = item.get('id_key')
        #     if item_id is not None:
        #         items_ids.append(item_id)
        #     else:
        #         print(f"Skipping item with no id for group id {group_id}")

        # [item.get(id_key) for item in items]

        all_items_id[group_id] = items_ids

    
    # print(json.dumps(all_items_id, indent=2))
    return all_items_id

def call_power_bi_api_for_all_data(access_token, items_dict, api_name, item_type):
    """
    the function makes a generic API call for each item in each group.
    """
    
    all_data = []
    skipped_data = []

#Looping through each group ID and its corresponding list of item IDs
    for group_id, item_ids in items_dict.items():
        # print(f"processing group id : {group_id}")
        # print(f"Type of item_ids: {type(item_ids)}")
        if not isinstance(item_ids, list):
            raise Exception(f"API response must be a list, but got type {type(item_ids)}")

        for item_id in item_ids:
            # print(f"Calling API for item ID: {item_id}")
            if not isinstance(item_id, str):
                raise Exception(f"API response must be a list, but got type {type(item_id)}")

            for item_id in item_ids:
                if item_id is None:
                    print(f"Skipping item with no id for group id {group_id}")
                    continue
            # print(f"Calling API for item ID: {item_id}")
            if not isinstance(item_id, str):
                raise Exception(f"API response must be a list, but got type {type(item_id)}")

            
            #Creating the API endpoint for the specific group, item type, and item ID
            endpoint=f"groups/{group_id}/{item_type}/{item_id}/{api_name}"
            # print(F"print api endpoint {endpoint}")


            #Call API and print the response
            try: 
                data = call_powerbi_api(access_token, endpoint)
                if isinstance(data, list):
                    all_data.extend(data)
                elif isinstance(data, dict):
                    all_data.append(data)
            except Exception as e:
                print(f"API call failed with Error: {e}")
                skipped_data.append(item_id)
                continue
        
    if skipped_data:
        print(f"Skipped data for {group_id} : {skipped_data}")

    return all_data

    def get_all_groups_ids(access_token):
    """
    retrieve all power Bi groups IDs (workspaces) for authenticated user
    """
    groups_data = call_powerbi_api(access_token, "groups")
    groups_ids = [group['id'] for group in groups_data]
    return groups_ids
    # print(groups_ids)


    def get_groups_id(access_token):
    groups_url = f"https://api.powerbi.com/v1.0/myorg/groups"
    headers ={ 'Authorization': f'Bearer {access_token}',
        # 'Content-Type': 'application/json'
    }
    response = requests.get(groups_url, headers=headers)
    
    if response.status_code == 200:
        groups_data = response.json()
        # print(json.dumps(groups_data, indent=2))
        group_ids = [group['id'] for group in groups_data['value']]
        # print(group_ids)
        return group_ids
    else:
        raise Exception(f"Failed to retrieve groups Status code: {response.status_code}, Response: {response.text}")

def convert_to_table_structure(data_list):

    if data_list is None or len(data_list) == 0:
        print(f"No data to convert")
        return [], []

    #extract unique keys t form table
    headers = set()
    for entry in data_list:
        headers.update(entry.keys())

    if len(headers) == 0:
        print(f"No headers to convert, data may be invalid")
        return    [], []  
        
    headers = list(headers)
    print(f"Headers {headers}")

    #create table rows
    rows=[]
    for entry in data_list:
        if entry:
            row ={header: entry.get(header, None) for header in headers}
            print(f"Rows created : {row}")
            rows.append(row)
    if len(rows) == 0:
        print(f"No rows created, data may be invalid")
        return    [], []
    
    return headers, rows

    def create_or_update_table_1(headers, rows, table_name,primary_key):
    
    if len(headers) != len(rows[0]):
         raise ValueError(f"The number of headers ({len(headers)}) does not match the number of values in the first row ({len(rows[0])}).")

    schema = StructType([StructField(header, StringType(), True) for header in headers])
    try:
        df = spark.createDataFrame([tuple(row.values()) for row in rows], schema)
    except Exception as e:
        raise Exception(f"Failed to create DataFrame from data: {e}")

    except Exception as e:
        print(f"An error occurred: {str(e)}")
    if spark._jsparkSession.catalog().tableExists(table_name):
        print(f"Table {table_name} exists, updating")

        existing_df = spark.table(table_name)

        df.createOrReplaceTempView('new_data')
        
        merge_condition = " AND ".join([f"t.{col} = n.{col}" for col in primary_key])

        merge_query = f"""
        MERGE INTO {table_name} AS t
        USING new_data AS n
        ON {merge_condition}
        WHEN MATCHED THEN 
        UPDATE SET *
        WHEN NOT MATCHED
         THEN INSERT *
        """
    else: 
        print(f"Table {table_name} does not exist. Creating a new table.")
        df.write.format("delta").saveAsTable(table_name)


def create_dataframe(api_name, group_ids ):
    try: # Retrieve JSON data for all groups 
        full_json_data = get_Item_ids_for_all_group_json(access_token, group_ids, api_name)
        print("Full JSON data:", json.dumps(full_json_data, indent=2))

        flattened_data = []
        for group_id, items in full_json_data.items():
            for item in items:
                item['group_id'] = group_id  
                flattened_data.append(item)

        if flattened_data:

            json_strings = [json.dumps(item) for item in flattened_data]
           
            data_rdd = spark.sparkContext.parallelize(json_strings)

           
            data_df = spark.read.json(data_rdd)
            data_df.show()
            return data_df
        else:
            print("No data available to create a DataFrame.")
            return None

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None

def create_or_update_table_2( data_df, table_name, primary_key): 
    try:
        table_exists = spark._jsparkSession.catalog().tableExists(table_name)

        if table_exists:
            data_df.createOrReplaceTempView("temp_view")

        
            merge_query = f"""
            MERGE INTO {table_name} AS target
            USING temp_view AS source
            ON target.{primary_key} = source.{primary_key}
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *
            """
            spark.sql(merge_query)

        else:
            print(f"Table {table_name} does not exist. Creating a new table.")
            data_df.write.mode("overwrite").saveAsTable(table_name)

    except Exception as e:
        print(f"An error occurred: {str(e)}")


    
