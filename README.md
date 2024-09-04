import requests, msal, json,time, datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col,cast, lit, when, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType, ArrayType

client_id = "dcb4b38c-e50a-49ec-a5a3-d5300184e4be"
tenant_id = "aadec23e-c40c-47d7-a647-2566ee1c67da"
client_secret = dbutils.secrets.get(scope="BIHubScope",key="databricks-powerbi-connector")
authority = f'https://login.microsoftonline.com/{tenant_id}'
scope = ['https://analysis.windows.net/powerbi/api/.default']
spark = SparkSession.builder.appName("ExtractDataSetsIds").getOrCreate()

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

    max_retries = 3
    attempts = 0
    while attempts < max_retries:

        url = 'https://api.powerbi.com/v1.0/myorg/'
        url = url + endpoint
        headers ={ 'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
        }
        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            data = response.json()
            return data['value'] if 'value' in data else data
        elif response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 8))
            time.sleep(retry_after)
            attempts += 1
        else:
            raise Exception(f"Request failed with status {response.status_code},Response: {response.text}")

def get_all_groups_ids(access_token):
    groups_data = call_powerbi_api(access_token, 'groups')

    #extrac the group id's
    group_ids = [groups_data['id'] for groups_data in groups_data]

    for group_id in group_ids:
        return group_ids


def get_Item_ids_for_all_groups(access_token, group_ids, api_name, id_key):
    """
    Retrieves all Specific item id's (e.g dataset,dataflow) for each group id
    and creates a dictionatory that maps  group IDs to a list of item IDs ( e.g Dataset and Dataflow).
    """
    all_items_id = {}
    
    for group_id in group_ids:
        
        endpoint = f"groups/{group_id}/{api_name}"
        items = call_powerbi_api(access_token, endpoint)

        if not isinstance(items, list):
            raise Exception(f"API response must be a list, but got type {type(items)} for group id {group_id}")

        #Extract the item IDs and store in a dictionary
        items_ids = [item.get(id_key) for item in items]

        all_items_id[group_id] = items_ids

    return all_items_id

    def call_power_bi_api_for_all_data(access_token, data_ids_dict, api_name, item_type, id_field):
    """
    the function makes a generic API call for each item in each group.
    """
    
    all_data = []
    skipped_data = []

#Looping through each group ID and its corresponding list of item IDs
    for group_id, item_ids in data_ids_dict.items():
        if not isinstance(item_ids, list):
            raise Exception(f"API response must be a list, but got type {type(item_ids)}")

        for item_id in item_ids:
            if not isinstance(item_id, str):
                raise Exception(f"API response must be a string, but got type {type(item_id)}")

            if item_id is None:
                print(f"Skipping item with no id for group id {group_id}")
                skipped_data.append(item_id)
                continue
            
            #Creating the API endpoint for the specific group, item type, and item ID
            endpoint=f"groups/{group_id}/{item_type}/{item_id}/{api_name}"

            #Call API and print the response
            try: 
                data = call_powerbi_api(access_token, endpoint)
                if isinstance(data, list):
                    for item in data:
                        item[id_field] = item_id
                    all_data.extend(data)
                elif isinstance(data, dict):
                    data[id_field] = item_id
                    all_data.append(data)
            except Exception as e:
                skipped_data.append(item_id)
                continue

    if skipped_data:
        print(f"Skipped data for {group_id} : {skipped_data}")

    return all_data


def get_Item_ids_for_all_group_json(access_token, group_ids, api_name):

    """
    Getting the json data for the various api_name associated to corresponding group id's
    """

    all_items_json = {}
    for group_id in group_ids:
        endpoint = f"groups/{group_id}/{api_name}" 
        headers = {"Authorization": f"Bearer {access_token}"} 
        response = requests.get(f"https://api.powerbi.com/v1.0/myorg/{endpoint}", headers=headers)

        if response.status_code == 200:
            all_items_json[group_id] = response.json().get('value', [])
        else:
            raise Exception(f"Failed to retrieve data for group {group_id}: {response.status_code}, {response.text}")
    return all_items_json

def convert_to_table_structure(data_list):
    """
    function converts list of dictionaries " data_list" 
    to a table structured format, with headers and rows
    """

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
    print(f"Headers: {headers}")

    #create table rows
    rows=[]
    for entry in data_list:
        if entry:
            row ={header: entry.get(header, None) for header in headers}
            rows.append(row)
    if len(rows) == 0:
        print(f"No rows created, data may be invalid")
        return    [], []
    
    return headers, rows

def create_or_update_table_1(headers, rows, table_name,primary_key):
    """
     creating if tables does not exist or updating the table if table exists 
    from the url https://api.powerbi.com/v1.0/myorg/groups/{group_id}/{api_name}/{name}
    while using data got from convert_to_table_structure
    """


    #checking is there is data in header and rows gotten from convert_to_table_structure
    if len(headers) != len(rows[0]):
         raise ValueError(f"The number of headers ({len(headers)}) does not match the number of values in the first row ({len(rows[0])}).")


    #giving a schema to the header and to the rows
    schema = StructType([StructField(header, StringType(), True) for header in headers])

    #converting rows and rows to a dataframe and creating a delta table
    try:
        df = spark.createDataFrame([tuple(row.values()) for row in rows], schema)
    except Exception as e:
        raise Exception(f"Failed to create DataFrame from data: {e}")

    if spark.catalog.tableExists(table_name):
        print(f"Table {table_name} exists, updating")

        existing_df = spark.table(table_name)

        df.createOrReplaceTempView('new_data')
        
        merge_condition = " AND ".join([f"t.{col} = n.{col}" for col in primary_keys])

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

    

    #creating dataframe data gotten from convert_to_table_structure

    try: # Retrieve JSON data for all groups 
        full_json_data = get_Item_ids_for_all_group_json(access_token, group_ids, api_name)

        flattened_data = []
        for group_id, items in full_json_data.items():
            for item in items:
                item['group_id'] = group_id  
                flattened_data.append(item)

        if flattened_data:

            json_strings = [json.dumps(item) for item in flattened_data]
           
            data_rdd = spark.sparkContext.parallelize(json_strings)

           
            data_df = spark.read.json(data_rdd)
            return data_df
        else:
            print("No data available to create a DataFrame.")
            return None

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None


def create_or_update_table_2( data_df, table_name, primary_keys): 
    try:
        """
        creating dataframe and table from the
         url https://api.powerbi.com/v1.0/myorg/groups/{group_id}/{api_name}

        """
        #creating a dataframe 
        data_df = data_df.dropDuplicates(primary_keys)
        data_df = data_df.withColumn("unique_id", monotonically_increasing_id())
        table_exists = spark.catalog.tableExists(table_name)
        
        #creating delta_table
        if table_exists:
            data_df.createOrReplaceTempView("temp_view")

            merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in primary_keys]) 
            
            merge_query = f"""
            MERGE INTO {table_name} AS target
            USING temp_view AS source
            ON {merge_condition}
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

def create_update_groups_table(access_token, groups_data, primary_keys, table_name, api_name):

    #creating dataframe and delta table from the url https://api.powerbi.com/v1.0/myorg/{api_name}"
    
    try:
        json_strings = [json.dumps(item) for item in groups_data]
        data_rdd = spark.sparkContext.parallelize(json_strings)
        data_df = spark.read.json(data_rdd)  
        table_exists = spark.catalog.tableExists(table_name)
            
        if table_exists:
            data_df.createOrReplaceTempView("temp_view")
            merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in primary_keys]) 
        
            merge_query = f"""
                MERGE INTO {table_name} AS target
                USING temp_view AS source
                ON {merge_condition}
                WHEN MATCHED THEN
                    UPDATE SET *
                WHEN NOT MATCHED THEN
                    INSERT *
                """

            spark.sql(merge_query)

        else:
                print(f"Table {table_name} does not exist. Creating a new table.")
                data_df.write.mode("overwrite").saveAsTable(table_name)
        return data_df

    except Exception as e:
        print(f"An error occurred: {str(e)}")


"""
Processing various sub power bi apis and creating or updating the data to delta table
from url  url https://api.powerbi.com/v1.0/myorg/groups/{group_id}/{api_name}
"""

api_configs = [
    {
        "api_name" : "datasets",
        "table_name" : "dev.bronze.pbi_datasets",
        "primary_keys" :["id"]
    },
    {
        "api_name" : "dataflows",
        "table_name" : "dev.bronze.pbi_dataflows",
        "primary_keys" :["objectId"]
    },
    {
        "api_name" : "reports",
        "table_name" : "dev.bronze.pbi_reports",
        "primary_keys" : ["id"]
    }
]
for config in api_configs:
    api_name = config["api_name"]
    table_name = config["table_name"]
    primary_keys = config["primary_keys"]

    try:
        access_token = get_access_token(client_id, client_secret, tenant_id, scope)
        group_ids = get_all_groups_ids(access_token)
        full_json_data = get_Item_ids_for_all_group_json(access_token, group_ids, api_name, )
        data_df = create_dataframe(api_name, full_json_data)
        create_or_update_table_2( data_df, table_name, primary_keys)
    except Exception as e: 
        print(f"Failed while processing API '{api_name}' for table '{table_name}': {e}")


"""
Processing various sub power bi apis and creating or updating the data to delta table, from
url = https://api.powerbi.com/v1.0/myorg/groups/{group_id}/{api_name}/{name}
"""
api_configs = [
    {
        "primary_keys" : ["datasourceId"],
        "table_name" : "dev.bronze.pbi_dataflows_datasource",
        "id_field" : "objectId",
        "sub_api" : "datasources",
        "api_name": "dataflows"
    },
    {
        "primary_keys" : ["datasourceId"],
        "table_name" : "dev.bronze.pbi_datasets_datasource",
        "id_field" : "id",
        "sub_api" : "datasources",
        "api_name": "datasets"
    },
    {
        "primary_keys" : ["name"],
        "table_name" : "dev.bronze.pbi_reports_page",
        "id_field" : "id",
        "sub_api" : "pages",
        "api_name": "reports"
    },    
]
for config in api_configs:
    api_name = config["api_name"]
    table_name = config["table_name"]
    primary_keys = config["primary_keys"]
    sub_api = config.get("sub_api")
    id_field = config.get("id_field")



    try:
        access_token = get_access_token(client_id, client_secret, tenant_id, scope)
        group_ids = get_all_groups_ids(access_token)
        data_ids_dict = get_Item_ids_for_all_groups(access_token, group_ids, api_name, id_field )
        all_data = call_power_bi_api_for_all_data(access_token, data_ids_dict, sub_api, api_name, id_field)
        headers, rows = convert_to_table_structure(all_data)
        print(headers, rows)
        create_or_update_table_1(headers,rows,table_name,primary_keys)
    except Exception as e: 
        print(f"Failed while processing API '{api_name}/{sub_api}' for table '{table_name}': {e}")



    
