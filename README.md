from datetime import datetime as dt
import requests, msal, json,time, datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col,cast, lit, when, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType, ArrayType

client_id = "dcb4b38c-e50a-49ec-a5a3-d5300184e4be"
tenant_id = "aadec23e-c40c-47d7-a647-2566ee1c67da"
client_secret = dbutils.secrets.get(scope="BIHubScope",key="databricks-powerbi-connector")
authority = f'https://login.microsoftonline.com/{tenant_id}'
scope = ['https://analysis.windows.net/powerbi/api/.default']

def get_access_token(client_id, client_secret,tenant_id, scope):
    # authority = f"https://login.microsoftonline.com/{tenant_id}"
    app = msal.ConfidentialClientApplication(
        client_id,
        authority=f"https://login.microsoftonline.com/{tenant_id}",
        client_credential=client_secret
    )
    token_response = app.acquire_token_for_client(scopes=['https://analysis.windows.net/powerbi/api/.default'])
    return token_response['access_token']

access_token = get_access_token(client_id, client_secret,tenant_id, scope)
base_url = 'https://api.powerbi.com/v1.0/myorg/'
headers= { 'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'



def call_powerbi_api(access_token, endpoint, params=None):

    url = base_url + endpoint
    for _ in range(3):
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            return data['value'] if 'value' in data else data
        elif response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 8))
            time.sleep(retry_after)
        else:
            raise Exception(f"Request failed with status {response.status_code},Response: {response.text}")


def get_all_groups_ids(access_token):

    groups_data = call_powerbi_api(access_token, 'groups')
    group_ids = [groups_data['id'] for groups_data in groups_data]
    return group_ids, groups_data

group_ids = get_all_groups_ids(access_token)


def get_all_object_id_for_each_object_type(access_token, object_type, key_id ):
    """
    Get object ids for each object type in Power BI api
    """

    print("Running get_all_object_id_for_each_object_type")

    group_ids = get_all_groups_ids(access_token)[0]
    powerbi_object_Ids = []
    response_json = []

    for group_id in group_ids:
        url = base_url + f"/groups/{group_id}/{object_type}"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            items = response.json().get('value', [])
            response_json.append(response.json().get('value', []))
            for item in items:
                if key_id in item:
                    powerbi_object_Ids.append(item[key_id])
        else:
            raise Exception(f"Request failed with status {response.status_code},Response: {response.text}")
    
    return response_json, powerbi_object_Ids



def get_object_type_items(access_token, object_type, key_id, sub_api_endpoint, ):
    """
    retrieves items from object_type (e.g. datasets, reports, dataflows) based on the
    object_id's
    """
    group_ids = get_all_groups_ids(access_token)[0]
    object_ids = get_all_object_id_for_each_object_type(access_token, object_type, key_id)[1]

    items_llist = []
    for group_id in group_ids:
        for object_id in object_ids:
            url = base_url + f"/groups/{group_id}/{object_type}/{object_id}/{sub_api_endpoint}"
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                items = response.json().get('value', [])
                for item in items:
                    item[key_id] = object_id
                items_llist.extend(items)
        return items_llist


def get_object_type_items(access_token, object_type, key_id, sub_api_endpoint):
    """
    retrieves items from object_type (e.g. datasets, reports, dataflows) based on the
    object_id's
    """
 
    group_ids = get_all_groups_ids(access_token)[0]
    object_ids = get_all_object_id_for_each_object_type(access_token, object_type, key_id)[1]

    items_llist = []
   
    for group_id in group_ids:
        for object_id in object_ids:
            url = base_url + f"/groups/{group_id}/{object_type}/{object_id}/{sub_api_endpoint}"
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                if sub_api_endpoint == "refreshSchedule":#sub_api_endpoint is refreshSchedule the process since the response is a json object
                    items = response.json()
                    items[key_id] = object_id
                    items_llist.append(items)
                else:
                    items = response.json().get('value', [])
                    for item in items:
                        item[key_id] = object_id
                    items_llist.extend(items)
        return items_llist
    
    


    

def create_dataframe_and_update_or_merge_table(access_token, table_name, primary_key, api_flag=None, object_type=None, key_id=None, sub_api_endpoint=None):

    """
    create Dataframe from json data and update or merge the data to delta table
    api_flag (e.g groups)
    """
    print("Running create_dataframe_and_update_or_merge_table")

    
    timestamp = dt.utcnow().isoformat() #Fetch the current timetamp

    if api_flag is not None:
        json_object = get_all_groups_ids(access_token)[1]
        json_string = json.dumps(json_object)
        json_rdd = spark.sparkContext.parallelize([json_string])
        spark_df = spark.read.option("multiLine", True).json(json_rdd)
        primary_key = spark_df[primary_key]
        
    elif object_type is not None and api_flag is None and sub_api_endpoint is None:
        json_object = get_all_object_id_for_each_object_type(access_token, object_type, key_id)[0]
        json_string = json.dumps(json_object)
        json_rdd = spark.sparkContext.parallelize([json_string])
        spark_df = spark.read.option("multiLine", True).json(json_rdd)
        primary_key = spark_df[primary_key]

    else:
        json_object = get_object_type_items(access_token, object_type, key_id, sub_api_endpoint)
        json_string = json.dumps(json_object)
        json_rdd = spark.sparkContext.parallelize([json_string])
        spark_df = spark.read.option("multiLine", True).json(json_rdd)
        primary_key = spark_df[primary_key]

    all_data_df = spark_df
    spark_df = spark_df.withColumn("LastModified", lit(timestamp))

    if spark.catalog.tableExists(table_name):
        print(f"Table {table_name} exists, updating")

        existing_df = spark.table(table_name)

        all_data_df.createOrReplaceTempView('new_data')
        
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
        spark_df.write.format("delta").saveAsTable(table_name)



#getting data and creating or updataing the table for groups
print("Running groupss Power BI API Call")
create_dataframe_and_update_or_merge_table(access_token, "dev.bronze.pbi_groups", ["id"], api_flag="groups")


#getting data and creating or updataing the table for datasets
print("Running datasets Power BI API Call")
create_dataframe_and_update_or_merge_table(access_token, "dev.bronze.pbi_datasets", ["id"],"datasets")

#getting data and creating or updataing the table for reports
print("Running reports Power BI API Call")
create_dataframe_and_update_or_merge_table(access_token, "dev.bronze.pbi_reports", ["id"],"reports")

print("Running datataflows Power BI API Call")
create_dataframe_and_update_or_merge_table(access_token,"dev.bronze.pbi_dataflow",["objectId"], None, "dataflows", None)


#getting data and creating or updataing the table for dataflows datasources
print("Running dataflows datasources Power BI API Call")
create_dataframe_and_update_or_merge_table(access_token=access_token, table_name="dev.bronze.pbi_dataflows_datasource", primary_key=["datasourceId"],api_flag=None, object_type="dataflows", key_id="objectId", sub_api_endpoint="datasources")


# getting data and creating or updataing the table for datasets datasources
print("Running datasets datasources Power BI API Call")
create_dataframe_and_update_or_merge_table(access_token=access_token, table_name="dev.bronze.pbi_datasets_datasource", primary_key=["datasourceId"],api_flag=None, object_type="datasets", key_id="id", sub_api_endpoint="datasources")


#getting data and creating or updataing the table for reports pages
print("Running reports pages ources Power BI API Call")
create_dataframe_and_update_or_merge_table(access_token=access_token, table_name="dev.bronze.pbi_reports_page", primary_key=["name"],api_flag=None, object_type="reports", key_id="id", sub_api_endpoint="pages")


#getting data and creating or updataing the table for datasets refreshes
print("Running datasets refreshes Power BI API Call")
create_dataframe_and_update_or_merge_table(access_token=access_token, table_name="dev.bronze.pbi_datasets_refreshes", primary_key=["requestId"],api_flag=None, object_type="datasets", key_id="id", sub_api_endpoint="refreshes")


# #getting data and creating or updataing the table for dataflows transactions
print("Running dataflows transactions Power BI API Call")
create_dataframe_and_update_or_merge_table(access_token=access_token, table_name="dev.bronze.pbi_dataflows_transactions", primary_key=["id"],api_flag=None, object_type="dataflows", key_id="objectId", sub_api_endpoint="transactions")


#getting data and creating or updataing the table for datasets refreshschedule
print("Running datasets refreshschedule Power BI API Call")
create_dataframe_and_update_or_merge_table(access_token=access_token, table_name="dev.bronze.pbi_datasets_refreshschedule", primary_key=["days"],api_flag=None, object_type="datasets", key_id="id", sub_api_endpoint="refreshSchedule")


    
