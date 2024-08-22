class  PowerBIApiData:
    def __init__(self, client_id, client_secret, tenant_id):
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.authority = f'https://login.microsoftonline.com/{tenant_id}'
        self.scope = ['https://analysis.windows.net/powerbi/api/.default']
        self.access_token = self.get_access_token()
        # self.table_name = table_name
        self.spark = SparkSession.builder.appName("ExtractDataSetsIds").getOrCreate()
        self.headers =headers = {
            'Authorization': f'Bearer {self.access_token}'
             }


    def get_access_token(self):
        app = msal.ConfidentialClientApplication(
            self.client_id,
            authority=self.authority,
            client_credential=self.client_secret
        )
        token_response = app.acquire_token_for_client(scopes=self.scope)
        return token_response['access_token']
    
    def get_groups_id(self):
        groups_url = f"https://api.powerbi.com/v1.0/myorg/groups/"
        response = requests.get(groups_url, headers=self.headers)
        
        if response.status_code == 200:
            groups_data = response.json()
            print(json.dumps(groups_data, indent=2))
            group_ids = [group['id'] for group in groups_data['value']]
            return group_ids
        else:
            raise Exception(f"Failed to retrieve groups Status code: {response.status_code}, Response: {response.text}")

    def get_gateways_id(self, group_id):
        groups_url = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/gateways/"
        response = requests.get(groups_url, headers=self.headers)
        
        if response.status_code == 200:
            gateways_data = response.json()
            print(json.dumps(gateways_data, indent=2))
            gateways_ids = [gateway['id'] for gateway in gateways_data['value']]
            return gateways_ids
        else:
            raise Exception(f"Failed to retrieve groups Status code: {response.status_code}, Response: {response.text}")


    def get_datasets(self,group_id):
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/datasets"
        response = requests.get(url, headers=self.headers)

        if response.status_code == 200:
            dataset_data = response.json()
            print(json.dumps(dataset_data, indent=2))
            dataset_ids =[dataset['id'] for dataset in dataset_data['value']]
            return dataset_ids
        else:
            print(f"Failed to retrieve datasets. Status code: {response.status_code}, Response: {response.text}")
            return None
    

    def get_dataflows(self,group_id):
                url = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/dataflows"
                response = requests.get(url, headers=self.headers)

                if response.status_code == 200:
                    dataflow_data = response.json()
                    print(json.dumps(dataflow_data, indent=2))
                    dataflow_ids =[dataflow['objectId'] for dataflow in dataflow_data['value']]
                    return dataflow_ids
                else:
                    print(f"Failed to retrieve datasets. Status code: {response.status_code}, Response: {response.text}")
                    return None
                

    def get_datasources(self, group_id,datasetId):
        datasource_url =f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/datasets/{datasetId}/datasources"
        response = requests.get(datasource_url, headers=self.headers)
        if response.status_code == 200:
            dataseource_data = response.json()
            print(json.dumps(dataseource_data, indent=2))
            return dataseource_data['value']
        else:
            raise Exception(f"Failed to retrieve datasources. Status code: {response.status_code}, Response: {response.text}")

    def get_datasources_dataflow(self, group_id,dataflowId):
        datasource_url =f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/dataflows/{dataflowId}/datasources"
        response = requests.get(datasource_url, headers=self.headers)
        if response.status_code == 200:
            dataseource_data = response.json()
            print(json.dumps(dataseource_data, indent=2))
            return dataseource_data['value']
        else:
            raise Exception(f"Failed to retrieve datasources. Status code: {response.status_code}, Response: {response.text}")



    def get_json_data(self, group_id, api_endpoint):
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/{api_endpoint}"
        response = requests.get(url, headers=self.headers)

        if response.status_code == 200:
            datasets_data = response.json()
            return datasets_data
        else:
            print(f"Failed to retrieve datasets. Status code: {response.status_code}, Response: {response.text}")
            return None
        
    def create_data_frame(self, api_endpoint):
        group_ids = self.get_groups_id()
        combined_df = None
        for group_id in group_ids:
            datasets_json_data = self.get_json_data(group_id, api_endpoint)
            datasets_rdd = spark.sparkContext.parallelize([json.dumps(datasets_json_data['value'])])
            datasets_df = spark.read.json(datasets_rdd)
            
            if combined_df is None:
                combined_df = datasets_df
            else:
                combined_df = combined_df.unionByName(datasets_df, allowMissingColumns=True)
        return combined_df
        # combined_df.show()

    def create_view_and_push_data_to_database(self, api_endpoint, table_name):
        datasets_df = self.create_data_frame(api_endpoint)
        datasets_df.createOrReplaceTempView("dataflow_temp")
        # table_name = self.table_name

        pbi_datasets_exists = spark.sql(f"SHOW TABLES IN dev.bronze LIKE '{table_name}'").count() > 0

        if not pbi_datasets_exists:
            datasets_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(table_name)       
        else:
            #Merge the new data from dataflow_temp to the already existing pbi_dataflows table
            spark.sql(f"""MERGE INTO '{table_name}' AS target
            USING datasets_temp AS source
            ON target.id = source.id 
            WHEN MATCHED THEN
            UPDATE SET *
            WHEN NOT MATCHED THEN
            INSERT * """ )


    def create_data_frame(self, api_endpoint):
        group_ids = self.get_groups_id()
        combined_df = None
        for group_id in group_ids:
            data_json_data = self.get_json_data(group_id, api_endpoint)
            data_rdd = spark.sparkContext.parallelize([json.dumps(data_json_data['value'])])
            data_df = spark.read.json(data_rdd)
            
            if combined_df is None:
                combined_df = data_df
            else:
                combined_df = combined_df.unionByName(data_df, allowMissingColumns=True)
        return combined_df
    
    def create_view_and_push_data_to_database(self, api_endpoint, table_name):
        data_df = self.create_data_frame(api_endpoint)
        data_df.createOrReplaceTempView("temp_table")
        # table_name = self.table_name
        if data_df is None:  # Check if datasets_df is None
            print("Error: No data available to create DataFrame. Skipping table creation/update.")
            return  # Exit the function if no data is available

        pbi_datasets_exists = spark.sql(f"SHOW TABLES IN dev.bronze LIKE '{table_name}'").count() > 0

        if not pbi_datasets_exists:
            data_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(table_name)       
        else:
            #Merge the new data from dataflow_temp to the already existing pbi_dataflows table
            spark.sql(f"""MERGE INTO '{table_name}' AS target
            USING datasets_temp AS source
            ON target.id = source.id 
            WHEN MATCHED THEN
            UPDATE SET *
            WHEN NOT MATCHED THEN
            INSERT * """ )



    def create_or_update_table(self, datasource_data):
        """
        Create or update the Delta table in the database based on its existence.
        """

        # Define the schema
        schema = StructType([
            StructField("datasourceType", StringType(), True),
            StructField("datasourceId", StringType(), True),
            StructField("gatewayId", StringType(), True),
            StructField("server", StringType(), True),
            StructField("database", StringType(), True)
        ])

        # Check if datasource_data is empty
        if not datasource_data:
            print("Debug: No data available to create DataFrame. Creating an empty DataFrame with the schema.")
            datasource_df = self.spark.createDataFrame([], schema)
        else:
            # Flatten the structure before creating DataFrame
            flattened_data = [
                {
                    "datasourceType": d["datasourceType"],
                    "datasourceId": d["datasourceId"],
                    "gatewayId": d["gatewayId"],
                    "server": d["connectionDetails"]["server"],
                    "database": d["connectionDetails"]["database"]
                }
                for d in datasource_data
            ]
            datasource_df = self.spark.createDataFrame(flattened_data, schema)

          #check if table exists
        exists = self.spark.catalog._jcatalog.tableExists(self.table_name)
        print(f"Table '{self.table_name}' {'exists' if exists else 'does not exist'}.")

            #create or update table

        exists = self.spark.catalog._jcatalog.tableExists(self.table_name)
        print(f"Table '{self.table_name}' {'exists' if exists else 'does not exist'}.")

        if exists:
            # Enable schema merging during write operation
            datasource_df.write.option("mergeSchema", "true").mode("append").format('delta').saveAsTable(self.table_name)
            print(f"Table '{self.table_name}' exists and was updated successfully.")
        else:
            # Enable schema merging for the new table
            self.spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
            datasource_df.write.mode('overwrite').format('delta').saveAsTable(self.table_name)
            print(f"Table '{self.table_name}' did not exist and has been created successfully.")

    def dataset_datasource(self, group_id,datasets_id): 
        power_bi_client = PowerBIApiData(client_id, client_secret, tenant_id)

        try:
            group_ids = power_bi_client.get_groups_id()
            if group_ids:
                for group_id in group_ids:
                    datasets_ids = power_bi_client.get_datasets(group_id)
                    print(f"Starting Process group ID: {group_id}")
                    datasets_ids = power_bi_client.get_datasets(group_id)
                    #print the dataset ID,s
                    if datasets_ids:
                        for datasets_id in datasets_ids:
                            #print("Dataset IDS:", datasets_id)
                            try:
                                #get datasource for each datasets_id
                                datasources = power_bi_client.get_datasources(group_id,datasets_id)
                                #Create or update Table
                                power_bi_client.create_or_update_table(datasources)
                            except Exception as e:
                                print(f"Error loading datasource for group ID: {group_id}, dataset ID: {datasets_id}. Error: {str(e)}")
                            #print each datasource
                            #for datasource in datasources:
                                #print(f"Datasource: {datasource}")
                    else:
                        print("No datasets found in group {group_id}")
            else:
                        print("no groups")
        except Exception as e:
            print(f"An error Occured:{str(e)}")


    def dataflow_datasource(self, group_id,datasets_id): 

        power_bi_client = PowerBIApiData(client_id,secret,tenant)
        try:
            group_ids = power_bi_client.get_groups_id()
            if group_ids:
                for group_id in group_ids:
                    dataflow_ids = power_bi_client.get_dataflows(group_id)
                    print(f"Starting Process group ID: {group_id}")
                    #print the dataset ID
                    if dataflow_ids:
                        for dataflow_id in dataflow_ids:
                            #print("Dataset IDS:", dataflowids)
                            try:
                                #get datasource for each dataflow_id
                                datasources = power_bi_client.get_datasources_dataflow(group_id,dataflow_id)
                                #Create or update Table
                                power_bi_client.create_or_update_table(datasources)
                            except Exception as e:
                                print(f"Error loading datasource for group ID: {group_id}, dataflow ID: {dataflow_id}. Error: {str(e)}")
                            #print each datasource
                            #for datasource in datasources:
                                #print(f"Datasource: {datasource}")
                    else:
                        print("No datasets found in group {group_id}")
            else:
                        print("no groups")
        except Exception as e:
            print(f"An error Occured:{str(e)}")


ValueError: Unable to get authority configuration for https://login.microsoftonline.com/[REDACTED]. Authority would typically be in a format of https://login.microsoftonline.com/your_tenant or https://tenant_name.ciamlogin.com or https://tenant_name.b2clogin.com/tenant.onmicrosoft.com/policy.  Also please double check your tenant name or GUID is correct.
File /databricks/python/lib/python3.10/site-packages/msal/authority.py:79, in Authority.__init__(self, authority_url, http_client, validate_authority, instance_discovery, oidc_authority_url)
     78 try:
---> 79     openid_config = tenant_discovery(
     80         tenant_discovery_endpoint,
     81         self._http_client)
     82 except ValueError:
File /databricks/python/lib/python3.10/site-packages/msal/authority.py:95, in Authority.__init__(self, authority_url, http_client, validate_authority, instance_discovery, oidc_authority_url)
     82 except ValueError:
     83     error_message = (
     84         "Unable to get OIDC authority configuration for {url} "
     85         "because its OIDC Discovery endpoint is unavailable at "
   (...)
     93         .format(authority_url)
     94         ) + " Also please double check your tenant name or GUID is correct."
---> 95     raise ValueError(error_message)
     96 logger.debug(
     97     'openid_config("%s") = %s', tenant_discovery_endpoint, openid_config)
     98 self.authorization_endpoint = openid_config['authorization_endpoint']
