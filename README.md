# terraform-project
terraform-project

self.headers =headers = {
            'Authorization': f'Bearer {self.access_token}'
             }
def get_dataflow_refreshes(self, group_id, dataflow_id):
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/dataflows/{dataflow_id}/refreshes"
        response = requests.get(url, headers=self.headers)

        if response.status_code == 200:
            dataflow_refresh_data = response.json()
            print(json.dumps(dataflow_refresh_data, indent=2))
            # dataset_refresh_ids =[dataset['id'] for dataset in dataset_refresh__data['value']]
            return dataflow_refresh_data ["value"]
        elif response.status_code == 404:
            print(f"Dataflow {dataflow_id} not found in group_id: {group_id}. skipping ...")
            return{}
        else:
            print(f"Failed to retrieve datasets. Status code: {response.status_code}, Response: {response.text}")
            return None
            
def create_or_update_dataflow_refresh_table(self, table_name):
        group_ids = self.get_groups_id()
        for group_id in group_ids:
            dataset_ids = self.get_dataflows_id(group_id)
            
            for dataset_id in dataset_ids:
                refreshes = self.get_dataflow_refreshes(group_id, dataset_id)
                
                if refreshes is not None:
                    for refresh in refreshes:
                        print(refresh)
                        self.create_or_update_dataflow_refreshes_table(table_name, refresh)


Failed to retrieve datasets. Status code: 404, Response: {"Message":"No HTTP resource was found that matches the request URI 'https://wabi-us-north-central-g-primary-redirect.analysis.windows.net/v1.0/myorg/groups/485c3f61-5ecd-4bc1-a96c-cafb637cdb8c/dataflows/19509fa9-c537-4842-9b9c-07b097d183bd/refreshes'."}
