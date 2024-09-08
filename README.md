def get_Item_ids_for_all_groups(access_token, group_ids, api_name, id_key =None, return_type ="json"):
    results = {}
    for group_id in group_ids:
        endpoint = f"groups/{group_id}/{api_name}"
        try:
            response_data = call_powerbi_api(access_token, endpoint)
            if isinstance(response_data, dict):
                items = response_data.get('value', [])
            elif isinstance(response_data, list):
                items = {'value': response_data}
            else:
                raise Exception(f"unexpected response format for group {group_id}")
        
            if return_type == "ids":
                if id_key is not None:
                    raise ValueError(f"id_key must be specified for return_type 'ids'")
                results[group_id] = [item.get(id_key) for item in response_data if item.get(id_key) is not None] 
                    # return [group_id for group_id, items in results.items() if items is not None]
            elif return_type == "json":
                results[group_id] = items
                        # results[group_id] = [ item for item in response_data.get("value") if item is not None]
                return results
            else:
                raise Exception(f"Invalid return type . Use 'ids' or 'json'")
        except Exception as e:
            raise Exception(f"unexpected response format for group {group_id}: {e}")

    return results


calling api for groups/e26b065e-851e-4fc4-95da-4600e0f52423/datasets/{'id': '38a7bb90-949b-41aa-ab2b-a5ef58482fb0', 'name': 'Microsoft Project Web App', 'webUrl': 'https://app.powerbi.com/groups/e26b065e-851e-4fc4-95da-4600e0f52423/datasets/38a7bb90-949b-41aa-ab2b-a5ef58482fb0', 'addRowsAPIEnabled': False, 'configuredBy': 'DBernard@Trugreenmail.Com', 'isRefreshable': True, 'isEffectiveIdentityRequired': False, 'isEffectiveIdentityRolesRequired': False, 'isOnPremGatewayRequired': True, 'targetStorageMode': 'Abf', 'createdDate': '2019-09-18T13:46:28.463Z', 'createReportEmbedURL': 'https://app.powerbi.com/reportEmbed?config=eyJjbHVzdGVyVXJsIjoiaHR0cHM6Ly9XQUJJLVVTLU5PUlRILUNFTlRSQUwtRy1QUklNQVJZLXJlZGlyZWN0LmFuYWx5c2lzLndpbmRvd3MubmV0IiwiZW1iZWRGZWF0dXJlcyI6eyJ1c2FnZU1ldHJpY3NWTmV4dCI6dHJ1ZX19', 'qnaEmbedURL': 'https://app.powerbi.com/qnaEmbed?config=eyJjbHVzdGVyVXJsIjoiaHR0cHM6Ly9XQUJJLVVTLU5PUlRILUNFTlRSQUwtRy1QUklNQVJZLXJlZGlyZWN0LmFuYWx5c2lzLndpbmRvd3MubmV0IiwiZW1iZWRGZWF0dXJlcyI6eyJ1c2FnZU1ldHJ
