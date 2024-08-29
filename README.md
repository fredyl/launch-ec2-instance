Exception: Request failed with status 401,Response: {"error":{"code":"PowerBINotAuthorizedException","pbi.error":{"code":"PowerBINotAuthorizedException","parameters":{},"details":[],"exceptionCulprit":1}}}

def gatways_id(access_token):

    url = f'https://api.powerbi.com/v1.0/myorg/gateways/2efa7c23cb'
    headers ={ 'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
        }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        print(json.dumps(data, indent=2))
    else:
        raise Exception(f"Request failed with status {response.status_code},Response: {response.text}")
