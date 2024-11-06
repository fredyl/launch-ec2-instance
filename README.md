api_key = dbutils.secrets.get(scope="Username",key="Holman-API-User")


def get_token():
    url = "https://customer-experience-api.arifleet.com/v1/users/authenticate"
    payload = {
        "Username" : "Holman-API-User",
        "Password" : "Holman-API-Password"
    }
    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    if response.status_code == 200:
        token = response.json().get("token")
        return token
    else:
        print(f"Authentication failed: {response.status_code}, {response.text}")

token = get_token()
