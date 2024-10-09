def fetch_fuels_code_data():

#   endpoint=["fuels"]
    transDateCode = [1,2,3]
    fuel_data= []

    for code in transDateCode:
        print(f"Fetching data from endpoint transDateCode={code}")
        page = 1
        while True:
            print(f"Fetching page {page} of data from endpoint transDateCode={code}")
            endpoint = f"fuels?transDateCode={code}&pageNumber={page}"
            response, response_data = get_holman_api_response(token, endpoint)
            # print(response_data)
            if 'us' not in response_data or not response_data['us']:
                print(f"No more records on page {page}, Stopping Pagination")
                break
            fuel_data.extend(response_data['us'])

            if int(response_data['pageNumber']) >= int(response_data['totalPages']):
                break           
            page +=1

fuel_data = fetch_endpoint_code_data()

def fetch_billing_code_data():

#   endpoint=["fuels"]
    billingTypeCode = [1,2,]
    fuel_data= []

    for code in billingTypeCode:
        print(f"Fetching data from endpoint transDateCode={code}")
        page = 1
        while True:
            print(f"Fetching page {page} of data from endpoint transDateCode={code}")
            endpoint = f"billing?billingTypeCode={code}&pageNumber={page}"
            response, response_data = get_holman_api_response(token, endpoint)
            # print(response_data)
            if 'billing' not in response_data or not response_data['billing']:
                print(f"No more records on page {page}, Stopping Pagination")
                break
            fuel_data.extend(response_data['billing'])

            if int(response_data['pageNumber']) >= int(response_data['totalPages']):
                break           
            page +=1

fuel_data = fetch_billing_code_data()

def fetch_violation_code_data():

#   endpoint=["fuels"]
    violationDateCode = [1,2,3]
    violation_data= []

    for code in violationDateCode:
        print(f"Fetching data from endpoint transDateCode={code}")
        page = 1
        while True:
            print(f"Fetching page {page} of data from endpoint transDateCode={code}")
            endpoint = f"violation?violationDateCode={code}&pageNumber={page}"
            response, response_data = get_holman_api_response(token, endpoint)
            # print(response_data)
            if 'violations' not in response_data or not response_data['violations']:
                print(f"No more records on page {page}, Stopping Pagination")
                break
            violation_data.extend(response_data['violations'])

            if int(response_data['pageNumber']) >= int(response_data['totalPages']):
                break           
            page +=1
    return violation_data

violation_data = fetch_violation_code_data()
