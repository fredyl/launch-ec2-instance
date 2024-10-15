
def fetch_accident_data(token, endpoint):
    accident_data = []
    limit=200
    page=1
    while True:
        endpoint = f"accidents?pageNumber={page}"
        response, response_data = get_holman_api_response(token, endpoint)
        if response.status_code == 200 and response_data:
            accident = response_data.get("accident", [])
            if len(accident) == 0:
                print(f"No more records on page {page}, Stopping Pagination")
                break
            accident_data.extend(accident)
            print(f"Processing page{page}, with {len(accident)} records")
            page +=1
        else:
            break
    
    return accident_data

fetch_accident_data(token, endpoint ="accidents")


def fetch_accident_data(token, endpoint):
    odometerHistory_data = []
    limit=200
    page=1
    while True:
        endpoint = f"Odometer?pageNumber={page}"
        response, response_data = get_holman_api_response(token, endpoint)
        if response.status_code == 200 and response_data:
            odometerHistory = response_data.get("odometerHistory", [])
            if len(odometerHistory) == 0:
                print(f"No more records on page {page}, Stopping Pagination")
                break
            odometerHistory_data.extend(odometerHistory)
            print(f"Processing page{page}, with {len(odometerHistory)} records")
            page +=1
        else:
            break
    
    return odometerHistory_data

fetch_accident_data(token, endpoint ="Odometer")


def fetch_accident_data(token, endpoint):
    orderHistory_data = []
    limit=200
    page=1
    while True:
        endpoint = f"orders?pageNumber={page}"
        response, response_data = get_holman_api_response(token, endpoint)
        if response.status_code == 200 and response_data:
            orderHistory = response_data.get("orderHistory", [])
            if len(orderHistory) == 0:
                print(f"No more records on page {page}, Stopping Pagination")
                break
            orderHistory_data.extend(orderHistory)
            print(f"Processing page{page}, with {len(orderHistory)} records")
            page +=1
        else:
            break
    
    return orderHistory_data

fetch_accident_data(token, endpoint ="orders")

