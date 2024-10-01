base_url = "/video/safety/eventsWithMetadata"
params = {
    "from": "2022-10-31T15:04:30.000Z",
    "to": "2024-10-01T15:04:30.777Z",
    "dateOption": "lastUpdatedDate",
    "sortDirection": "desc",
    "sortBy": "lastUpdatedDate",
    "includeSubgroups": "true",
    "limit": limit
}

# Function to make the API call using lytx_get_repoonse_from_event_api
def get_events_from_api(page):
    # Add the page number to the parameters and create the endpoint
    endpoint = f"{base_url}?from={params['from']}&to={params['to']}&dateOption={params['dateOption']}" \
               f"&sortDirection={params['sortDirection']}&sortBy={params['sortBy']}&includeSubgroups={params['includeSubgroups']}" \
               f"&limit={params['limit']}&page={page}"

    # Use the lytx_get_repoonse_from_event_api function to make the request
    response_data = lytx_get_repoonse_from_event_api(endpoint)
    
    return response_data
