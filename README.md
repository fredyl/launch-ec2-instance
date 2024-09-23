def behaviors_api_response():
    endpoint = "/safety/events/behaviors"
    response_data = lytx_get_repoonse_from_event_api(endpoint)
    return response_data
    print(response_data)

behaviors_api_response()

def triggersubtypes_api_response():
    endpoint = "/safety/events/triggersubtypes"
    response_data = lytx_get_repoonse_from_event_api(endpoint)
    return response_data
    print(response_data)

triggersubtypes_api_response()

def triggers_api_response():
    endpoint = "/safety/events/triggers"
    response_data = lytx_get_repoonse_from_event_api(endpoint)
    return response_data
    print(response_data)
triggers_api_response()
