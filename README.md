lets say below is the result from response_data from an APi call how do i select a code and count the number id's amd make sure the follow the paging method above
"id": "4300ffff-6680-6341-2570-60a3e15b0000",
    "customerEventId": "DDPC56081",
    "eventTriggerId": 26,
    "eventTriggerSubTypeId": 1026,
    "eventStatusId": 1,
    "recordDateUTC": "2024-10-02T15:33:49Z",
    "recordDateTZ": "CST ",
    "recordDateUTCOffset": -300,
    "downloadedDate": "2024-10-02T15:34:27Z",
    "score": 0,
    "reviewedDate": null,
    "erSerialNumber": "QM40905404",
    "overDue": 0.0,


def fetch_events_meta_data():
    limit = 1000
    all_events_metadata = []
    page = 1
    while True:
        endpoint = f"/video/safety/eventsWithMetadata?dateOption=recordDate&sortDirection=desc&sortBy=recordDate&includeSubgroups=true&limit={limit}&page={page}"
        response_data = lytx_get_repoonse_from_event_api(endpoint)
        print(json.dumps(response_data,indent=2))
        print(f"getting data for page:{page}")
        all_events_metadata.extend(response_data)
        if len(response_data) < limit:
        # if not response_data:
            break
        page += 1

    return all_events_metadata
