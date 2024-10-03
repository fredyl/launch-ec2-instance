limit = 500
from_date = "2022-09-16T16:19:58.0000Z"
to_date = "2022-10-16T16:19:58.0000Z"
all_events_metadata = []
page = 1

while True:
    endpoint = f"/video/safety/eventsWithMetadata?from={from_date}&to={to_date}&dateOption=lastUpdatedDate&sortDirection=desc&sortBy=lastUpdatedDate&includeSubgroups=true&limit={limit}&page={page}"
    response_data = lytx_get_response_from_event_api(endpoint)  # Corrected function name
    if not response_data:
        print("No more data")
        break
    print(f"getting data for page:{page}")
    all_events_metadata.extend(response_data)
    if len(response_data) < limit:
        break
    page += 1


 {
    "id": "4300ffff-6680-363c-12be-60a3e15b0000",
    "customerEventId": "DBYJ14484",
    "eventTriggerId": 18,
    "eventTriggerSubTypeId": 1018,
    "eventStatusId": 3,
    "recordDateUTC": "2022-11-02T20:34:46Z",
    "recordDateTZ": "CST ",
    "recordDateUTCOffset": -300,
    "downloadedDate": "2022-11-02T21:14:34Z",
    "score": 0,
    "reviewedDate": "2022-11-03T14:20:33Z",
    "erSerialNumber": "QM00557970",
    "overDue": 739161.0,
    "vehicleId": "9100ffff-48a9-e663-46a6-60a3e15b0000",
    "groupId": "5100ffff-60b6-e4cd-ece9-60a3e15b0000",
    "forwardMax": 0.07,
    "lateralMax": 0.1,
    "forwardThreshold": 0.45,
    "lateralThreshold": 0.55,
    "shockThreshold": 0.0,
    "speed": 28.346111111111114,
    "latitude": 27.219882,
    "longitude": -80.400195,
    "heading": 0.3,
    "driverId": "0000ffff-0000-2d00-2a33-4fa5892e0000",
    "coachId": "00000000-0000-0000-0000-000000000000",
    "coachedDate": "9999-01-01T00:00:00Z",
    "creationDate": "2022-11-02T21:14:36.0077842Z",
    "notes": [
      {
        "id": "5f00ffff-5bec-9963-3974-60a3e15b0000",
        "body": "HWW:|The event was triggered due to the Following Distance signal.|",
        "creationDate": "2022-11-03T14:20:33.0635649Z",
        "authorUserId": "0000ffff-0000-2800-2f4d-4fa5892e0000",
        "firstName": "Atharva",
        "lastName": "Londhe"
      }
    ],
    "eventNotes": [],
    "coachingSessionNotes": [],
    "reviewNotes": [
      {
        "id": "5f00ffff-5bec-9963-3974-60a3e15b0000",
        "body": "HWW:|The event was triggered due to the Following Distance signal.|",
        "creationDate": "2022-11-03T14:20:33.0635649Z",
        "authorUserId": "0000ffff-0000-2800-2f4d-4fa5892e0000",
        "firstName": "Atharva",
        "lastName": "Londhe"
      }
    ],
    "behaviors": [],
    "objectRevision": 6,
    "revisionDate": "2022-11-03T14:20:33.1115674Z",
    "coachEmployeeNum": null,
    "coachFirstName": "",
    "coachLastName": "",
    "driverEmployeeNum": "522360",
    "driverFirstName": "Franklin",
    "driverLastName": "Lloyd",
    "coachingOverdueDate": "9999-01-01T00:00:00Z"
  },

