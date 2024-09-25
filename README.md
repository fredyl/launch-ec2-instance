![pic1](https://github.com/user-attachments/assets/4758ce09-9ef6-46f8-ac09-f6ffbac6e9c5)
from pyspark.sql import Row
def statuses_api_response(parent_key='', sep= '_'):
    endpoint = "/video/vehicles"   

    response_data = lytx_get_repoonse_from_event_api(endpoint)

    if 'vehicles' not in response_data:
        raise Exception(f"No vehicles key found in response data for endpoint: {endpoint}")
    vehicle_ids= [vehicle['id'] for vehicle in response_data['vehicles']]

    struct_response_data =[]
    all_keys = set()
    for vehicle_id in vehicle_ids:
        endpoint = f"/video/vehicles/{vehicle_id}"
        vehicle_data = lytx_get_repoonse_from_event_api(endpoint)
        vehicle_json_data ={}
        for key, value in vehicle_data.items():
            if isinstance(value, dict) or isinstance(value, list):
                vehicle_json_data[key] =json.dumps(value)
            else:
                vehicle_json_data[key] =value
        all_keys.update(vehicle_json_data.keys())
        # print(json.dumps(response_data,indent=2))

    struct_response_data.append(vehicle_json_data)
    for data in struct_response_data:
        for key in all_keys:
            if key not in data:
                data[key] = None
    
    rows = [Row(**data) for data in struct_response_data]
    if isinstance(struct_response_data, list):
        df = spark.createDataFrame(struct_response_data)
[
  {
    "id": 5000175793,
    "groupId": "5100ffff-6015b0000",
    "name": "01305",
    "status": 16,
    "isWaking": false,
    "wakeable": false,
    "lastCommunication": "2024-09-25T19:35:32.3369544Z",
    "devices": [
      {
        "id": 5000741,
        "serialNumber": "QM4079",
        "lastCommunication": "2024-09-25T19:35:32.3369777Z",
        "onlineStatus": 16,
        "views": [
          {
            "id": 5000052,
            "name": "FORWARD",
            "label": "Outside"
          },
          {
            "id": 5000414053,
            "name": "REAR",
            "label": "Inside"
          }
        ],
        "capabilities": [],
        "roleId": 1,
        "supportedCommands": [
          "checkinv1",
          "clipdatav1",
          "datareqv1",
          "filerequestv1",
          "ftladdv1",
          "ftllistv1",
          "ftlremovev1",
          "getsyslogv1",
          "labv1",
          "locationv1",
          "moduleremovev1",
          "moduleupdatev1",
          "performupdatev1",
          "pingv1",
          "propertiesv1",
          "rawcamv1",
          "requestsettingsreportv1",
          "requestversionsv1",
          "restartcanv1",
          "settingsv1",
          "snapshotv2",
          "statev1",
          "streamdatav1",
          "streamresetv1",
          "streamvideov1",
          "timelinev1",
          "updateavailablev1",
          "videostatev1"
        ],
        "hardwarePlatform": "SF400"
      }
    ],
    "dcVehicleId": "9100ffff-45b0000"
  }
]
