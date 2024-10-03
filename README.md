Hello black I need a second though on something.  am doing pagination on the below url but the data is not constant. one moment its 5 pages, the next 2 the next 6 . please any idea on why and yes how could it be resolved

endpoint =f"/video/safety/eventsWithMetadata?from=2022-03-16T16:19:58.80Z&to=2024-03-16T16:19:58.80Z&dateOption=lastUpdatedDate&sortDirection=desc&sortBy=lastUpdatedDate&includeSubgroups=true&limit={limit}&page={page}"


{
  "totalCount": 6891,
  "vehicles": [
    {
      "id": 5000050116,
      "groupId": "5100ffff-60b6-e4cd-edc8-60a3e15b0000",
      "name": "7R096",
      "status": 14,
      "isWaking": false,
      "wakeable": false,
      "lastWakeAttempt": "2022-04-12T12:54:12.18Z",
      "lastCommunication": "2023-07-24T19:59:08.35Z",
      "devices": [
        {
          "id": 5000060990,
          "serialNumber": "MV00240938",
          "lastCommunication": "2023-07-24T19:59:08.35Z",
          "onlineStatus": 14,
          "views": [
            {
              "id": 5000116178,
              "name": "FORWARD",
              "label": "Outside"
            },
            {
              "id": 5000116179,
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
            "getsyslogv1",
            "labv1",
            "locationv1",
            "moduleremovev1",
            "moduleupdatev1",
            "performupdatev1",
            "pingv1",
            "propertiesv1",
            "rawcamv1",
            "snapshotv2",
            "statev1",
            "streamdatav1",
            "streamresetv1",
            "streamvideov1",
            "timelinev1",
            "updateavailablev1",
            "videostatev1"
          ],
          "hardwarePlatform": "SF300"
        }
      ],
      "dcVehicleId": "9100ffff-48a9-e563-fa64-60a3e15b0000"
    },
    {
      "id": 5000208979,
      "groupId": "5100ffff-60b6-e4cd-ebbd-60a3e15b0000",
      "name": "7R102",
      "status": 14,
      "isWaking": false,
      "wakeable": false,
      "lastCommunication": "2024-09-11T16:45:18.183Z",
      "devices": [
        {
          "id": 5000216366,
          "serialNumber": "QM40913049",
          "lastCommunication": "2024-09-11T16:45:18.183Z",
          "onlineStatus": 14,
          "views": [
            {
              "id": 5000543136,
              "name": "FORWARD",
              "label": "Outside"
            },
            {
              "id": 5000543137,
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
      "dcVehicleId": "9100ffff-48a9-e863-e2e6-60a3e15b0000"
    },
    {
      "id": 5000055488,
      "groupId": "5100ffff-60b6-e4cd-ec57-60a3e15b0000",
      "name": "7R146",
      "status": 14,
      "isWaking": false,
      "wakeable": false,
      "lastCommunication": "2024-07-10T13:29:52.32Z",
      "devices": [
        {
          "id": 5000066365,
          "serialNumber": "QM00018257",
          "lastCommunication": "2024-07-10T13:29:52.32Z",
          "onlineStatus": 14,
          "views": [
            {
              "id": 5000128043,
              "name": "FORWARD",
              "label": "Outside"
            },
            {
              "id": 5000128044,
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
            "getsyslogv1",
            "labv1",
            "locationv1",
            "moduleremovev1",
            "moduleupdatev1",
            "performupdatev1",
            "pingv1",
            "propertiesv1",
            "rawcamv1",
            "requestversionsv1",
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
          "hardwarePlatform": "SF300V2"
        }
      ],
      "dcVehicleId": "9100ffff-48a9-e663-0509-60a3e15b0000"
    },
    {
      "id": 5000011231,
      "groupId": "5100ffff-60b6-e4cd-ec41-60a3e15b0000",
      "name": "7R147",
      "status": 14,
      "isWaking": false,
      "wakeable": false,
      "lastCommunication": "2022-10-04T15:52:55.913Z",
      "devices": [
        {
          "id": 5000011708,
          "serialNumber": "MV00161921",
          "lastCommunication": "2022-10-04T15:52:55.913Z",
          "onlineStatus": 14,
          "views": [
            {
              "id": 5000026656,
              "name": "FORWARD",
              "label": "Outside"
            },
            {
              "id": 5000026657,
              "name": "REAR",
              "label": "Inside"
            }
          ],
          "capabilities": [],
          "roleId": 1,
          "supportedCommands": [
            "checkinv1",
            "clipdatav1",
            "datadigestv1",
            "datareqv1",
            "gapfillreqv1",
            "lastrecordedv1",
            "locationv1",
            "pingv1",
            "propertiesv1",
            "rawcamv1",
            "snapshotv2",
            "statev1",
            "streamdatav1",
            "streamresetv1",
            "streamvideov1",
            "timelinev1",
            "updateavailablev1",
            "videostatev1"
          ],
          "hardwarePlatform": "SF300"
        }
      ],
      "dcVehicleId": "9100ffff-48a9-e563-40a0-60a3e15b0000"
    },
    {
      "id": 5000153209,
      "groupId": "5100ffff-60b6-e4cd-edc1-60a3e15b0000",
      "name": "7R155",
      "status": 14,
      "isWaking": false,
      "wakeable": false,
      "lastWakeAttempt": "2023-09-11T12:49:46.1Z",
      "lastCommunication": "2024-09-10T17:47:37.87Z",
      "devices": [
        {
          "id": 5000162688,
          "serialNumber": "QM40807762",
          "lastCommunication": "2024-09-10T17:47:37.87Z",
          "onlineStatus": 14,
          "views": [
            {
              "id": 5000355843,
              "name": "FORWARD",
              "label": "Outside"
            },
            {
              "id": 5000355844,
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
      "dcVehicleId": "9100ffff-48a9-e763-d748-60a3e15b0000"
    },
    {
      "id": 5000218254,
      "groupId": "5100ffff-60b6-e4cd-ee06-60a3e15b0000",
      "name": "7R158",
      "status": 14,
      "isWaking": false,
      "wakeable": false,
      "lastCommunication": "2024-05-13T18:26:44.95Z",
      "devices": [
        {
          "id": 5000214902,
          "serialNumber": "QM40919671",
          "lastCommunication": "2024-05-13T18:26:44.95Z",
          "onlineStatus": 14,
          "views": [
            {
              "id": 5000583302,
              "name": "FORWARD",
              "label": "Outside"
            },
            {
              "id": 5000583303,
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
      "dcVehicleId": "9100ffff-48a9-e863-e3e1-60a3e15b0000"
    },
    {
      "id": 5000207474,
      "groupId": "5100ffff-60b6-e4cd-ee06-60a3e15b0000",
      "name": "7R165",
      "status": 14,
      "isWaking": false,
      "wakeable": false,
      "lastCommunication": "2024-08-08T14:13:13.083Z",
      "devices": [
        {
          "id": 5000214873,
          "serialNumber": "QM40919770",
          "lastCommunication": "2024-08-08T14:13:13.083Z",
          "onlineStatus": 14,
          "views": [
            {
              "id": 5000536767,
              "name": "FORWARD",
              "label": "Outside"
            },
            {
              "id": 5000536768,
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
            "getsyslogv1",
            "labv1",
            "locationv1",
            "moduleremovev1",
            "moduleupdatev1",
            "performupdatev1",
            "pingv1",
            "propertiesv1",
            "rawcamv1",
            "requestversionsv1",
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
      "dcVehicleId": "9100ffff-48a9-e863-e3e2-60a3e15b0000"
    },
    {
      "id": 5000209110,
      "groupId": "5100ffff-60b6-e4cd-ec50-60a3e15b0000",
      "name": "7R169",
      "status": 14,
      "isWaking": false,
      "wakeable": false,
      "lastCommunication": "2024-08-16T10:53:41.893Z",
      "devices": [
        {
          "id": 5000216521,
          "serialNumber": "QM40898526",
          "lastCommunication": "2024-08-16T10:53:41.893Z",
          "onlineStatus": 14,
          "views": [
            {
              "id": 5000543614,
              "name": "FORWARD",
              "label": "Outside"
            },
            {
              "id": 5000543615,
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
      "dcVehicleId": "9100ffff-48a9-e863-d0bd-60a3e15b0000"
    },
    {
      "id": 5000207166,
      "groupId": "5100ffff-60b6-e4cd-ee06-60a3e15b0000",
      "name": "7R178",
      "status": 14,
      "isWaking": false,
      "wakeable": false,
      "lastCommunication": "2024-08-12T15:56:17.17Z",
      "devices": [
        {
          "id": 5000214575,
          "serialNumber": "QM40919748",
          "lastCommunication": "2024-08-12T15:56:17.17Z",
          "onlineStatus": 14,
          "views": [
            {
              "id": 5000535358,
              "name": "FORWARD",
              "label": "Outside"
            },
            {
              "id": 5000535359,
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
      "dcVehicleId": "9100ffff-48a9-e863-e3e3-60a3e15b0000"
    },
    {
      "id": 5000209484,
      "groupId": "5100ffff-60b6-e4cd-ee11-60a3e15b0000",
      "name": "7R181",
      "status": 14,
      "isWaking": false,
      "wakeable": false,
      "lastCommunication": "2024-09-27T04:25:11.47Z",
      "devices": [
        {
          "id": 5000216881,
          "serialNumber": "QM40919826",
          "lastCommunication": "2024-09-27T04:25:11.47Z",
          "onlineStatus": 14,
          "views": [
            {
              "id": 5000545283,
              "name": "FORWARD",
              "label": "Outside"
            },
            {
              "id": 5000545284,
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
      "dcVehicleId": "9100ffff-48a9-e863-e031-60a3e15b0000"
    },
    {
      "id": 5000211235,
      "groupId": "5100ffff-60b6-e4cd-ec4f-60a3e15b0000",
      "name": "7R191",
      "status": 14,
      "isWaking": false,
      "wakeable": false,
      "lastCommunication": "2024-08-30T15:00:45.597Z",
      "devices": [
        {
          "id": 5000218640,
          "serialNumber": "QM40919914",
          "lastCommunication": "2024-08-30T15:00:45.597Z",
          "onlineStatus": 14,
          "views": [
            {
              "id": 5000552475,
              "name": "FORWARD",
              "label": "Outside"
            },
            {
              "id": 5000552476,
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
      "dcVehicleId": "9100ffff-48a9-e863-dffb-60a3e15b0000"
    },
    {
      "id": 5000204830,
      "groupId": "5100ffff-60b6-e4cd-ec48-60a3e15b0000",
      "name": "7R197",
      "status": 14,
      "isWaking": false,
      "wakeable": false,
      "lastCommunication": "2024-09-25T11:18:10.187Z",
      "devices": [
        {
          "id": 5000212653,
          "serialNumber": "QM40899348",
          "lastCommunication": "2024-09-25T11:18:10.187Z",
          "onlineStatus": 14,
          "views": [
            {
              "id": 5000515161,
              "name": "FORWARD",
              "label": "Outside"
            },
            {
              "id": 5000515162,
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
      "dcVehicleId": "9100ffff-48a9-e863-d0e0-60a3e15b0000"
    },
    {
      "id": 5000067439,
      "groupId": "5100ffff-60b6-e4cd-eb78-60a3e15b0000",
      "name": "7R203",
      "status": 14,
      "isWaking": false,
      "wakeable": false,
      "lastCommunication": "2023-08-05T12:18:22.31Z",
      "devices": [
        {
          "id": 5000078123,
          "serialNumber": "MV00834898",
          "lastCommunication": "2023-08-05T12:18:22.31Z",
          "onlineStatus": 14,
          "views": [
            {
              "id": 5000155664,
              "name": "FORWARD",
              "label": "Outside"
            },
            {
              "id": 5000155665,
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
            "getsyslogv1",
            "labv1",
            "locationv1",
            "moduleremovev1",
            "moduleupdatev1",
            "performupdatev1",
            "pingv1",
            "propertiesv1",
            "rawcamv1",
            "requestversionsv1",
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
          "hardwarePlatform": "SF300"
        }
      ],
      "dcVehicleId": "9100ffff-48a9-e663-4f82-60a3e15b0000"
    },
    {
      "id": 5000152812,
      "groupId": "5100ffff-60b6-e4cd-eb80-60a3e15b0000",
      "name": "7R210",
      "status": 14,
      "isWaking": false,
      "wakeable": false,
      "lastCommunication": "2024-09-26T20:06:57.497Z",
      "devices": [
        {
          "id": 5000159006,
          "serialNumber": "QM40808229",
          "lastCommunication": "2024-09-26T20:06:57.497Z",
          "onlineStatus": 14,
          "views": [
            {
              "id": 5000354919,
              "name": "FORWARD",
              "label": "Outside"
            },
            {
              "id": 5000354920,
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
      "dcVehicleId": "9100ffff-48a9-e763-d478-60a3e15b0000"
    },
    {
      "id": 5000046461,
      "groupId": "5100ffff-60b6-e4cd-ece5-60a3e15b0000",
      "name": "7S007",
      "status": 14,
      "isWaking": false,
      "wakeable": false,
      "lastCommunication": "2024-09-11T18:22:34.753Z",
      "devices": [
        {
          "id": 5000057014,
          "serialNumber": "MV00233346",
          "lastCommunication": "2024-09-11T18:22:34.753Z",
          "onlineStatus": 14,
          "views": [
            {
              "id": 5000108124,
              "name": "FORWARD",
              "label": "Outside"
            },
            {
              "id": 5000108125,
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
          "hardwarePlatform": "SF300"
        }
      ],
      "dcVehicleId": "9100ffff-48a9-e563-eccc-60a3e15b0000"
    },
    {
      "id": 5000046541,
      "groupId": "5100ffff-60b6-e4cd-ece5-60a3e15b0000",
      "name": "7S019",
      "status": 14,
      "isWaking": false,
      "wakeable": false,
      "lastWakeAttempt": "2022-10-11T15:28:25.697Z",
      "lastCommunication": "2024-06-01T18:14:52.767Z",
      "devices": [
        {
          "id": 5000057095,
          "serialNumber": "MV00233206",
          "lastCom

*** WARNING: max output size exceeded, skipping output. ***

            "datareqv1",
            "gapfillreqv1",
            "lastrecordedv1",
            "locationv1",
            "pingv1",
            "propertiesv1",
            "rawcamv1",
            "snapshotv2",
            "statev1",
            "streamdatav1",
            "streamresetv1",
            "streamvideov1",
            "timelinev1",
            "updateavailablev1",
            "videostatev1"
          ],
          "hardwarePlatform": "SF300"
        }
      ],
      "dcVehicleId": "9100ffff-48a9-e563-fa4e-60a3e15b0000"
    },
    {
      "id": 5000062195,
      "groupId": "5100ffff-60b6-e4cd-ece1-60a3e15b0000",
      "name": "8R044",
      "status": 14,
      "isWaking": false,
      "wakeable": false,
      "lastCommunication": "2023-01-19T15:22:43.47Z",
      "devices": [
        {
          "id": 5000072740,
          "serialNumber": "QM00556160",
          "lastCommunication": "2023-01-19T15:22:43.47Z",
          "onlineStatus": 14,
          "views": [
            {
              "id": 5000143635,
              "name": "FORWARD",
              "label": "Outside"
            },
            {
              "id": 5000143636,
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
            "getsyslogv1",
            "labv1",
            "locationv1",
            "performupdatev1",
            "pingv1",
            "propertiesv1",
            "rawcamv1",
            "snapshotv2",
            "statev1",
            "streamdatav1",
            "streamresetv1",
            "streamvideov1",
            "timelinev1",
            "updateavailablev1",
            "videostatev1"
          ],
          "hardwarePlatform": "SF300V2"
        }
      ],
      "dcVehicleId": "9100ffff-48a9-e663-55ed-60a3e15b0000"
    },
    {
      "id": 5000054789,
      "groupId": "5100ffff-60b6-e4cd-ede8-60a3e15b0000",
      "name": "8R045",
      "status": 14,
      "isWaking": false,
      "wakeable": false,
      "lastCommunication": "2022-09-27T17:31:31.73Z",
      "devices": [
        {
          "id": 5000065550,
          "serialNumber": "QM00017109",
          "lastCommunication": "2022-09-27T17:31:31.73Z",
          "onlineStatus": 14,
          "views": [
            {
              "id": 5000126514,
              "name": "FORWARD",
              "label": "Outside"
            },
            {
              "id": 5000126515,
              "name": "REAR",
              "label": "Inside"
            }
          ],
          "capabilities": [],
          "roleId": 1,
          "supportedCommands": [
            "checkinv1",
            "clipdatav1",
            "datadigestv1",
            "datareqv1",
            "gapfillreqv1",
            "lastrecordedv1",
            "locationv1",
            "pingv1",
            "propertiesv1",
            "rawcamv1",
            "snapshotv2",
            "statev1",
            "streamdatav1",
            "streamresetv1",
            "streamvideov1",
            "timelinev1",
            "updateavailablev1",
            "videostatev1"
          ],
          "hardwarePlatform": "SF300V2"
        }
      ],
      "dcVehicleId": "9100ffff-48a9-e663-033e-60a3e15b0000"
    },
    {
      "id": 5000060881,
      "groupId": "5100ffff-60b6-e4cd-ec99-60a3e15b0000",
      "name": "8R049",
      "status": 14,
      "isWaking": false,
      "wakeable": false,
      "lastCommunication": "2023-09-05T16:36:29.71Z",
      "devices": [
        {
          "id": 5000071474,
          "serialNumber": "QM00557174",
          "lastCommunication": "2023-09-05T16:36:29.71Z",
          "onlineStatus": 14,
          "views": [
            {
              "id": 5000140610,
              "name": "FORWARD",
              "label": "Outside"
            },
            {
              "id": 5000140611,
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
            "getsyslogv1",
            "labv1",
            "locationv1",
            "performupdatev1",
            "pingv1",
            "propertiesv1",
            "rawcamv1",
            "snapshotv2",
            "statev1",
            "streamdatav1",
            "streamresetv1",
            "streamvideov1",
            "timelinev1",
            "updateavailablev1",
            "videostatev1"
          ],
          "hardwarePlatform": "SF300V2"
        }
      ],
      "dcVehicleId": "9100ffff-48a9-e663-4d3e-60a3e15b0000"
    },
    {
      "id": 5000193279,
      "groupId": "5100ffff-60b6-e5cd-0cd8-60a3e15b0000",
      "name": "92648",
      "status": 14,
      "isWaking": false,
      "wakeable": false,
      "lastCommunication": "2024-09-10T19:45:51.647Z",
      "devices": [
        {
          "id": 5000201384,
          "serialNumber": "QM40855288",
          "lastCommunication": "2024-09-10T19:45:51.647Z",
          "onlineStatus": 14,
          "views": [
            {
              "id": 5000460357,
              "name": "FORWARD",
              "label": "Outside"
            },
            {
              "id": 5000460358,
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
      "dcVehicleId": "9100ffff-48a9-e863-4450-60a3e15b0000"
    },
    {
      "id": 5000193258,
      "groupId": "5100ffff-60b6-e5cd-0cd8-60a3e15b0000",
      "name": "92721",
      "status": 14,
      "isWaking": false,
      "wakeable": false,
      "lastCommunication": "2024-09-23T22:01:57.003Z",
      "devices": [
        {
          "id": 5000201364,
          "serialNumber": "QM40855007",
          "lastCommunication": "2024-09-23T22:01:57.003Z",
          "onlineStatus": 14,
          "views": [
            {
              "id": 5000460304,
              "name": "FORWARD",
              "label": "Outside"
            },
            {
              "id": 5000460305,
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
      "dcVehicleId": "9100ffff-48a9-e863-4451-60a3e15b0000"
    },
    {
      "id": 5000193311,
      "groupId": "5100ffff-60b6-e5cd-0cd8-60a3e15b0000",
      "name": "93549",
      "status": 14,
      "isWaking": false,
      "wakeable": false,
      "lastCommunication": "2024-08-26T23:22:11.29Z",
      "devices": [
        {
          "id": 5000201413,
          "serialNumber": "QM40846472",
          "lastCommunication": "2024-08-26T23:22:11.29Z",
          "onlineStatus": 14,
          "views": [
            {
              "id": 5000460453,
              "name": "FORWARD",
              "label": "Outside"
            },
            {
              "id": 5000460454,
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
      "dcVehicleId": "9100ffff-48a9-e863-4452-60a3e15b0000"
    },
    {
      "id": 5000193333,
      "groupId": "5100ffff-60b6-e5cd-0cd8-60a3e15b0000",
      "name": "94166",
      "status": 14,
      "isWaking": false,
      "wakeable": false,
      "lastCommunication": "2024-09-10T19:50:04.513Z",
      "devices": [
        {
          "id": 5000201427,
          "serialNumber": "QM40855334",
          "lastCommunication": "2024-09-10T19:50:04.513Z",
          "onlineStatus": 14,
          "views": [
            {
              "id": 5000460504,
              "name": "FORWARD",
              "label": "Outside"
            },
            {
              "id": 5000460505,
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
      "dcVehicleId": "9100ffff-48a9-e863-4453-60a3e15b0000"
    },
    {
      "id": 5000175934,
      "groupId": "5100ffff-60b6-e5cd-0cdb-60a3e15b0000",
      "name": "94264",
      "status": 14,
      "isWaking": false,
      "wakeable": false,
      "lastCommunication": "2024-09-11T15:49:38.067Z",
      "devices": [
        {
          "id": 5000183885,
          "serialNumber": "QM40857618",
          "lastCommunication": "2024-09-11T15:49:38.067Z",
          "onlineStatus": 14,
          "views": [
            {
              "id": 5000414424,
              "name": "FORWARD",
              "label": "Outside"
            },
            {
              "id": 5000414425,
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
      "dcVehicleId": "9100ffff-48a9-e863-4471-60a3e15b0000"
    },
    {
      "id": 5000193619,
      "groupId": "5100ffff-60b6-e5cd-0cd8-60a3e15b0000",
      "name": "94407",
      "status": 14,
      "isWaking": false,
      "wakeable": false,
      "lastCommunication": "2024-09-16T05:58:46.987Z",
      "devices": [
        {
          "id": 5000201591,
          "serialNumber": "QM40854882",
          "lastCommunication": "2024-09-16T05:58:46.987Z",
          "onlineStatus": 14,
          "views": [
            {
              "id": 5000461183,
              "name": "FORWARD",
              "label": "Outside"
            },
            {
              "id": 5000461184,
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
      "dcVehicleId": "9100ffff-48a9-e863-4454-60a3e15b0000"
    },
    {
      "id": 5000175781,
      "groupId": "5100ffff-60b6-e5cd-0cdb-60a3e15b0000",
      "name": "94408",
      "status": 14,
      "isWaking": false,
      "wakeable": false,
      "lastCommunication": "2024-09-25T19:44:29.747Z",
      "devices": [
        {
          "id": 5000183727,
          "serialNumber": "QM40857726",
          "lastCommunication": "2024-09-25T19:44:29.747Z",
          "onlineStatus": 14,
          "views": [
            {
              "id": 5000414022,
              "name": "FORWARD",
              "label": "Outside"
            },
            {
              "id": 5000414023,
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
      "dcVehicleId": "9100ffff-48a9-e863-4472-60a3e15b0000"
    },
    {
      "id": 5000175836,
      "groupId": "5100ffff-60b6-e5cd-0cdb-60a3e15b0000",
      "name": "94410",
      "status": 14,
      "isWaking": false,
      "wakeable": false,
      "lastWakeAttempt": "2024-06-26T21:14:31.49Z",
      "lastCommunication": "2024-09-16T15:47:27.9Z",
      "devices": [
        {
          "id": 5000183777,
          "serialNumber": "QM40857381",
          "lastCommunication": "2024-09-16T15:47:27.9Z",
          "onlineStatus": 14,
          "views": [
            {
              "id": 5000414142,
              "name": "FORWARD",
              "label": "Outside"
            },
            {
              "id": 5000414143,
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
      "dcVehicleId": "9100ffff-48a9-e863-4473-60a3e15b0000"
    },
    {
      "id": 5000193314,
      "groupId": "5100ffff-60b6-e5cd-0cd8-60a3e15b0000",
      "name": "94415",
      "status": 14,
      "isWaking": false,
      "wakeable": false,
      "lastCommunication": "2024-09-05T09:25:26.263Z",
      "devices": [
        {
          "id": 5000201415,
          "serialNumber": "QM40846565",
          "lastCommunication": "2024-09-05T09:25:26.263Z",
          "onlineStatus": 14,
          "views": [
            {
              "id": 5000460461,
              "name": "FORWARD",
              "label": "Outside"
            },
            {
              "id": 5000460462,
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
      "dcVehicleId": "9100ffff-48a9-e863-4455-60a3e15b0000"
    },
    {
      "id": 5000242670,
      "groupId": "5100ffff-60b6-e4cd-a462-60a3e15b0000",
      "name": "Bodyswap",
      "status": 14,
      "isWaking": false,
      "wakeable": false,
      "lastCommunication": "2024-09-11T16:57:53.7Z",
      "devices": [
        {
          "id": 5000247284,
          "serialNumber": "QM40109392",
          "lastCommunication": "2024-09-11T16:57:53.7Z",
          "onlineStatus": 14,
          "views": [
            {
              "id": 5000719094,
              "name": "FORWARD",
              "label": "Outside"
            },
            {
              "id": 5000719095,
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
            "getsyslogv1",
            "labv1",
            "locationv1",
            "moduleremovev1",
            "moduleupdatev1",
            "performupdatev1",
            "pingv1",
            "propertiesv1",
            "rawcamv1",
            "requestversionsv1",
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
      "dcVehicleId": "9100ffff-48a9-e963-89ad-60a3e15b0000"
    },
    {
      "id": 5000250098,
      "groupId": "5100ffff-60b6-e4cd-a462-60a3e15b0000",
      "name": "tru_green",
      "status": 14,
      "isWaking": false,
      "wakeable": false,
      "lastCommunication": "2024-09-27T21:27:25.683Z",
      "devices": [
        {
          "id": 5000251718,
          "serialNumber": "QM40962404",
          "lastCommunication": "2024-09-27T21:27:25.683Z",
          "onlineStatus": 14,
          "views": [
            {
              "id": 5000876585,
              "name": "FORWARD",
              "label": "Outside"
            },
            {
              "id": 5000876586,
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
      "dcVehicleId": "ffffcce6-ffff-ffff-0e2d-565c05741069"
    },
    {
      "id": 5000250094,
      "groupId": "5100ffff-60b6-e4cd-a462-60a3e15b0000",
      "name": "tru-green",
      "status": 14,
      "isWaking": false,
      "wakeable": false,
      "lastCommunication": "2024-09-25T19:06:46.757Z",
      "devices": [
        {
          "id": 5000252285,
          "serialNumber": "QM40962265",
          "lastCommunication": "2024-09-25T19:06:46.757Z",
          "onlineStatus": 14,
          "views": [
            {
              "id": 5000876530,
              "name": "FORWARD",
              "label": "Outside"
            },
            {
              "id": 5000876531,
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
      "dcVehicleId": "ffffcce6-ffff-ffff-0e2d-565c056f1069"
    }
  ]
}
