[
  {
    "@odata.context": "https://wabi-us-north-central-g-primary-redirect.analysis.windows.net/v1.0/myorg/groups/3b1b45fe-32cd-4bbb-afa8-005c505e6f6a/$metadata#datasets",
    "value": [
      {
        "id": "dcdf3e80-5af2-4343-9433-3770e5136d12",
        "name": "Scorecard Report",
        "webUrl": "https://app.powerbi.com/groups/3b1b45fe-32cd-4bbb-afa8-005c505e6f6a/datasets/dcdf3e80-5af2-4343-9433-3770e5136d12",
        "addRowsAPIEnabled": false,
        "configuredBy": "DBernard@Trugreenmail.Com",
        "isRefreshable": true,
        "isEffectiveIdentityRequired": false,
        "isEffectiveIdentityRolesRequired": false,
        "isOnPremGatewayRequired": true,
        "targetStorageMode": "Abf",
        "createdDate": "2019-09-18T13:54:31.827Z",
        "createReportEmbedURL": "https://app.powerbi.com/reportEmbed?config=eyJjbHVzdGVyVXJsIjoiaHR0cHM6Ly9XQUJJLVVTLU5PUlRILUNFTlRSQUwtRy1QUklNQVJZLXJlZGlyZWN0LmFuYWx5c2lzLndpbmRvd3MubmV0IiwiZW1iZWRGZWF0dXJlcyI6eyJ1c2FnZU1ldHJpY3NWTmV4dCI6dHJ1ZX19",
        "qnaEmbedURL": "https://app.powerbi.com/qnaEmbed?config=eyJjbHVzdGVyVXJsIjoiaHR0cHM6Ly9XQUJJLVVTLU5PUlRILUNFTlRSQUwtRy1QUklNQVJZLXJlZGlyZWN0LmFuYWx5c2lzLndpbmRvd3MubmV0IiwiZW1iZWRGZWF0dXJlcyI6eyJ1c2FnZU1ldHJpY3NWTmV4dCI6dHJ1ZX19",
        "upstreamDatasets": [],
        "users": [],
        "queryScaleOutSettings": {
          "autoSyncReadOnlyReplicas": true,
          "maxReadOnlyReplicas": 0
        }
      }
    ]
  },
  {
    "@odata.context": "https://wabi-us-north-central-g-primary-redirect.analysis.windows.net/v1.0/myorg/groups/9b884ac0-d5bb-41a2-9c35-32f2a47e004b/$metadata#datasets",
    "value": [
      {
        "id": "d6d1a397-ea6d-418f-9a6b-0701f55a6c1e",
        "name": "Performance Analyzer Report",
        "webUrl": "https://app.powerbi.com/groups/9b884ac0-d5bb-41a2-9c35-32f2a47e004b/datasets/d6d1a397-ea6d-418f-9a6b-0701f55a6c1e",
        "addRowsAPIEnabled": false,
        "configuredBy": "DBernard@Trugreenmail.Com",
        "isRefreshable": true,
        "isEffectiveIdentityRequired": false,
        "isEffectiveIdentityRolesRequired": false,
        "isOnPremGatewayRequired": true,
        "targetStorageMode": "Abf",
        "createdDate": "2019-09-18T13:56:25.943Z",
        "createReportEmbedURL": "https://app.powerbi.com/reportEmbed?config=eyJjbHVzdGVyVXJsIjoiaHR0cHM6Ly9XQUJJLVVTLU5PUlRILUNFTlRSQUwtRy1QUklNQVJZLXJlZGlyZWN0LmFuYWx5c2lzLndpbmRvd3MubmV0IiwiZW1iZWRGZWF0dXJlcyI6eyJ1c2FnZU1ldHJpY3NWTmV4dCI6dHJ1ZX19",
        "qnaEmbedURL": "https://app.powerbi.com/qnaEmbed?config=eyJjbHVzdGVyVXJsIjoiaHR0cHM6Ly9XQUJJLVVTLU5PUlRILUNFTlRSQUwtRy1QUklNQVJZLXJlZGlyZWN0LmFuYWx5c2lzLndpbmRvd3MubmV0IiwiZW1iZWRGZWF0dXJlcyI6eyJ1c2FnZU1ldHJpY3NWTmV4dCI6dHJ1ZX19",
        "upstreamDatasets": [],
        "users": [],
        "queryScaleOutSettings": {
          "autoSyncReadOnlyReplicas": true,
          "maxReadOnlyReplicas": 0
        }
      }
    ]
  },


   if 'value' in json_df.columns:
        nested_df = json_df.select(explode("value").alias("data"))
        flattened_df = nested_df.select("data.*")
        flattened_df.show(truncate=False)
    else:
        json_df.show(truncate=False)
