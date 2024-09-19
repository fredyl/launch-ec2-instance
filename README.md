        MERGE INTO bronze.pbi_datasets_refreshschedule AS t
        USING new_data AS n
        ON t.days = n.days
        WHEN MATCHED THEN 
        UPDATE SET t.@odata.context = n.@odata.context, t.days = n.days, t.enabled = n.enabled, t.id = n.id, t.localTimeZoneId = n.localTimeZoneId, t.notifyOption = n.notifyOption, t.times = n.times, t.LastModified = n.LastModified
        WHEN NOT MATCHED THEN 
        INSERT *
