schema = StructType([
            StructField("capacityId", StringType(), True),
            StructField("defaultDatasetStorageFormat", StringType(), True),
            StructField("id", StringType(), True),
            StructField("isOnDedicatedCapacity", BooleanType(), True),
            StructField("isReadOnly", BooleanType(), True),
            StructField("name", StringType(), True),
            StructField("type", StringType(), True)
        ])
