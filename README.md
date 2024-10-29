+--------------------+--------------------+
|                 url|                data|
+--------------------+--------------------+
|https://customer-...|{"pageNumber":"1"...|
+--------------------+--------------------+


fetch_data_udf = udf(lambda url: fetch_data_from_url(url), StringType())
