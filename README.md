
from delta.tables import DeltaTable
from pyspark.sql.functions import col, expr, current_timestamp
import json

def Holman_Upsert_data(data_type, data_key, data_list, primary_key=None):
    
    table_name = f"bronze.holman_{data_type}_{data_key}"
    print(table_name)
    df = spark.createDataFrame(data_list)
    current_time = current_timestamp()
    df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)
    # df.display()
    # dup_check_df = df.groupBy(primary_key).count().filter(col("count") > 1)
    # duplicate_id = [row[primary_key] for row in dup_check_df.collect()]
    # duplicate_rows_df = df.filter(col(primary_key).isin(duplicate_id)).orderBy(desc(primary_key))
    # duplicate_rows_df.show()
    # duplicate_rows = df.join(dup_check_df.select(primary_key), primary_key, how='inner')
    # dup_check_df.show(truncate=False)
    # df_partitioned = df.repartition('id')
    # df_sorted = df_partitioned.sortWithinPartitions('id',desc('lastConnected'))
    # df = df_sorted.dropDuplicates(subset=['id'])
    if spark.catalog.tableExists(table_name):
        print(f"Table {table_name} exists. Performing upsert (merge)...")
        delta_table = DeltaTable.forName(spark, table_name)
        delta_table.alias("existing_data") \
            .merge(
                df.alias("new_data"), expr(f"new_data.{primary_key} = existing_data.{primary_key}")
            ) \
            .whenMatchedUpdate(
                condition=" OR ".join([
                    f"existing_data.{col_name} != new_data.{col_name}" for col_name in df.columns 
                    if  col_name != primary_key and col_name != "tg_inserted"
                ]),  # Set tg_updated to current timestamp and update other columns from new data
                set={ "tg_updated": current_time, 
                    **{col_name: col(f"new_data.{col_name}") for col_name in df.columns if col_name != primary_key and col != "tg_inserted"}}
            ) \
            .whenNotMatchedInsert(
                values={**{col_name: col(f"new_data.{col_name}") for col_name in df.columns}}
            ) \
            .execute()
    else:
        print(f"Table does not exists, creating new table {table_name}")
        df.write.format("delta").saveAsTable(table_name)



def fetch_Holman_code_data(data_type, code_key, data_key,code):
    data_list = []
    # for code in range(1,3):
    # print(f"Fetching data from endpoint code={code}")
    page = 1
    while True:
        print(f"Fetching page {page} of data from endpoint code={code}")
        endpoint = f"{data_type}?{code_key}={code}&pageNumber={page}"
        response, response_data = get_holman_api_response(token, endpoint)
        if not response_data or data_key not in response_data or not response_data[data_key]:
            print(f"No more records on page {page}, Stopping Pagination")
            break
        if response.status_code == 200:
            fetched_data = response_data[data_key]
            data_list.extend(fetched_data)
        if int(response_data['pageNumber']) >= int(response_data['totalPages']):
            break
        page +=1
    return data_list

# fetch_Holman_code_data(data_type="billing", code_key="billingTypeCode", data_key="billing",code=2)


#define a set of endpoints with corresponding data keys
holman_coded_endpoints =[
    {
        "data_type" : "billins",
        "code_key": "billingTypeCode",
        "data_key": "billing",
        "primary_key": "invoiceNumber"
     },
    # {
    #     "data_type" = "fuels",
    #     "code_key": "transDateCode",
    #     "data_key": "us",
    #     "primary_key": "transDateCode"
    # },
    # {
    #     "data_type" = "violation",
    #     "code_key": "violationDateCode",
    #     "data_key": "violations",
    #     "primary_key": "violationDateCode"
    # }
]
   

#iterate through the endpoints to fetch and upsert data
for endpoint_config in holman_coded_endpoints:
    data_type = endpoint_config["data_type"]
    code_key = endpoint_config["code_key"]
    data_key = endpoint_config["data_key"]
    primary_key = endpoint_config["primary_key"]
    code =2
    # for code in range(1,3):
    data_list = fetch_Holman_code_data(data_type, code_key, data_key,code)
    if data_list:
        Holman_Upsert_data(data_type, data_key, data_list, primary_key)
    else:
            print(f"No data found for {data_type}_{data_key}")




Exception: ('Failed:', 500, 'ORA-20001: Invalid EndPoint\nORA-06512: at "AUO_DIS_API_PROCS.API_COMMON_PKG", line 176\nORA-06512: at line 1 \r\n Oracle.ManagedDataAccess.Client.OracleException (0x80004005): ORA-20001: Invalid EndPoint\nORA-06512: at "AUO_DIS_API_PROCS.API_COMMON_PKG", line 176\nORA-06512: at line 1\r\n   at Enterprise.Data.Utilities.OracleDataAccess.ExecuteNonQuery(DatabaseID dbId, CommandType commandType, String storedProcName, OracleParameter[] oraParams, String userId, String sessionId, String pageDetailsForLog, Boolean isLogTransaction) in D:\\TeamCity\\buildAgent.1\\work\\947ed79e016c873d\\Enterprise.Data\\Utils\\OracleDataAccess.cs:line 244\r\n   at Enterprise.Data.Utilities.OracleDataAccess.ExecuteNonQuery(DatabaseID dbId, CommandType commandType, String storedProcName, OracleParameter[] oraParams) in D:\\TeamCity\\buildAgent.1\\work\\947ed79e016c873d\\Enterprise.Data\\Utils\\OracleDataAccess.cs:line 198\r\n   at Enterprise.Data.Utilities.OracleDataAccess.ExecuteNonQuerySproc(DatabaseID dbId, String storedProcName, OracleParameter[] paramDetails) in D:\\TeamCity\\buildAgent.1\\work\\947ed79e016c873d\\Enterprise.Data\\Utils\\OracleDataAccess.cs:line 164\r\n   at Enterprise.Data.Repositories.LoggingRepository.LogAPIRequest(String customer_api_mgmt_key, String param_json) in D:\\TeamCity\\buildAgent.1\\work\\947ed79e016c873d\\Enterprise.Data\\Repositories\\LoggingRepository.cs:line 38\r\n   at Enterprise.Core.Helpers.RequestResponseLoggingMiddleware.LogRequest(HttpContext context) in D:\\TeamCity\\buildAgent.1\\work\\947ed79e016c873d\\Enterprise.Core\\Helpers\\RequestResponseLoggingMiddleware.cs:line 85\r\n   at Enterprise.Core.Helpers.RequestResponseLoggingMiddleware.LogRequest(HttpContext context) in D:\\TeamCity\\buildAgent.1\\work\\947ed79e016c873d\\Enterprise.Core\\Helpers\\RequestResponseLoggingMiddleware.cs:line 88\r\n   at Enterprise.Core.Helpers.RequestResponseLoggingMiddleware.Invoke(HttpContext context) in D:\\TeamCity\\buildAgent.1\\work\\947ed79e016c873d\\Enterprise.Core\\Helpers\\RequestResponseLoggingMiddleware.cs:line 32\r\n   at Enterprise.Core.Helpers.JwtMiddleware.Invoke(HttpContext context, IUserService userService) in D:\\TeamCity\\buildAgent.1\\work\\947ed79e016c873d\\Enterprise.Core\\Helpers\\JwtMiddleware.cs:line 32\r\n   at Ari.Cx.AspNetCore.Logging.HttpLoggingHeaderMiddleware.InvokeAsync(HttpContext context)\r\n   at Microsoft.AspNetCore.Authorization.AuthorizationMiddleware.Invoke(HttpContext context)\r\n   at Microsoft.AspNetCore.Authentication.AuthenticationMiddleware.Invoke(HttpContext context)\r\n   at Swashbuckle.AspNetCore.SwaggerUI.SwaggerUIMiddleware.Invoke(HttpContext httpContext)\r\n   at Swashbuckle.AspNetCore.Swagger.SwaggerMiddleware.Invoke(HttpContext httpContext, ISwaggerProvider swaggerProvider)\r\n   at Ari.Cx.AspNetCore.Logging.RequestResponseLoggingMiddleware.InvokeAsync(HttpContext context)\r\n   at Microsoft.AspNetCore.Diagnostics.ExceptionHandlerMiddleware.<Invoke>g__Awaited|6_0(ExceptionHandlerMiddleware middleware, HttpContext context, Task task)')
File <command-3094085711592474>, line 32
     30 code =2
     31 # for code in range(1,3):
---> 32 data_list = fetch_Holman_code_data(data_type, code_key, data_key,code)
     33 if data_list:
     34     Holman_Upsert_data(data_type, data_key, data_list, primary_key)
File <command-310280906944989>, line 16, in get_holman_api_response(token, endpoint)
     14     return response, response_data
     15 else:
---> 16     raise Exception("Failed:", response.status_code, response.text)
