

now= datetime.now()
year = now.strftime("%Y")
month = now.strftime("%m")
day = now.strftime("%d")
viewName = "vw_CTNPF"
sourcePath = f'{base_path}/{viewName}/{viewName}_{yearmonth}.txt'

spark_sql = f'CREATE VOLUME IF NOT EXISTS {env}.bronze_vendor.edhingest2'
spark.sql(spark_sql)

daily_delta_volumes = f'zonea/missiontables/{year}/{month}/{day}'
daily_delta_path = f'/Volumes/{env}/bronze_vendor/edhingest2/{daily_delta_volumes}'
daily_delta_file_path = f'{daily_delta_path}/{viewName}.txt'

base_delta_volumes = f'/zonea/missiontables/{year}'



dbutils.fs.mkdirs(daily_delta_path)
if dbutils.fs.ls(sourcePath):
  dbutils.fs.cp(sourcePath, daily_delta_file_path, recurse=True)



def get_daily_volumes():
    return [row["volume_name"] for row in spark.sql(f"SHOW VOLUMES IN {env}.bronze_vendor").select("volume_name").collect()]
daily_volume_names = get_daily_volumes()

if base_delta_volumes in daily_volume_names:
    spark_sql = f'DROP VOLUME IF EXISTS {env}.bronze_vendor.edhingest2.{base_delta_volumes}'
    spark.sql(spark_sql)
