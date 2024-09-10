#sends out email to distrubution group
eh = ErrorHandler(sendEmail = False)
env = spark.sql("SELECT current_catalog()").collect()[0][0]
