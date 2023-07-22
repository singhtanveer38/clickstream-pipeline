from pyspark.sql import SparkSession

#create spark session
spark = SparkSession.builder \
    .appName("PostgreSQL Connection with PySpark") \
    .config("spark.jars", "/home/tanveer/drivers/postgresql-42.6.0.jar") \
    .getOrCreate()

#postgres details
url = "jdbc:postgresql://localhost:5432/clickstream"

properties = {
    "user": "postgres",
    "password": "password",
    "driver": "org.postgresql.Driver"
}
table_name = "clicks"

#read the table
df = spark.read.jdbc(url, table_name, properties=properties)

#aggregating the data
df.createOrReplaceTempView("aggre")

df1 = spark.sql("select url, country, count(eventid),count(distinct(userid)) from aggre group by url, country")

#writing the data to OVERVIEW table
_ = spark.sql("DROP TABLE IF EXISTS overview")
df1.write.jdbc(url, table="overview", mode="overwrite", properties=properties)
