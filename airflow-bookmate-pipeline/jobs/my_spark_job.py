# filename=my_spark_job.py
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import sys

# Создаём Spark-сессию и при необходимости добавляем конфигурации
spark = SparkSession.builder.appName("myAggregateTest").config("fs.s3a.endpoint", "storage.yandexcloud.net").getOrCreate()

# Указываем порт и параметры кластера ClickHouse
jdbcPort = DATA_DELETED
jdbcHostname = "DATA_DELETED"
jdbcDatabase = "DATA_DELETED"
jdbcUrl = f"jdbc:clickhouse://{jdbcHostname}:{jdbcPort}/{jdbcDatabase}?ssl=true"


# Получаем аргумент из Airflow
my_date = sys.argv[1].replace('-', '_')

# Считываем исходные данные за нужную дату
df = spark.read.csv(f"s3a://da-plus-dags/script_bookmate/data_{my_date}/audition_content.csv", inferSchema=True, header=True)

# Строим агрегат по пользователям
result_df = df.groupBy("puid").agg(
    F.countDistinct("audition_id").alias("audition_count"),
    F.avg("hours").alias("avg_hours")
)

result_df.write.format("jdbc") \
    .option("url", jdbcUrl) \
    .option("user", "DATA_DELETED") \
    .option("password", "DATA_DELETED") \
    .option("dbtable", "DATA_DELETED") \
    .mode('append') \
    .save()
