from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql.functions import col,  udf, explode

spark = SparkSession.builder \
    .appName("data cgv") \
    .master("local[*]") \
    .config("spark.executor.memory", "4g")\
    .getOrCreate()

schema_type = StructType([
    StructField("theater", StringType(), True),
    StructField("name_movie", StringType(), True),
    StructField("date", StringType(), True), 
    StructField("times", ArrayType(StringType()), True)
])

path = "../data/cgv_movies.json"
df = spark.read.option("multiLine", True).schema(schema_type).json(path)

def clean_date(date_str):
    date_list = re.findall(r'\d+', date_str)
    day, month = date_list[::-1]

    return (day, month)

clean_date_udf = udf(
                clean_date,
                StructType([
                    StructField("day", StringType(), True),
                    StructField("month", StringType(), True),
]))



def clean_time(times):
    list_time = []
    for time in times:
        list_time.extend(time.split("\n"))
    return list_time

clean_time_udf = udf(
    clean_time,
    ArrayType(StringType())
)

df_final = (
    df.withColumn("date_new", clean_date_udf(col("date")))
      .withColumn("times_clean", clean_time_udf(col("times")))
      .select(
          "theater",
          "name_movie",
          col("date_new.day").alias("day"),
          col("date_new.month").alias("month"),
          "times_clean"
      )
)


df_exploded = df_final.withColumn("time", explode(col("times_clean"))) \
                      .drop("times_clean")  # bỏ cột cũ nếu muốn



db_url = "jdbc:mysql://localhost:3306/movie_db" 


# Lưu DataFrame vào bảng `showtimes`
df.write.jdbc(url=db_url, table="showtimes", mode="append", properties=db_url)
print("✅ Data đã được lưu vào MySQL từ Spark")
