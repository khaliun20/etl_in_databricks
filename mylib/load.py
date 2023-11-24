from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import *
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType, StructField, BooleanType

spark = SparkSession.builder.appName("ind-project").getOrCreate()
soccer_df = spark.table("soccer_result")
filtered_df = soccer_df.filter((col("home_team").contains("Argentina")) | (col("away_team").contains("Argentina")))


# Define schema for the argentina_soccer_df DataFrame
schema = StructType([
  StructField("date", StringType(), True),
  StructField("home_team", StringType(), True),
  StructField("away_team", StringType(), True),
  StructField("home_score", IntegerType(), True),
  StructField("away_score", IntegerType(), True),
  StructField("tournament", StringType(), True),
  StructField("city", StringType(), True),
  StructField("country", StringType(), True),
  StructField("neutral", BooleanType(), True),
  StructField("winner", StringType(), True),
])

argentina_soccer_df = filtered_df.select(
  "date",
  "home_team",
  "away_team",
  "home_score",
  "away_score",
  "tournament",
  "city",
  "country",
  "neutral",
  "winner",

)

argentina_soccer_df.show()
new_delta_table_path = "dbfs:/user/hive/warehouse/argentina_soccer_result_delta"
new_delta_table = "argentina_soccer"

argentina_soccer_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").option("schema", schema.json()).saveAsTable(new_delta_table)

