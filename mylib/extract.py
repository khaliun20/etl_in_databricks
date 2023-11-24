from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructType, StructField

spark = SparkSession.builder.appName("ind_project").getOrCreate()

schema = StructType([
    StructField("date", StringType(), True),
    StructField("home_team", StringType(), True),
    StructField("away_team", StringType(), True),
    StructField("home_score", IntegerType(), True),
    StructField("away_score", IntegerType(), True),
    StructField("tournament", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("neutral", StringType(), True),
])

file_path = "dbfs:/FileStore/shared_uploads/km632@duke.edu/result.csv"

df = (spark.read
  .format("csv")
  .schema(schema)
  .option("header", "true") 
  .option("sep", ",")
  .load(file_path)
)

table_name = "soccer_result"
df.write.mode("overwrite").saveAsTable(table_name)

