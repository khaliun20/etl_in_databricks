from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when


spark = SparkSession.builder.appName("ind-project").getOrCreate()

delta_table_name = "soccer_result"

df = spark.read.format("delta").table(delta_table_name)

empty_columns = [col_name for col_name 
                in df.columns 
                if df.filter(col(col_name).isNull()).count() > 0]

if not empty_columns:
    print("No columns have empty fields.")
else:
    print("Columns with empty fields:")
    for col_name in empty_columns:
        print(col_name)

delta_table_path = "dbfs:/user/hive/warehouse/soccer_result"

df = df.withColumn(
    "winner",
    when(col("home_score") > col("away_score"), "home")
    .when(col("home_score") < col("away_score"), "away")
    .otherwise("tie")
)

df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(delta_table_path)


