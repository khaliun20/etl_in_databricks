# Databricks notebook source
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql.functions import count, desc, col


import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql.functions import col

df = spark.table("argentina_soccer_result")
home_matches = df.filter((col("home_team") == 'Argentina'))
total_home_matches = home_matches.count()
home_wins = home_matches.filter(col("winner") == 'home').count()
home_losses = total_home_matches - home_wins
labels = ['Home Wins', 'Home Losses']
counts = [home_wins, home_losses]
fig, ax = plt.subplots()
bars = plt.bar(labels, counts, color=['green', 'orange'])
plt.xlabel('Outcome')
plt.ylabel('Number of Matches')
plt.title('Argentina Home Wins and Losses')
for bar in bars:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2, yval, round(yval), ha='center', va='bottom')

plt.show()


# COMMAND ----------


df = spark.table("argentina_soccer_result")
away_matches = df.filter((col("away_team") == 'Argentina'))
total_away_matches = away_matches.count()
away_wins = away_matches.filter(col("winner") == 'away').count()
away_losses = total_away_matches - away_wins
labels = ['Away Wins', 'Away Losses']
counts = [away_wins, away_losses]
fig, ax = plt.subplots()
bars = plt.bar(labels, counts, color=['green', 'orange'])
plt.xlabel('Outcome')
plt.ylabel('Number of Matches')
plt.title('Argentina Away Wins and Losses')
for bar in bars:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2, yval, round(yval), ha='center', va='bottom')

plt.show()


# COMMAND ----------

df = spark.table("argentina_soccer_result")
home_losses = df.filter((col("home_team") == 'Argentina') & (col("winner") == 'away'))
most_home_losses = home_losses.groupBy("away_team").agg(count("*").alias("losses")).orderBy(desc("losses")).limit(5)
most_home_losses = most_home_losses.withColumnRenamed("away_team", "Argentina lost as home team")
most_home_losses.show()
away_losses = df.filter((col("away_team") == 'Argentina') & (col("winner") == 'home'))
most_away_losses = away_losses.groupBy("home_team").agg(count("*").alias("losses")).orderBy(desc("losses")).limit(5)
most_away_losses = most_away_losses.withColumnRenamed("home_team", "Argentina lost as away team")
most_away_losses.show()


