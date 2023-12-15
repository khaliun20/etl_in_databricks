[![cicd](https://github.com/khaliun20/etl_in_databricks/actions/workflows/python.yml/badge.svg)](https://github.com/khaliun20/etl_in_databricks/actions/workflows/python.yml)

# ETL Pipeline in Databricks

In this project, I gained a bit of Databricks experience by making ETL pipeline in it. I used historical international match results dating back to 1872 as my dataset.

## Key Components

* Extract:
  Dataset was first accessed from the Databricks dbfs and saved as delta table with the outlines schemas. 
  
* Transform:
  Empty columns were checked and new column called "winner" was added to the table. The column compares the goals scored by home verus away games. If away teams won, I mark it as "away", and if the home team won, I mark it as home. If tief, "tie". 
  
* Load:
  Next, only the Argentina games as home and away team are filtered and added as another delta table called "argentina_soccer". 

## ETL Auto Trigger

![Alt Text](imgs/10.png)



