# Data Engineering Projects: Stock Prices From VNDIRECT 
## Introduction

In this first project, I will create make an ETL process to get information and analyze prices of stocks. The ETL pipeline will insert new batches of data once a day. 
I’ll create the first ETL pipeline using Airflow to populate Google Cloud Storage and BigQuery with the first large batch of VNDIRECT prices. The second ETL pipeline will be scheduled to run once a day and insert new price into BigQuery.

And the data will be connected with Google Data Studio (Looker Studio) for furthur analysis. 

## Platform Architecture
![de_pipeline](https://github.com/emmanguyen0602/Stock-Price-Data-Engineering/blob/main/image/Stock%20DE%20Project.jpg)

### Pipeline 1:

![pipeline 1](https://github.com/emmanguyen0602/Stock-Price-Data-Engineering/blob/main/image/pipeline%201.png)

### Pipeline 2: 

![pipeline 2](https://github.com/emmanguyen0602/Stock-Price-Data-Engineering/blob/main/image/pipeline%202.png)

### Docker:
![docker](https://github.com/emmanguyen0602/Stock-Price-Data-Engineering/blob/main/image/docker.png)
