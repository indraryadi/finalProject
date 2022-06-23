
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from sqlalchemy import true
from datetime import datetime
import os
import re

def sparkSession():
    try:
        spark=SparkSession.builder.appName("Submitted2").\
                                   config("spark.jars", "file:///home/hadoop/postgresql-42.2.6.jar").\
                                   getOrCreate()
        # print(spark)
        print("SESSION CREATED!!!")
        return spark
    except (Exception) as e:
        print("SESSION NOT CREATED!!!")
        return e

# load from local
def ingest_data(sparkSession,filePath):
    try:
        print("READ DATA FROM SOURCE...")
        path=f"file:///home/hadoop/Documents/finalProject/{filePath}"
        df2=sparkSession.read.option("header",True).option("delimiter",",").csv(path)
        return df2
    except (Exception) as e:
        print("READ DATA FAILED!!!")
        return e
    #save to hdfs
def save_to_hdfs(data,fileName):
    try:       
        print("LOAD TO HDFS...")
        date = datetime.now().strftime("%Y_%m_%d")
        dates=f"{date}"
        path=f"hdfs://localhost:9000/finalProject/{fileName}_{dates}"
        # path=f"hdfs:///finalProject/{fileName}_{dates}"
        data.write.mode("overwrite").option("header",True).csv(path)
        print("LOAD DATA SUCCESS!!!")
    except (Exception) as e:
        print("LOAD DATA FAILED!!!")
        return e

# def load_mul_to_hdfs(sparkSession):
#     try:
#         print("TRY LOOP DIR...")
#         # assign directory
#         cwd=os.getcwd()
#         directory = "file:///home/hadoop/Documents/finalProject/dataset/"
#         # directory='dataset'
#         for name in os.scandir(directory):
#             if name.is_file():
#                 # path=name.path.split('/')
#                 filePath=re.split('.csv|/',name.path)
#                 fileName=filePath[1]
#                 data=ingest_data(sparkSession,name.path)
#                 save_to_hdfs(data,fileName)
#     except (Exception) as e:
#         print("TRY LOOP FAILED!!!")
#         print(e)
#         return e        



#bash command to run on spark 
# ./spark-submit --master yarn --queue dev ~/Documents/ETL_Batch_Processing-COVID19/connection/submittohdfs.py 
if __name__=="__main__":
    spark=sparkSession()
    # load_mul_to_hdfs(spark)
    # print(os.getcwd())
    
    data=ingest_data(spark,'dataset/products.csv')
    data2=ingest_data(spark,'dataset/order_items.csv')
    save_to_hdfs(data,'products')
    save_to_hdfs(data2,'order_items')
    
   
   
   
   
   
   # load from local
# def ingest_data(sparkSession):
#     try:
#         print("READ DATA FROM SOURCE...")
#         df2=sparkSession.read.option("header",True).option("delimiter",",").csv("file:///home/hadoop/Documents/finalProject/dataset/order_items.csv")
#         return df2
#     except (Exception) as e:
#         print("READ DATA FAILED!!!")
#         return e
#     #save to hdfs
# def save_to_hdfs(data):
#     # df.write.mode("overwrite").csv("hdfs:///covid19/raw_data_test.csv")
#     try:       
#         print("LOAD TO HDFS...")
#         date = datetime.now().strftime("%Y_%m_%d")
#         # print(f"filename_{date}")
#         dates=f"{date}"
#         path=f"hdfs://localhost:9000/finalProject/orderItems_{dates}"
#         data.write.mode("overwrite").option("header",True).csv(path)
#         print("LOAD DATA SUCCESS!!!")
#     except (Exception) as e:
#         print("LOAD DATA FAILED!!!")
#         return e
# ===============================