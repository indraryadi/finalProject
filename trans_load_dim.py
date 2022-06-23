from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import json
def sparkSession():
    try:
        spark=SparkSession.builder.appName("Submitted2").config("spark.jars", "file:///home/hadoop/postgresql-42.2.6.jar").getOrCreate()
        # print(spark)
        print("SESSION CREATED!!!")
        return spark
    except (Exception) as e:
        print("SESSION NOT CREATED!!!")
        return e
    
# load from hdfs
def loadData(sparkSession,fileName):
    try:
        print("LOAD DATA...")
        path=f"hdfs://localhost:9000/finalProject/{fileName}"
        rawdf=sparkSession.read.format('csv').\
                       option('inferSchema','True').\
                       option('header',True).\
                       option('separator',',').\
                       load(path)
        print("DATA LOADED!!!")
        return rawdf
    except (Exception) as e:
        print("DATA NOT LOADED!!!")
        return e

def dim_product(rawDf):
    try:
        print("TRY CREATE DIM PRODUCT...")
        df=rawDf.select('id','name','category').distinct()
        df=df.withColumnRenamed('id','product_id').\
             withColumnRenamed('name','product_name').\
             withColumnRenamed('category','product_category')
        print("DIM PRODUCT CREATED!!!")
        return df
    except (Exception) as e:
        print("DIM PRODUCT NOT CREATED!!!")
        return e

def dim_status(rawDf):
    try:
        print("TRY CREATE DIM STATUS...")
        df=rawDf.select('status').distinct()
        df=df.withColumnRenamed('status','status_name')
        w= Window.orderBy('status_name')
        newdf=df.withColumn("id",row_number().over(w))
        newdf=newdf.select(['id','status_name'])
        
        print("DIM STATUS CREATED!!!")
        return newdf
    except (Exception) as e:
        print("DIM STATUS NOT CREATED!!!")
        return e

def dim_date(rawDf):
    try:
        print("TRY CREATE DIM DATE...")
        df=rawDf.select('created_at').distinct()
        df=df.withColumnRenamed('created_at','date')

        df=df.withColumn('day',substring('date',1,11))
        df=df.withColumn('month',substring('date',1,7))
        df=df.withColumn('year',substring('date',1,4))
        df=df.withColumn('date',df['day'])
        df=df.select('date','day','month','year')

        print("DIM DATE CREATED!!!")
        return df
    except (Exception) as e:
        print("DIM DATE NOT CREATED!!!")
        return e

def load_to_dwh(df,tb_name):
    # import json
    try:
        print(f"TRY LOAD {tb_name} TO DWH...")
        with open('/home/hadoop/Documents/finalProject/credentials.json','r') as d:
            data=json.load(d)

        db=data['postgresql']['database']
        user=data['postgresql']['username']
        password=data['postgresql']['password']

        mode = "overwrite"
        url = f"jdbc:postgresql://localhost:5432/{db}"
        properties = {"user": user,"password": password,"driver": "org.postgresql.Driver"}

        df.write.jdbc(url=url, table=tb_name, mode=mode, properties=properties)
        # mode = "overwrite"
        # url = "jdbc:postgresql://localhost:5432/covid19"
        # properties = {"user": "postgres","password": "indra24","driver": "org.postgresql.Driver"}
        # df.write.jdbc(url=url, table='dim_province1', mode=mode, properties=properties)
        
        print("LOAD TO DWH SUCESS!!!")
        return "LOAD TO DWH SUCESS!!!"
    except (Exception) as e:
        print("LOAD TO DWH FAILED!!!")
        return e    
    
if __name__=="__main__":
    spark=sparkSession()
    data=loadData(spark,'products_2022_06_23')
    dim_products=dim_product(data)
    # print(dim_products.count())
    # dim_products.show()
    
    dataOrder=loadData(spark,'order_items_2022_06_23')
    dim_stat=dim_status(dataOrder)
    # print(dim_stat.count())
    # dim_stat.show()
    
    
    dim_dates=dim_date(dataOrder)
    # print(dim_dates.count())
    # dim_dates.show()
    # 180172
    
    load00=load_to_dwh(dim_stat,'dim_status')
    print(load00)
    
    load11=load_to_dwh(dim_dates,'dim_date')
    print(load11)
    
    load22=load_to_dwh(dim_products,'dim_product')
    print(load22)
    
    # print(type(data))
    # data.show()