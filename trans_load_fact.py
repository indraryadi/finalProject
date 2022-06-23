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
    
#fact district monthly
def fact_monthly(rawdf,dimstatus):
    
    try:
        print("TRY CREATE FACT TABLE...")
        # df = rawdf.select(['created_at', 'product_id','status','sale_price'])
        df = rawdf
        df=df.withColumn('month',substring('created_at',1,7))
        df=df.withColumn('created_at',df['month'])
        # df=df.groupBy(['created_at','month', 'product_id','status']).count()
        # df=df.withColumnRenamed("count","total_item")
        # df=df.groupBy(['created_at','month', 'product_id','status']).agg(sum(df['sale_price']))
        # df=df.withColumnRenamed("sum(sale_price)","total_sale_price")
        df=df.groupBy(['created_at','month', 'product_id','status']).agg(count('product_id').alias("total_item"),round(sum('sale_price'),2).alias("sum_sale_price"))
        df=df.where(df['status']=="Complete")
        df=df.orderBy(df['product_id'])
        w= Window.orderBy('month')
        newdf=df.withColumn("id",row_number().over(w))
        # # newdf2.show()
        # # print(newdf2.count())
        # #join
        dimstatus=dimstatus.withColumnRenamed("id","status_id")
        # # dimcase.show()
        newdf=newdf.join(dimstatus,newdf['status']==dimstatus['status_name'])
        newdf=newdf.select(["id","product_id","status_id","month","total_item","sum_sale_price"]).orderBy('month')
        # newdf=newdf.withColumnRenamed("kode_kab","district_id").\
        #               withColumnRenamed("tanggal","month").\
        #               withColumnRenamed("sum(count)","total")
        print("FACT TABLE CREATED!!!")
        return newdf
    except (Exception) as e:
        print("FACT TABLE NOT CREATED!!!")
        print(e)
        return e

def load_dim_status(sparkSession,tb_name):
    #load from postgre
    try:
        print(f"TRY LOAD {tb_name} TO TRANSFORM...")
        with open('/home/hadoop/Documents/finalProject/credentials.json','r') as d:
            data=json.load(d)

        db=data['postgresql']['database']
        user=data['postgresql']['username']
        password=data['postgresql']['password']


        url = f"jdbc:postgresql://localhost:5432/{db}"
        jdbcDF = sparkSession.read \
            .format("jdbc") \
            .option("url", url)\
            .option("dbtable", tb_name) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        return jdbcDF
    except (Exception) as e:
        print(f"{tb_name} NOT LOADED!!!")
        return e

#LOAD TO POSTGRESQL
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
        
        print("LOAD TO DWH SUCESS!!!")
        return "LOAD TO DWH SUCESS!!!"
    except (Exception) as e:
        print("LOAD TO DWH FAILED!!!")
        return e
    
if __name__=="__main__":
    spark=sparkSession()
    data=loadData(spark,'order_items_2022_06_23')
    dim_status=load_dim_status(spark,"dim_status")
    fact=fact_monthly(data,dim_status)
    # fact.show()
    load00=load_to_dwh(fact,'fact_product_complete_monthly')
    print(load00)