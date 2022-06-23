#!/home/indra/project/ETL-basic/venv/bin python3.10
import json
from unicodedata import category
import pandas as pd
import numpy as np
import sqlalchemy

from connection.mysql import MySQL
def read_credentials():
    with open('/home/hadoop/Documents/finalProject/credentials.json','r') as d:
        data=json.load(d)
    return data

def raw_videos(engine):
    try:    
        # dataset=['order_items.csv','orders.csv','products.csv','users.csv']
        # df=pd.DataFrame(pd.read_csv('/home/hadoop/Documents/finalProject/dataset/{}'.format(dataset[0]),encoding="UTF-8"))
        df=pd.DataFrame(pd.read_csv('/home/hadoop/Documents/finalProject/dataset/orders.csv',encoding="UTF-8"))
        print("Try insert raw videos data into MySQL...")
        df.to_sql(name='raw_orders',con=engine,if_exists="replace",index=False)
        print("Success !!!")
    except (Exception) as e:
        print(e)
        
    engine.dispose()

def insert_raw_data_to_mysql():
    
    cfg=read_credentials()['mysql_lake']
    mysql_auth=MySQL(cfg)
    engine,engine_conn=mysql_auth.conn()
    try:
        raw_videos(engine)
        print("Success !!!")
    except (Exception) as e:
        print(e)        
    
    engine.dispose()
    
if __name__=='__main__':
    insert_raw_data_to_mysql()
    # cfg=read_credentials()['mysql_lake']
    # mysql_auth=MySQL(cfg)
    # engine,engine_conn=mysql_auth.conn()
    # # print(mysql_auth.conn())
    # df=raw_videos(engine)
    # print(type(df))
    # print(df)
    