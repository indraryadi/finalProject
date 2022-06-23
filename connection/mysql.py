from sqlalchemy import create_engine
import json
# import pymysql 

class MySQL:
    
    def __init__(self,cfg):
        self.host=cfg["host"]
        self.port=cfg["port"]
        self.database=cfg["database"]
        self.username=cfg["username"]
        self.password=cfg["password"]
        
    def conn(self):
        try:
            engine= create_engine('mysql+pymysql://{}:{}@{}:{}/{}'.\
                format(self.username,self.password,self.host,self.port,self.database))
            engine_conn=engine.connect()
            print("Connect Engine MySQL")
            return engine, engine_conn    
        except(Exception) as e:
            print("Cannot Connect Engine MySQL!!!")
            return e

if __name__=='__main__':
    with open('/home/hadoop/Documents/finalProject/credentials.json','r') as d:
        data=json.load(d)
    print(data['mysql_lake'])
    mysql_conn=MySQL(data['mysql_lake'])
    print(mysql_conn.conn())
    # engine,conn_engine=mysql_conn.conn()