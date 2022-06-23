from connection.postgresql import PostgreSQL
import json
# IMPORT SQL
from sql.query import create_table_dim, create_table_fact
def read_credentials():
    with open('/home/hadoop/Documents/finalProject/credentials.json','r') as d:
        data=json.load(d)
    return data

def create_star_schema(schema):
    cfg=read_credentials()['postgresql']  
    postgre_auth = PostgreSQL(cfg)
    conn, cursor = postgre_auth.conn(conn_type='cursor')
    # print("CREATE DIM TABLE...")
    # query_dim = create_table_dim(schema=schema)
    # cursor.execute(query_dim)
    # conn.commit()
    print("CREATE FACT TABLE...")
    query_fact = create_table_fact(schema=schema)
    cursor.execute(query_fact)
    conn.commit()
    print("SUCESS!!!")    
    cursor.close()
    conn.close()

if __name__ == '__main__':
  create_star_schema(schema='public')