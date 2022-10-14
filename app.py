#!/usr/bin/python3
from datetime import datetime
from distutils.command.config import config

import os
import json
import sqlparse

import pandas as pd
import numpy as np

import connection
import conn_warehouse

if __name__ == '__main__':
    filetime = datetime.now().strftime('%Y%m%d')
    print(f"[INFO] Service ETL is Starting .....")

    #connect db warehouse
    conn_dwh, engine_dwh  = conn_warehouse.conn()
    cursor_dwh = conn_dwh.cursor()

    #connect db source
    conf = connection.config('postgresql')
    conn, engine = connection.psql_conn(conf)
    cursor = conn.cursor()

    #connect spark
    conf = connection.config('spark')
    client = connection.spark_conn(app="ETL-final-project",config=conf)

    #query extract db source
    path_query = os.getcwd()+'/queries/'
    query = sqlparse.format(
        open(
            path_query+'query.sql','r'
            ).read(), strip_comments=True).strip()

    #query load db warehouse
    query_dwh = sqlparse.format(
        open(
            path_query+'dwh_design.sql','r'
            ).read(), strip_comments=True).strip()

    try:
        print(f"[INFO] Service ETL is Running .....")
        df = pd.read_sql(query, engine)

        #upload local
        path = os.getcwd()
        directory = path+'/'+'local'+'/'
        if not os.path.exists(directory):
            os.makedirs(directory)
        df.to_csv(f"{directory}dim_orders_{filetime}.csv", index=False)
        print(f"[INFO] Upload Data in LOCAL Success .....")

        #insert dwh
        cursor_dwh.execute(query_dwh)
        conn_dwh.commit()
        df.to_sql('dim_transaction', engine_dwh, if_exists='append', index=False)
        print(f"[INFO] Update WDH Success .....")

        #Spark Processing
        directory_spark_transform = path+'/'+'spark_transform'+'/'
        SparkDF = Spark.createDataFrame(df)
        sparkDF.GroupBy("prodcut_transaction").agg(sf.sum("amount_transaction").alias('amount_transaction')) \
            .toPandas() \
                .to_csv(f"{directory_spark_transform}output1.csv", index=False)
             
        print(f"[INFO] Service ETL is Success .....")
    except:
        print(f"[INFO] Service ETL is Failed .....")
    

    