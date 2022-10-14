#mendump excel ke database
#!pyhton3

from venv import create
import pandas as pd
import numpy as np



from sqlalchemy import create_engine

if __name__=="__main__":
    #https://docs.sqlalchemy.org/en/14/core/engines.html
    username = 'postgres'
    password = 'palambas'
    database = 'postgres'
    ip = '127.0.0.1'

    try:
        engine = create_engine(f"postgresql://{username}:{password}@{ip}:5432/{database}")
        print(f"[INFO] Success Connect PostgreSQL......")
    except:
        print(f"[INFO] Error Connect PostgreSQL......")

    list_filename = ['customer','product','transaction']
    for file in list_filename:
           pd.read_csv(f"bigdata_{file}.csv").to_sql(f"bigdata_{file}", con=engine)
           print(f"[INFO] Success Dump File {file}.........")