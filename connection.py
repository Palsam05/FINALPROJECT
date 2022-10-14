#!/usr/bin/python3

import os
import json
import psycopg2
import hdfs
from pyspark.sql import SparkSession

from sqlalchemy import create_engine

def config(param):
    path = os.getcwd()
    with open(path+'/'+'config.json') as file:
        conf = json.load(file)[param]
    return conf

def psql_conn(conf):
    try:
        conn = psycopg2.connect(host=conf['host'], 
                                database=conf['db'], 
                                user=conf['user'], 
                                password=conf['pwd']
                                )
        print(f"[INFO] Success connect PostgreSQL .....")
        engine = create_engine(f"postgresql+psycopg2://{conf['user']}:{conf['pwd']}@{conf['host']}/{conf['db']}")
        return conn, engine
    except:
        print(f"[INFO] Can't connect PostgreSQL .....")

def spark_conn(app, config):
    master = config['ip']
    try:
        spark = SparkSession.builder \
            .master(master) \
                .appName(app) \
                    .getOrCreate()

        print(f"[INFO] Success Connect SPARK ENGINE.....")
        return spark
    except:
        print(f"[INFO] Error Connect SPARK ENGINE.....")
