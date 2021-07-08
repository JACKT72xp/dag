
# pylint: disable=missing-function-docstring
# [START]
# [START import_module]

import redis
import json
import requests
import sys
import subprocess
import os


from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models.baseoperator import cross_downstream
from airflow.operators.dummy import DummyOperator
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.utils.trigger_rule import TriggerRule
from requests.auth import HTTPBasicAuth
import pandas as pd
from pandas.io.json import json_normalize
subprocess.check_call([sys.executable, "-m", "pip", "install", "confluent_kafka"])
# subprocess.check_call([sys.executable, "-m", "pip", "install", "kafka"])
# import pymongo
# from pymongo import MongoClient
from functools import reduce
from datetime import datetime,timedelta
from sqlalchemy import create_engine,text
import numpy as np
from confluent_kafka import Producer
# subprocess.check_call([sys.executable, "-m", "pip", "install", "bson"])
# subprocess.check_call([sys.executable, "-m", "pip", "install", "pymongo"])

# config = open("config.json","r")
# config = json.loads(config.read())
# config = config[0]

# [END import_module]

# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
}
# [END default_args]


# [START instantiate_dag]
@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['idirect_lima'])
def puller_idirect():
    # sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

    # import confluent_kafka
    # import kafka
    # from kafka.errors import KafkaError
    # uri = "mongodb://bifrostProdUser:Maniac321.@cluster0-shard-00-00.bvdlk.mongodb.net:27017,cluster0-shard-00-01.bvdlk.mongodb.net:27017,cluster0-shard-00-02.bvdlk.mongodb.net:27017/myFirstDatabase?ssl=true&replicaSet=atlas-nn38a4-shard-0&authSource=admin&retryWrites=true&w=majority"
    # conection = MongoClient(uri)
    db_ = []
    # db_ = conection["bifrost"]

    # config = open("config.json","r")
    # config = json.loads(config.read())
    # config = config[0]


    engine = create_engine("mysql://admin:Maniac321.@bifrosttiws-instance-1.cn4dord7rrni.us-west-2.rds.amazonaws.com/bifrostprod10dev?charset=utf8", connect_args={'connect_timeout':120})
    engine_puller = create_engine("mysql://testuser:testpassword@192.168.36.21:6033/puller?charset=utf8", connect_args={'connect_timeout': 120})


    """
    ### TaskFlow API Tutorial Documentation
    This is a simple ETL data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.
    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html)
    """
    # [END instantiate_dag]




    def generateConcatKeySecondary(df,cols):
        try:
            df_stnd_key = df[cols].astype(str) 
            df_stnd_key['concat_key_generate_secondary'] = df_stnd_key[cols].agg('-'.join, axis=1)
            df['concat_key_generate_secondary'] = df_stnd_key['concat_key_generate_secondary']
            return df
        except:
            print("ERROR IN COLUMNS")
            
    def generateConcatKey(df,cols):
        try:
            df_stnd_key = df[cols].astype(str) 
            df_stnd_key['concat_key_generate'] = df_stnd_key[cols].agg('-'.join, axis=1)
            df['concat_key_generate'] = df_stnd_key['concat_key_generate']
            return df
        except:
            print("ERROR IN COLUMNS")
            
            


            
    # [START extract]
    @task()
    def extract_old(key,config):
        redis_cn = redis.Redis(host= '10.233.49.128',    port= '6379',    password="tmCN3FwkP7")
        response = redis_cn.get(key)
        response = json.loads(response)
        df_old = pd.DataFrame(response)
        # df_old = df_old[df_old.columns].add_prefix('old_')
        df_old = generateConcatKey(df_old,[config['primary_join_cols']['old']])
        # df_old = generateConcatKey(df_old,['old_'+config['primary_join_cols']['old']])
        df_old = generateConcatKeySecondary(df_old,config['secondary_join_cols']['old'])
        return {'data': df_old.to_json(orient='records'), 'status':200}




    @task()
    def extract_platform(config):
        try:
            response = requests.get(config['url'], auth=HTTPBasicAuth(config['user'],config['password']), verify=config['verify'],timeout=config['timeout'])
            response = response.text
            response = json.loads(response)
            try:
                for x in config['route_trunk'].split("-"):
                    if x.isnumeric():
                        response=response[int(x)]
                    else:
                        response=response[x]

                response =  pd.DataFrame(response) 
                # response = response[response.columns].add_prefix('platform_')
                response = generateConcatKey(response,[config['primary_join_cols']['platform']])
                # response = generateConcatKey(response,['platform_'+config['primary_join_cols']['platform']])
                response = generateConcatKeySecondary(response,config['secondary_join_cols']['platform'])
                response = response.to_json(orient='records')
            except:
                print("ERROR IN route_trunk")
            # response = pd.DataFrame(response) 
            # response = response[response.columns].add_prefix('platform_')
                response = {}

        except requests.exceptions.RequestException as e:
            response = {}
            print("ERROR IN GET DATA PLATFORM")
        # return response.to_json(orient='records')
        print(response)
        return {'data': response, 'status':200}



    @task()
    def extract_mongo(db_,config):
      df_datamongo = []
      return {'data': df_datamongo, 'status':200}

    #     coltn_mdb = db_[config['mongo_collection']]
        
    #     if config['mongo_limit_time']:
    #         now_day = datetime.now() 
    #         day_generate = now_day 
    #         day_generate = day_generate  - timedelta(minutes=50) 
    # #         day_generate = day_generate  - timedelta(minutes=config['mongo_limit_time']) 
    #         data_mongo = coltn_mdb.find({'platform':config['platform_id']})
    # #         data_mongo = coltn_mdb.find({'timeP':{'$gte':day_generate.strftime("%Y-%m-%d %H:%M:%S")},'platform':config['platform_id']})
    #     else:
    #         data_mongo = coltn_mdb.find({'platform':config['platform_id']})
    #     list_cur = list(data_mongo)
    #     if len(list_cur)==0:
    #         return pd.DataFrame()
    #     json_data = dumps(list_cur, indent = 2)
    #     df_datamongo = pd.DataFrame(loads(json_data))
    #     df_datamongo_origin = pd.DataFrame(loads(json_data))
    #     df_datamongo = df_datamongo[config['mongo_normalization']].apply(pd.Series)
    #     df_datamongo[df_datamongo_origin.columns] = df_datamongo_origin
    #     del df_datamongo[config['mongo_normalization']]
    #     df_datamongo = df_datamongo[df_datamongo.columns].add_prefix('mongo_')
      # return {'data': df_datamongo, 'status':200}




    @task()
    def extract_mysql(engine,config):
        query = "SELECT  * FROM "+str(config['mysql_table'])+" where status = 1 and  platformId = "+str(config['platform_id'])
        df_mysql_total = pd.read_sql_query(query, engine)
        df_mysql_total = df_mysql_total[df_mysql_total.columns].add_prefix('mysql_')
        df_mysql_total = generateConcatKey(df_mysql_total,['mysql_'+config['primary_join_cols']['mysql']])
        df_mysql_total = generateConcatKeySecondary(df_mysql_total,config['secondary_join_cols']['mysql'])
        df_mysql_total = df_mysql_total.to_json(orient='records')
        return {'data': df_mysql_total, 'status':200}





    @task()
    def comparate_old_vs_new(data_platform,data_old):
        df1 = pd.DataFrame(data_platform)
        df2 = pd.DataFrame(data_old)
        print(df1)
        print(df2)
        # df1 = pd.DataFrame(df1['concat_key_generate'])
        # df2 = pd.DataFrame(df2['concat_key_generate'])
        comparison_df = df1.merge(
            df2,
            indicator="_merge_",
            on="concat_key_generate",
            how='outer'
        )
        return {'platform_data':data_platform,'both':comparison_df[comparison_df['_merge_']=='both'].to_json(orient='records'),'left':comparison_df[comparison_df['_merge_']=='left_only'].to_json(orient='records'),'right':comparison_df[comparison_df['_merge_']=='right_only'].to_json(orient='records')}


    @task()
    def comparate_primary_mysql(both,df_mysql,df_plat):
        print("compara con mysql primary")
        both = pd.DataFrame(both)
        df_mysql = pd.DataFrame(df_mysql)
        df_plat = pd.DataFrame(df_plat)
        df_plat_vs_old = dataframe_difference(pd.DataFrame(df_plat['concat_key_generate']),pd.DataFrame(df_old['concat_key_generate']))
        only_new = df_plat_vs_old['left']
        only_old = df_plat_vs_old['right']
        both = df_plat_vs_old['both']
        return {'both':both.to_json(orient='records'),'left':only_new.to_json(orient='records'),'right':only_old.to_json(orient='records')}


    # [END extract]

    # [START load]
    @task()
    def load(data):
        """
        #### Load task
        A simple Load task which takes in the result of the Transform task and
        instead of saving it to end user review, just prints it out.
        """

        print(data)
        return ['ok']
    # [END load]
    # [START main_flow]
    
    config = [
      {
        "route_trunk": "data",
        "url": "http://192.168.36.50:81/api/v1/evo/config/obj/remote",
        "user": "systemapi",
        "password": "tiws2019",
        "timeout": 120,
        "verify": "False",
        "platform_id": 2,
        "mysql_table": "bifrost_terminal_test",
        "mongo_normalization": "puller",
        "mongo_limit_time": 55,
        "mongo_collection": "idirect_test_lima",
        "primary_join_cols": {
          "mysql": "siteId",
          "mongo": "siteId",
          "platform": "Name",
          "old": "Name"
        },
        "secondary_join_cols": {
          "mysql": [
            "mysql_siteId",
            "mysql_id_nms"
          ],
          "mongo": [
            "mongo_Name",
            "mongo_ID"
          ],
          "platform": [
            "platform_Name",
            "platform_ID"
          ],
          "old": [
            "old_Name",
            "old_ID"
          ]
        },
        "platform_name": "idirect_lima"
      }
    ]
    config = config[0]
    key_process = str(config["platform_id"])+"-"+str(config["platform_name"])
    # platform_data = extract_platform(config)
    # mysql_data = extract_mysql(engine,config)
    # mongo_data = extract_mongo(db_,config)
    # old_vs_new = comparate_old_vs_new( extract_platform(config)['data'],extract_old(key_process)['data'])
    # primary_vs_mysql = comparate_primary_mysql(old_vs_new['both'],mysql_data['data'],platform_data['data'])
    
    # platform_data
    old_vs_new = comparate_old_vs_new(extract_platform(config)['data'],extract_old(key_process,config)['data']) 
    # old_vs_new
    old_vs_new >> comparate_primary_mysql(old_vs_new['both'], extract_mysql(engine,config)['data'],old_vs_new['platform_data'])
    # primary_vs_mysql
    # [END main_flow]


# [START dag_invocation]
puller_idirect = puller_idirect()
# [END dag_invocation]

# [END tutorial]
