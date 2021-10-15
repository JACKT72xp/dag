
# pylint: disable=missing-function-docstring
# [START]
# [START import_module]

from numpy.lib.function_base import extract
import redis
import json
import requests
import sys
import subprocess
import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.decorators import dag, task
from airflow.models.baseoperator import cross_downstream
from airflow.operators.dummy import DummyOperator
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
# from airflow.utils.edgemodifier import Label
# from airflow.operators.bash_operator import BashOperator


from airflow.utils.trigger_rule import TriggerRule
from requests.auth import HTTPBasicAuth
import pandas as pd
from pandas.io.json import json_normalize
# subprocess.check_call([sys.executable, "-m", "pip3", "install", "confluent_kafka"])
# subprocess.check_call([sys.executable, "-m", "pip", "install", "kafka"])
# import pymongo
from bson.json_util import dumps,loads
from functools import reduce
from datetime import datetime,timedelta
from sqlalchemy import create_engine,text
import numpy as np
from sqlalchemy.sql.expression import exists


r = redis.Redis(host= '192.168.29.20',    port= '6379',    password="bCL3IIuAwv")

# import confluent_kafka
# from confluent_kafka import Producer
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
    'depends_on_past': False,
    'retry_delay': timedelta(seconds=15),
    # 'start_date': yesterday_at_elevenpm,
    # 'email': ['tech.team@industrydive.com'],
    # 'email_on_failure': True,
    # 'email_on_retry': True,
    'retries': 4
}
# [END default_args]
# start_date=days_ago(2)
time_send_now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# [START instantiate_dag]
@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['hughes'])
# @dag(default_args=default_args, schedule_interval='*/10 * * * *', start_date=datetime(2021, 10, 14, 12, 0), tags=['hughes'])
def puller_hughes():
    
    # sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

    # import confluent_kafka
    # import kafka
    # from kafka.errors import KafkaError


    # db_ = []

    # config = open("config.json","r")
    # config = json.loads(config.read())
    # config = config[0]


    engine = create_engine("mysql://admin:Maniac321.@bifrosttiws-instance-1.cn4dord7rrni.us-west-2.rds.amazonaws.com/bifrostprod10dev?charset=utf8", connect_args={'connect_timeout':120})
    config = [
      {
        "route_trunk": "",
        "url": "http://192.168.36.50:80/NMSAPI/vsats/status",
        "user": "",
        "password": "",
        "timeout": 120,
        "verify": "False",
        "platform_id": 1,
        "mysql_table": "bifrost_terminal_full",
        "mongo_normalization": "puller",
        "mongo_limit_time": 55,
        "mongo_collection": "hughes_test",
        "primary_join_cols": {
          "mysql": "siteId",
          "mongo": "deviceID",
          "platform": "deviceID",
          "old": "deviceID"
        },
        "secondary_join_cols": {
          "mysql": [
            "mysql_esn",
            "mysql_statusTerminal",
            "mysql_latitud",
            "mysql_longitud"
          ],
          "mongo": [
            "mongo_esn",
            "mongo_terminalStatus",
            "mongo_latitude",
            "mongo_longitude"
          ],
          "platform": [
            "platform_esn",
            "platform_terminalStatus",
            "platform_latitude",
            "platform_longitude",
          ],
          "old": [
            "old_esn",
            "old_terminalStatus",
            "old_latitude",
            "old_longitude"
          ]
        },
        "platform_name": "hughes"
      }
    ]
    config = config[0]

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
            print("ERROR IN COLUMNS PRIMARY")
    # @task()
    def valid_exist_puller_runing():
        key_redis = None
        query = f"SELECT * FROM puller_cron_platform where status=1 and status_cron=2  limit 1 "
        df = pd.read_sql_query(query, engine)
        data = json.loads(df.to_json(orient="records"))
        if len(data)==0:
            key_redis = 'start'
        else:
            key_redis = 'finish_process'

        return key_redis
    def getDataRedisByKey(key):
        data =  r.get(key)
        data = json.loads(data)
        return data

    @task()
    #------------------------------------------------------------------------
    def save_in_redis_data_old(config,data,key_process):
        df = pd.DataFrame(data)
        df.columns = df.columns.str.replace('platform_', '') 
        del df['concat_key_generate']
        del df['concat_key_generate_secondary']
        data = df.to_json(orient="records")
        redis_cn = redis.Redis(host= '192.168.29.20',    port= '6379',    password="bCL3IIuAwv")
        redis_cn.set('1-hughes',data)
        return {"status":True,"data":""}

    @task()
    #------------------------------------------------------------------------
    def save_in_redis_data_equals_api(config,data,keyname):

        # redis_cn = redis.Redis(host= '192.168.29.20',    port= '6379',    password="bCL3IIuAwv")
        key_insert = keyname+'-insert'
        key_update = keyname+'-update'
        key_delete = keyname+'-delete'
        try:
            data_insert = json.dumps(data['insert_mysql'])
            r.set(key_insert,data_insert)
        except:
            data_insert = data['insert_mysql']
            r.set(key_insert,data_insert)
        

        try:
            data_update = json.dumps(data['update_mysql'])
            r.set(key_update,data_update)
        except:
            data_update = data['update_mysql']
            r.set(key_update,data_update)


        try:
            data_delete = json.dumps(data['delete_mysql'])
            r.set(key_delete,data_delete)
        except:
            data_delete = data['delete_mysql']
            r.set(key_delete,data_delete)

        return {"status":True,"key_insert":key_insert,"key_update":key_update,"key_delete":key_delete}

    @task()
    #------------------------------------------------------------------------
    def save_in_redis_data_only_platform_api(config,data,keyname):
        try:
            data = json.dumps(data)
        except:
            data = data
        redis_cn = redis.Redis(host= '192.168.29.20',    port= '6379',    password="bCL3IIuAwv")
        redis_cn.set(keyname,data)
        return {"status":True,"data":""}



    @task()
    def save_in_redis_data_only_old_api(config,data,keyname):
        try:
            data = json.dumps(data)
        except:
            data = data
        redis_cn = redis.Redis(host= '192.168.29.20',    port= '6379',    password="bCL3IIuAwv")
        redis_cn.set(keyname,data)
        return {"status":True,"data":""}
       
    @task()
    def extract_old(key,config,valid_puller_runing):
        if valid_puller_runing is None:
            return []
        try:
            redis_cn = redis.Redis(host= '192.168.29.20',    port= '6379',    password="bCL3IIuAwv")
            response = redis_cn.get('1-hughes')
            response = json.loads(response)
            df_old = pd.DataFrame(response)
            df_old = df_old[df_old.columns].add_prefix('old_')
            if df_old is None:
                return []
            df_old = generateConcatKey(df_old,['old_'+config['primary_join_cols']['old']])
            df_old = generateConcatKeySecondary(df_old,config['secondary_join_cols']['old'])
            if df_old is None:
                return []
            return [df_old.to_json(orient='records')]
        except:
            return []


    @task()
    #------------------------------------------------------------------------
    def save_in_redis_data_only_platform_mongo_api(config,data,keyname):
        try:
            data = json.dumps(data)
        except:
            data = data
        redis_cn = redis.Redis(host= '192.168.29.20',    port= '6379',    password="bCL3IIuAwv")
        redis_cn.set(keyname,data)
        return {"status":True,"data":""}



    @task()
    #------------------------------------------------------------------------
    def save_in_redis_data_only_old_mongo_api(config,data,keyname):
        try:
            data = json.dumps(data)
        except:
            data = data
        redis_cn = redis.Redis(host= '192.168.29.20',    port= '6379',    password="bCL3IIuAwv")
        redis_cn.set(keyname,data)
        return {"status":True,"data":""}



    @task()
    #------------------------------------------------------------------------
    def save_in_redis_data_equals_mongo_api(config,data,keyname):
        try:
            data = json.dumps(data)
        except:
            data = data
        redis_cn = redis.Redis(host= '192.168.29.20',    port= '6379',    password="bCL3IIuAwv")
        redis_cn.set(keyname,data)
        return {"status":True,"data":""}


    @task()
    def extract_mongo(config,valid_puller_runing):
        if valid_puller_runing is None:
            return []
        from pymongo import MongoClient
        uri = "mongodb://bifrostProdUser:Maniac321.@cluster0-shard-00-00.bvdlk.mongodb.net:27017,cluster0-shard-00-01.bvdlk.mongodb.net:27017,cluster0-shard-00-02.bvdlk.mongodb.net:27017/myFirstDatabase?ssl=true&replicaSet=atlas-nn38a4-shard-0&authSource=admin&retryWrites=true&w=majority"
        conection = MongoClient(uri,connect=False)
        db_ = conection["bifrost"]
        coltn_mdb = db_['hughes_test']
        data_mdb = coltn_mdb.find({},{"_id":True,"siteId":True,"puller.deviceID":True,"puller.esn":True,"puller.latitude":True,"puller.terminalStatus":True,"puller.longitude":True,"_id":True})
        list_cur = list(data_mdb)
        if len(list_cur)==0:
            return []

        json_data = dumps(list_cur, indent = 2)
        # df_datamongo = pd.DataFrame(data_mdb)
        # df_datamongo_origin = pd.DataFrame(data_mdb)
        # json_data = dumps(list_cur, indent = 2)
        df_datamongo = pd.DataFrame(loads(json_data))
        # df_datamongo_origin = pd.DataFrame(
        # df_datamongo_origin = pd.DataFrame(json_data)
        # print(df_datamongo)
        df_datamongo_origin = pd.DataFrame(json.loads(json_data))
        df_datamongo = df_datamongo[config['mongo_normalization']].apply(pd.Series)
        df_datamongo[df_datamongo_origin.columns] = df_datamongo_origin
        del df_datamongo[config['mongo_normalization']]
        del df_datamongo['_id']
        df_datamongo = df_datamongo[df_datamongo.columns].add_prefix('mongo_')
        df_datamongo = generateConcatKey(df_datamongo,['mongo_'+config['primary_join_cols']['mongo']])
        df_datamongo = generateConcatKeySecondary(df_datamongo,config['secondary_join_cols']['mongo'])
        return json.loads(df_datamongo.to_json(orient='records'))
        # return {'data': df_old.to_json(orient='records'), 'status':200}




    @task()
    def save_in_history_mongo(config):
        # uri = "mongodb://bifrostProdUser:Maniac321.@cluster0-shard-00-00.bvdlk.mongodb.net:27017,cluster0-shard-00-01.bvdlk.mongodb.net:27017,cluster0-shard-00-02.bvdlk.mongodb.net:27017/myFirstDatabase?ssl=true&replicaSet=atlas-nn38a4-shard-0&authSource=admin&retryWrites=true&w=majority"
        # conection = MongoClient(uri)
        # db_ = conection["bifrost"]

        from pymongo import MongoClient
        uri = "mongodb://bifrostProdUser:Maniac321.@cluster0-shard-00-00.bvdlk.mongodb.net:27017,cluster0-shard-00-01.bvdlk.mongodb.net:27017,cluster0-shard-00-02.bvdlk.mongodb.net:27017/myFirstDatabase?ssl=true&replicaSet=atlas-nn38a4-shard-0&authSource=admin&retryWrites=true&w=majority"
        conection = MongoClient(uri,connect=False)
        db_ = conection["bifrost"]
        coltn_mdb = db_['puller_history']
        time_send = datetime.now()
        formatted_date = time_send.strftime('%Y-%m-%d %H:%M:%S')
        coltn_mdb.replace_one({'platform':'hughes'},{'platform':'hughes','date':formatted_date},upsert=True)
        return ['OK']


    @task()
    def extract_platform(config,valid_puller_runing):
        if valid_puller_runing is None:
            return []
        try:
            if config['user']!="":
                response = requests.get(config['url'], auth=HTTPBasicAuth(config['user'],config['password']), verify=config['verify'],timeout=config['timeout'])
            else:
                response = requests.get(config['url'], verify=config['verify'],timeout=config['timeout'])
            response = response.text
            response = json.loads(response)
            if  config['route_trunk'] == "":
                response =  pd.DataFrame(response) 
                response = response[response.columns].add_prefix('platform_')
                response = generateConcatKey(response,['platform_'+config['primary_join_cols']['platform']])
                response = generateConcatKeySecondary(response,config['secondary_join_cols']['platform'])
                response = response.to_json(orient='records')
                response = json.loads(response)
                return response
            try:
                for x in config['route_trunk'].split("-"):
                    try:
                            
                        if x.isnumeric():
                            response=response[int(x)]
                        else:
                            response=response[x]
                    except:
                            response=response

                response =  pd.DataFrame(response) 
                response = response[response.columns].add_prefix('platform_')
                response = generateConcatKey(response,['platform_'+config['primary_join_cols']['platform']])
                response = generateConcatKeySecondary(response,config['secondary_join_cols']['platform'])
                response = response.to_json(orient='records')
                response = json.loads(response)
            except:
                print("ERROR IN route_trunk")
                response = {}

        except requests.exceptions.RequestException as e:
            response = {}
            print("ERROR IN GET DATA PLATFORM")
        return response

    @task()
    def extract_mysql(engine,config,valid_puller_runing):
        if valid_puller_runing is None:
            return []
        query = "SELECT  * FROM "+str(config['mysql_table'])+" where status = 1 and  platformId = "+str(config['platform_id'])
        df_mysql_total = pd.read_sql_query(query, engine)
        if df_mysql_total.empty:
            return '[{}]'
        df_mysql_total = df_mysql_total[df_mysql_total.columns].add_prefix('mysql_')
        # df_mysql_total = generateConcatKey(df_mysql_total,[config['primary_join_cols']['mysql']])
        df_mysql_total = generateConcatKey(df_mysql_total,['mysql_'+config['primary_join_cols']['mysql']])
        df_mysql_total = generateConcatKeySecondary(df_mysql_total,config['secondary_join_cols']['mysql'])
        df_mysql_total = df_mysql_total.to_json(orient='records')
        return df_mysql_total

    @task()
    def comparate_old_vs_new(data_platform,data_old):
        df1 = pd.DataFrame(data_platform)
        if len(data_old)==0:
            data_platform=df1.to_json(orient="records")
            return {'platform_data':data_platform,'comparation':[],'both':data_platform,'only_platform':[],'only_old':[]}
        else:
            df2 = pd.DataFrame(json.loads(data_old[0]))
        comparation = df1.merge(
            df2,
            on="concat_key_generate",
            indicator="_merge_",
            how='outer'
        )
        both = comparation[comparation['_merge_']=='both']
        plat = comparation[comparation['_merge_']=='left_only']
        old = comparation[comparation['_merge_']=='right_only']
        if both.empty:
            both_send="empty"
        else:
            both_send=both.to_json(orient="records")

        if plat.empty:
            plat_send="empty"
        else:
            plat_send=plat.to_json(orient="records")
            
        if old.empty:
            old_send=[]
        else:
            old_send=old.to_json(orient="records")
        data_platform=df1.to_json(orient="records")
        return {'platform_data':data_platform,'comparation':comparation.to_json(orient="records"),'both':both_send,'only_platform':plat_send,'only_old':old_send}

    @task()
    def comparate_primary_mysql_equals(df_mysql,comparate):
        df_mysql = pd.DataFrame(json.loads(df_mysql))
        if df_mysql.empty:
            df_mysql = pd.DataFrame(columns=['concat_key_generate'])

        platform_data = pd.DataFrame(json.loads(comparate['platform_data']))
        comparate = pd.DataFrame(json.loads(comparate['both']))
        both = comparate
        both['exist_mysql'] = np.where(both['concat_key_generate'].isin(list(df_mysql['concat_key_generate'])) , 1, 0)
        exist_mysql_p = both[both['exist_mysql']==1]
        not_exist_mysql_p = both[both['exist_mysql']==0]
        exist_mysql_p = platform_data[platform_data['concat_key_generate'].isin(list(exist_mysql_p['concat_key_generate']))]

        if exist_mysql_p.empty:
            exist_mysql_p=[]
        else:
            exist_mysql_p=json.loads(exist_mysql_p.to_json(orient="records"))


        if not_exist_mysql_p.empty:
            not_exist_mysql_p=[]
        else:
            not_exist_mysql_p=json.loads(not_exist_mysql_p.to_json(orient="records"))
        
        return {'exist_mysql':exist_mysql_p,'not_exist_mysql':not_exist_mysql_p}

    @task()
    def comparate_primary_mysql_only_platform(df_mysql,comparate):
        df_mysql = pd.DataFrame(json.loads(df_mysql))
        if df_mysql.empty:
            df_mysql = pd.DataFrame(columns=['concat_key_generate'])

        if comparate['only_platform']=='empty':
            return {'exist_mysql':[],'not_exist_mysql':[]}

        platform_data = pd.DataFrame(json.loads(comparate['platform_data']))

        try:
            comparate = pd.DataFrame(json.loads(comparate['only_platform']))
        except:
            # try:
                # comparate = pd.DataFrame(comparate['only_platform'])
            # except:
                comparate = pd.DataFrame(columns=['concat_key_generate'])
        only_platform = comparate
        only_platform['exist_mysql'] = np.where(only_platform['concat_key_generate'].isin(list(df_mysql['concat_key_generate'])) , 1, 0)
        exist_mysql_p = only_platform[only_platform['exist_mysql']==1]
        not_exist_mysql_p = only_platform[only_platform['exist_mysql']==0]
        exist_mysql_p = platform_data[platform_data['concat_key_generate'].isin(list(exist_mysql_p['concat_key_generate']))]

        if exist_mysql_p.empty:
            exist_mysql_p=[]
        else:
            exist_mysql_p=json.loads(exist_mysql_p.to_json(orient="records"))


        if not_exist_mysql_p.empty:
            not_exist_mysql_p=[]
        else:
            not_exist_mysql_p=json.loads(not_exist_mysql_p.to_json(orient="records"))
        
        return {'exist_mysql':exist_mysql_p,'not_exist_mysql':not_exist_mysql_p}


    @task()
    def comparate_primary_mysql_only_data_old(df_mysql,comparate):
        df_mysql = pd.DataFrame(json.loads(df_mysql))
        if df_mysql.empty:
            df_mysql = pd.DataFrame(columns=['concat_key_generate'])

        platform_data = pd.DataFrame(json.loads(comparate['platform_data']))

        if comparate['only_old']==[]:
            return {'update_mysql':[],'insert_mysql':[],'delete_mysql':[]}

        comparate = pd.DataFrame(json.loads(comparate['only_old']))
        only_old = comparate
        only_old['exist_mysql'] = np.where(only_old['concat_key_generate'].isin(list(df_mysql['concat_key_generate'])) , 1, 0)
        exist_mysql_p = only_old[only_old['exist_mysql']==1]
        not_exist_mysql_p = only_old[only_old['exist_mysql']==0]
        exist_mysql_p = platform_data[platform_data['concat_key_generate'].isin(list(exist_mysql_p['concat_key_generate']))]

        if exist_mysql_p.empty:
            exist_mysql_p=[]
        else:
            exist_mysql_p=json.loads(exist_mysql_p.to_json(orient="records"))


        if not_exist_mysql_p.empty:
            not_exist_mysql_p=[]
        else:
            not_exist_mysql_p=json.loads(not_exist_mysql_p.to_json(orient="records"))
        
        return {'delete_mysql':exist_mysql_p}
    


    @task()
    def comparate_primary_mongo_equals(df_mongo,comparate):
        df_mongo = pd.DataFrame(df_mongo)
        platform_data = pd.DataFrame(json.loads(comparate['platform_data']))
        both = pd.DataFrame(json.loads(comparate['both']))
        try:
            comparate = pd.DataFrame(json.loads(comparate['both']))
        except:
            comparate = pd.DataFrame(columns=['concat_key_generate'])
        both = comparate

        if df_mongo.empty:
            df_mongo = pd.DataFrame(columns=['concat_key_generate'])


        both['exist_mongo'] = np.where(both['concat_key_generate'].isin(list(df_mongo['concat_key_generate'])) , 1, 0)
        exist_mongo_p = both[both['exist_mongo']==1]
        not_exist_mongo_p = both[both['exist_mongo']==0]
        exist_mongo_p = platform_data[platform_data['concat_key_generate'].isin(list(exist_mongo_p['concat_key_generate']))]
        not_exist_mongo_p = platform_data[platform_data['concat_key_generate'].isin(list(not_exist_mongo_p['concat_key_generate']))]

        if exist_mongo_p.empty:
            exist_mongo_p=[]
        else:
            exist_mongo_p=json.loads(exist_mongo_p.to_json(orient="records"))

    
        if not_exist_mongo_p.empty:
            not_exist_mongo_p=[]
        else:
            not_exist_mongo_p=json.loads(not_exist_mongo_p.to_json(orient="records"))
            # not_exist_mongo_p=not_exist_mongo_p.to_json(orient="records")

        print("exist_mongo_p")
        print(exist_mongo_p)
        print("not_exist_mongo_p")
        print(not_exist_mongo_p)

        # return exist_mongo_p.to_json(orient="records")
        return {'exist_mongo':exist_mongo_p,'not_exist_mongo':not_exist_mongo_p}


    @task()
    def comparate_primary_mongo_only_platform(df_mongo,comparate):
        df_mongo = pd.DataFrame(df_mongo)
        platform_data = pd.DataFrame(json.loads(comparate['platform_data']))
        
        if comparate['only_platform']=='empty':
            return {'exist_mongo':[],'not_exist_mongo':[]}


        # only_platform = pd.DataFrame(json.loads(comparate['only_platform']))
        try:
            comparate = pd.DataFrame(json.loads(comparate['only_platform']))
            print(comparate,'comparatecomparatecomparatecomparate')
        except:
            comparate = pd.DataFrame(columns=['concat_key_generate'])
        only_platform = comparate

        if df_mongo.empty:
            df_mongo = pd.DataFrame(columns=['concat_key_generate'])
        only_platform['exist_mongo'] = np.where(only_platform['concat_key_generate'].isin(list(df_mongo['concat_key_generate'])) , 1, 0)
        exist_mongo_p = only_platform[only_platform['exist_mongo']==1]
        not_exist_mongo_p = only_platform[only_platform['exist_mongo']==0]
        exist_mongo_p = platform_data[platform_data['concat_key_generate'].isin(list(exist_mongo_p['concat_key_generate']))]
        not_exist_mongo_p = platform_data[platform_data['concat_key_generate'].isin(list(not_exist_mongo_p['concat_key_generate']))]

        if exist_mongo_p.empty:
            exist_mongo_p=[]
        else:
            exist_mongo_p=json.loads(exist_mongo_p.to_json(orient="records"))

    
        if not_exist_mongo_p.empty:
            not_exist_mongo_p=[]
        else:
            not_exist_mongo_p=json.loads(not_exist_mongo_p.to_json(orient="records"))
            # not_exist_mongo_p=not_exist_mongo_p.to_json(orient="records")

        print("exist_mongo_p")
        print(exist_mongo_p)
        print("not_exist_mongo_p")
        print(not_exist_mongo_p)

        # return exist_mongo_p.to_json(orient="records")
        return {'exist_mongo':exist_mongo_p,'not_exist_mongo':not_exist_mongo_p}



    @task()
    def comparate_primary_mongo_only_old(df_mongo,comparate):
        df_mongo = pd.DataFrame(df_mongo)
        platform_data = pd.DataFrame(json.loads(comparate['platform_data']))

        if comparate['only_old']==[]:
            return {'update_mongo':[],'insert_mongo':[],'delete_mongo':[]}

        only_old = pd.DataFrame(json.loads(comparate['only_old']))


        try:
            comparate = pd.DataFrame(json.loads(comparate['only_old']))
        except:
            comparate = pd.DataFrame(columns=['concat_key_generate'])
        only_platform = comparate

        if df_mongo.empty:
            df_mongo = pd.DataFrame(columns=['concat_key_generate'])

        only_old['exist_mongo'] = np.where(only_old['concat_key_generate'].isin(list(df_mongo['concat_key_generate'])) , 1, 0)
        exist_mongo_p = only_old[only_old['exist_mongo']==1]
        not_exist_mongo_p = only_old[only_old['exist_mongo']==0]
        exist_mongo_p = platform_data[platform_data['concat_key_generate'].isin(list(exist_mongo_p['concat_key_generate']))]
        not_exist_mongo_p = platform_data[platform_data['concat_key_generate'].isin(list(not_exist_mongo_p['concat_key_generate']))]

        if exist_mongo_p.empty:
            exist_mongo_p=[]
        else:
            exist_mongo_p=json.loads(exist_mongo_p.to_json(orient="records"))

    
        if not_exist_mongo_p.empty:
            not_exist_mongo_p=[]
        else:
            not_exist_mongo_p=json.loads(not_exist_mongo_p.to_json(orient="records"))
        return {'delete_mongo':exist_mongo_p}
        
        

    @task()
    def comparate_secondary_mysql_only_platform(df_mysql,comparate,old):
        glob_comparate = comparate
        try:
            comparate = pd.DataFrame(comparate['exist_mysql'])
        except:
            comparate = pd.DataFrame(columns=['concat_key_generate_secondary'])
        df_mysql = pd.DataFrame(json.loads(df_mysql))
        
        if df_mysql.empty:
            df_mysql = pd.DataFrame(columns=['concat_key_generate_secondary'])

        both = comparate
        try:
            both['exist_mysql_secondary'] = np.where(both['concat_key_generate_secondary'].isin(list(df_mysql['concat_key_generate_secondary'])) , 1, 0)
        except:
            return {'update_mysql':[],'insert_mysql':glob_comparate['not_exist_mysql'],'delete_mysql':old['only_old']}

        exist_mysql_s = both[both['exist_mysql_secondary']==1]
        not_exist_mysql_s = both[both['exist_mysql_secondary']==0]
        not_exist_mysql_s_com = both[both['exist_mysql_secondary']==0]
        if exist_mysql_s.empty:
            exist_mysql_s = []
        else:
            exist_mysql_s = json.loads(exist_mysql_s.to_json(orient="records"))

        if not_exist_mysql_s.empty:
            not_exist_mysql_s = []
            data_mysql_not_exist_s = []
        else:
            data_mysql_not_exist_s = df_mysql[df_mysql['concat_key_generate'].isin(list(not_exist_mysql_s_com['concat_key_generate']))]
            data_mysql_not_exist_s = pd.merge(not_exist_mysql_s_com, data_mysql_not_exist_s, on="concat_key_generate")
            data_mysql_not_exist_s = json.loads(data_mysql_not_exist_s.to_json(orient="records"))
        return {'update_mysql':data_mysql_not_exist_s,'insert_mysql':glob_comparate['not_exist_mysql'],'delete_mysql':old['only_old']}



        

    @task()
    def comparate_secondary_mysql_equals(df_mysql,comparate,old):
        glob_comparate = comparate
        try:
            comparate = pd.DataFrame(comparate['exist_mysql'])
        except:
            comparate = pd.DataFrame(columns=['concat_key_generate_secondary'])
        df_mysql = pd.DataFrame(json.loads(df_mysql))
        
        if df_mysql.empty:
            df_mysql = pd.DataFrame(columns=['concat_key_generate_secondary'])

        both = comparate
        try:
            both['exist_mysql_secondary'] = np.where(both['concat_key_generate_secondary'].isin(list(df_mysql['concat_key_generate_secondary'])) , 1, 0)
        except:
            return {'exist_mysql_secondary':[],'not_exist_mysql_secondary':[]}

        exist_mysql_s = both[both['exist_mysql_secondary']==1]
        not_exist_mysql_s = both[both['exist_mysql_secondary']==0]
        not_exist_mysql_s_com = both[both['exist_mysql_secondary']==0]
        if exist_mysql_s.empty:
            exist_mysql_s = []
        else:
            exist_mysql_s = json.loads(exist_mysql_s.to_json(orient="records"))

        if not_exist_mysql_s.empty:
            not_exist_mysql_s = []
            data_mysql_not_exist_s = []
        else:
            data_mysql_not_exist_s = df_mysql[df_mysql['concat_key_generate'].isin(list(not_exist_mysql_s_com['concat_key_generate']))]
            data_mysql_not_exist_s = pd.merge(not_exist_mysql_s_com, data_mysql_not_exist_s, on="concat_key_generate")
            data_mysql_not_exist_s = json.loads(data_mysql_not_exist_s.to_json(orient="records"))
        return {'update_mysql':data_mysql_not_exist_s,'insert_mysql':glob_comparate['not_exist_mysql'],'delete_mysql':old['only_old']}
        # return ['ok']

    @task()
    def comparate_secondary_mongo_equals(df_mongo,comparate,old):
        glob_comparate = comparate
        df_mongo = pd.DataFrame(df_mongo)
        
        if df_mongo.empty:
            df_mongo = pd.DataFrame(columns=['concat_key_generate_secondary'])

        try:
            comparate = pd.DataFrame(comparate['exist_mongo'])
        except:
            comparate = pd.DataFrame(columns=['concat_key_generate_secondary'])


    
        try:
            comparate_not_exist = pd.DataFrame(glob_comparate['not_exist_mongo'])
            comparate_not_exist.columns = comparate_not_exist.columns.str.replace('platform_', '') 
            # del comparate_not_exist['concat_key_generate']
            print(comparate_not_exist,'comparate_not_existcomparate_not_existcomparate_not_existcomparate_not_exist')

            comparate_not_exist = json.loads(comparate_not_exist.to_json(orient="records"))
            print(comparate_not_exist,'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')
            # del comparate_not_exist['concat_key_generate_secondary']
        except:
            comparate_not_exist = []


        both = comparate
        try:
            both['exist_mongo_secondary'] = np.where(both['concat_key_generate_secondary'].isin(list(df_mongo['concat_key_generate_secondary'])) , 1, 0)
        except:
            return {'update_mongo':[],'insert_mongo':comparate_not_exist,'delete_mongo':old['only_old']}

        exist_mongo_s = both[both['exist_mongo_secondary']==1]
        not_exist_mongo_s = both[both['exist_mongo_secondary']==0]

        if exist_mongo_s.empty:
            exist_mongo_s = []
        else:
            exist_mongo_s = json.loads(exist_mongo_s.to_json(orient="records"))

        if not_exist_mongo_s.empty:
            not_exist_mongo_s = []
        else:
            not_exist_mongo_s.columns = not_exist_mongo_s.columns.str.replace('platform_', '') 
            del not_exist_mongo_s['concat_key_generate']
            del not_exist_mongo_s['concat_key_generate_secondary']
            not_exist_mongo_s = json.loads(not_exist_mongo_s.to_json(orient="records"))

        # try:
            # comparate_not_exist = json.loads(comparate_not_exist.to_json(orient="records"))
        # except:
            # comparate_not_exist = []
        return {'update_mongo':not_exist_mongo_s,'insert_mongo':comparate_not_exist,'delete_mongo':old['only_old']}


    @task()
    def comparate_secondary_mongo_only_platform(df_mongo,comparate,old):
        glob_comparate = comparate
        df_mongo = pd.DataFrame(df_mongo)
        
        if df_mongo.empty:
            df_mongo = pd.DataFrame(columns=['concat_key_generate_secondary'])

        try:
            comparate = pd.DataFrame(comparate['exist_mongo'])
        except:
            comparate = pd.DataFrame(columns=['concat_key_generate_secondary'])
        both = comparate
        try:
            both['exist_mongo_secondary'] = np.where(both['concat_key_generate_secondary'].isin(list(df_mongo['concat_key_generate_secondary'])) , 1, 0)
        except:
            return {'update_mongo':[],'insert_mongo':glob_comparate['not_exist_mongo'],'delete_mongo':old['only_old']}

        exist_mongo_s = both[both['exist_mongo_secondary']==1]
        not_exist_mongo_s = both[both['exist_mongo_secondary']==0]

        if exist_mongo_s.empty:
            exist_mongo_s = []
        else:
            exist_mongo_s = json.loads(exist_mongo_s.to_json(orient="records"))

        if not_exist_mongo_s.empty:
            not_exist_mongo_s = []
        else:
            not_exist_mongo_s = json.loads(not_exist_mongo_s.to_json(orient="records"))

        return {'update_mongo':not_exist_mongo_s,'insert_mongo':glob_comparate['not_exist_mongo'],'delete_mongo':old['only_old']}


    @task()
    def save_key_in_history_puller_cron(key,type_puller):
        query_update = f"INSERT INTO puller_cron_platform (key_redis,status_cron,platform_id,type) values('{key}',1,1,'{type_puller}')"
        print(query_update)
        connection_engi = engine.connect()
        resp = connection_engi.execute(query_update)
        if resp.rowcount >0:
            resp = 'INSERT OK'
        else:
            resp = 'ERROR'
        return resp
    @task()
    def processDataInsertMysql(keys):
        key = keys['key_insert']
        try:
            data = getDataRedisByKey(key)
        except:
        # if len(data)==0:
            return []
        data_insert_send = pd.DataFrame(data)
        data_insert_send = data_insert_send[['platform_esn','platform_deviceID','platform_latitude','platform_longitude','platform_terminalStatus','platform_esn']]
        data_insert_send.rename(columns={"platform_deviceID": "siteId"}, inplace = True)
        data_insert_send.rename(columns={"platform_latitude": "latitud"}, inplace = True)
        data_insert_send.rename(columns={"platform_longitude": "longitud"}, inplace = True)
        data_insert_send.rename(columns={"platform_terminalStatus": "statusTerminal"}, inplace = True)
        data_insert_send.rename(columns={"platform_esn": "esn"}, inplace = True)
        data_insert_send['platformId'] = 1
        data_insert_send['status'] = 1
        # data_insert_send.to_sql('bifrost_terminal_full', engine, if_exists='append', index=False)
        # dateSaveHistoryInsert(data)
        return "ok"

    @task()
    def processDataUpdateMysql(engine,keys):
        key = keys['key_update']
        try:
            data = getDataRedisByKey(key)
        except:
        # if len(data)==0:
            return []

        if len(data)==0:
            return []
        connection_engi = engine.connect()
        data = pd.DataFrame(data)
        args = data[['platform_esn','platform_latitude','platform_longitude','platform_deviceID']].iloc[0:].to_dict('record') 
        elements = []
        query_update = text("""             UPDATE bifrost_terminal_full            SET esn=:platform_esn, latitud=:platform_latitude, longitud=:platform_longitude WHERE siteId = :platform_deviceID """)         
        connection_engi.execute(query_update, args)
        return ['ok']
    
    @task()
    def processDataDeleteMysql(engine,keys):
        key = keys['key_delete']
        try:
            data = getDataRedisByKey(key)
        except:
        # if len(data)==0:
            return []
        # if len(data)==0:
        #     return []
        connection_engi = engine.connect()

        # time_send = time_send_now
        # formatted_date = str(time_send)
        for x in data:
            sqlesn = "UPDATE bifrost_terminal_full SET status =0  WHERE siteId = '"+  x['platform_deviceID']  +"' and status!=0"
            connection_engi.execute(sqlesn)
            # dateSaveHistory({"type":"delete_mysql","principal_key":x['platform_deviceID'],"changes":{'status':0}})
        return ['ok']    

    @task()
    def finish_process():
        return ['ok']
    @task()
    def finish():
        return ['ok']
    @task()
    def start():
        return ['ok']


    # [START main_flow]
    valid_puller_runing = valid_exist_puller_runing()

    checkTask = BranchPythonOperator(
    task_id='valid_puller_runing',
    trigger_rule=TriggerRule.ONE_SUCCESS,
    python_callable=valid_exist_puller_runing, #Registered method
    provide_context=True,
    # dag=dag
    )
    # valid_puller_runing =None
    # print(valid_puller_runing,'valid_puller_runingvalid_puller_runingvalid_puller_runingvalid_puller_runing')
    # if valid_puller_runing is None:
        # end = finish([{"status":True}])
        # print(valid_puller_runing.items)
    # else:
    rs = start()
    key_process = str(config["platform_id"])+"-"+str(config["platform_name"])
    old_data = extract_old(key_process,config,valid_puller_runing)
    platform_data = extract_platform(config,valid_puller_runing)
    comp = comparate_old_vs_new(platform_data,old_data)
    mysql_data = extract_mysql(engine,config,valid_puller_runing)
    mongo_data = extract_mongo(config,valid_puller_runing)
    ##COMPARATE MYSQL
    time_send = datetime.now()
    formatted_date = time_send.strftime('%Y-%m-%d-%H-%M')
    key_redis_mysql = key_process+'-mysql-'+formatted_date
    key_redis_mongo = key_process+'-mongo-'+formatted_date
    
    primary_vs_mysql_equals = comparate_primary_mysql_equals(mysql_data,comp)
    secondary_vs_mysql_equals = comparate_secondary_mysql_equals(mysql_data,primary_vs_mysql_equals,comp)
    save_in_redis_result_equals = save_in_redis_data_equals_api(config,secondary_vs_mysql_equals,key_redis_mysql)
    insert_data_mysql_equals = processDataInsertMysql(save_in_redis_result_equals)
    update_data_mysql_equals = processDataUpdateMysql(engine,save_in_redis_result_equals)
    delete_data_mysql_equals = processDataDeleteMysql(engine,save_in_redis_result_equals)
    
    
    # save_key_in_history_puller_cron_equals = save_key_in_history_puller_cron(key_redis_mysql+'-equals','mysql')
    
    primary_vs_mysql_only_platform= comparate_primary_mysql_only_platform(mysql_data,comp)
    secondary_vs_mysql_only_platform = comparate_secondary_mysql_only_platform(mysql_data,primary_vs_mysql_only_platform,comp)
    save_in_redis_result_only_platform = save_in_redis_data_only_platform_api(config,secondary_vs_mysql_only_platform,key_redis_mysql+'-platform')
    save_key_in_history_puller_cron_only_platform = save_key_in_history_puller_cron(key_redis_mysql+'-platform','mysql')
    
    primary_vs_mysql_only_old= comparate_primary_mysql_only_data_old(mysql_data,comp)
    save_in_redis_result_only_old = save_in_redis_data_only_old_api(config,primary_vs_mysql_only_old,key_redis_mysql+'-old')
    save_key_in_history_puller_cron_only_old = save_key_in_history_puller_cron(key_redis_mysql+'-old','mysql')
    
    
    ##COMPARATE MONGODB


    primary_vs_mongo_equals = comparate_primary_mongo_equals(mongo_data,comp)
    secondary_vs_mongo_equals = comparate_secondary_mongo_equals(mongo_data,primary_vs_mongo_equals,comp)
    save_in_redis_result_mongo_equals = save_in_redis_data_equals_mongo_api(config,secondary_vs_mongo_equals,key_redis_mongo+'-equals')
    save_key_in_history_puller_cron_equals_mongo = save_key_in_history_puller_cron(key_redis_mongo+'-equals','mongo')
    
    primary_vs_mongo_only_platform = comparate_primary_mongo_only_platform(mongo_data,comp)
    secondary_vs_mongo_only_platform = comparate_secondary_mongo_only_platform(mongo_data,primary_vs_mongo_only_platform,comp)
    save_in_redis_result_mongo_only_platform = save_in_redis_data_only_platform_mongo_api(config,secondary_vs_mongo_only_platform,key_redis_mongo+'-platform')
    save_key_in_history_puller_cron_only_platform_mongo = save_key_in_history_puller_cron(key_redis_mongo+'-platform','mongo')
    
    primary_vs_mongo_only_data_old = comparate_primary_mongo_only_old(mongo_data,comp)
    save_in_redis_result_mongo_only_old = save_in_redis_data_only_old_mongo_api(config,primary_vs_mongo_only_data_old,key_redis_mongo+'-old')
    save_key_in_history_puller_cron_only_old_mongo = save_key_in_history_puller_cron(key_redis_mongo+'-old','mongo')
    save_in_redis_end = save_in_redis_data_old(config,platform_data,key_process)
    save_in_history_mongo_puller = save_in_history_mongo(config)
    
    end = finish()
    end_process = finish_process()
    
    checkTask >> end_process
    checkTask >> rs
    rs >> mysql_data
    rs >> mongo_data
    # rs >> [platform_data,old_data,mysql_data,mongo_data] >> comp,mysql_data >> [primary_vs_mysql_equals >> secondary_vs_mysql_equals >>  [insert_data_mysql_equals,update_data_mysql_equals,delete_data_mysql_equals] >> primary_vs_mysql_only_platform >> secondary_vs_mysql_only_platform >> save_in_redis_result_only_platform >> save_key_in_history_puller_cron_only_platform ,  primary_vs_mysql_only_old >> save_in_redis_result_only_old >> save_key_in_history_puller_cron_only_old ] >> save_in_redis_end >> save_in_history_mongo_puller >> end
    # rs >> [platform_data,old_data,mongo_data,mysql_data] >> comp,mongo_data >> [primary_vs_mongo_equals >> secondary_vs_mongo_equals >> save_in_redis_result_mongo_equals >> save_key_in_history_puller_cron_equals_mongo, primary_vs_mongo_only_platform >> secondary_vs_mongo_only_platform >> save_in_redis_result_mongo_only_platform >> save_key_in_history_puller_cron_only_platform_mongo  , primary_vs_mongo_only_data_old >> save_in_redis_result_mongo_only_old >> save_key_in_history_puller_cron_only_old_mongo]  >> save_in_redis_end >> save_in_history_mongo_puller >> end
    rs >> [platform_data,old_data] >> comp,mysql_data >> [primary_vs_mysql_equals >> secondary_vs_mysql_equals >>  save_in_redis_result_equals >> insert_data_mysql_equals,update_data_mysql_equals,delete_data_mysql_equals,primary_vs_mysql_only_platform >> secondary_vs_mysql_only_platform >> save_in_redis_result_only_platform >> save_key_in_history_puller_cron_only_platform ,  primary_vs_mysql_only_old >> save_in_redis_result_only_old >> save_key_in_history_puller_cron_only_old ] >> save_in_redis_end >> save_in_history_mongo_puller >> end
    rs >> [platform_data,old_data] >> comp,mongo_data >> [primary_vs_mongo_equals >> secondary_vs_mongo_equals >> save_in_redis_result_mongo_equals >> save_key_in_history_puller_cron_equals_mongo, primary_vs_mongo_only_platform >> secondary_vs_mongo_only_platform >> save_in_redis_result_mongo_only_platform >> save_key_in_history_puller_cron_only_platform_mongo  , primary_vs_mongo_only_data_old >> save_in_redis_result_mongo_only_old >> save_key_in_history_puller_cron_only_old_mongo]  >> save_in_redis_end >> save_in_history_mongo_puller >> end

    # [END main_flow]


# [START dag_invocation]
puller_hughes = puller_hughes()
# [END dag_invocation]
