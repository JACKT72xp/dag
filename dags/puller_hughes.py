
# pylint: disable=missing-function-docstring
# [START]
# [START import_module]

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
# from airflow.utils.edgemodifier import Label


from airflow.utils.trigger_rule import TriggerRule
from requests.auth import HTTPBasicAuth
import pandas as pd
from pandas.io.json import json_normalize
# subprocess.check_call([sys.executable, "-m", "pip3", "install", "confluent_kafka"])
# subprocess.check_call([sys.executable, "-m", "pip", "install", "kafka"])
# import pymongo
from pymongo import MongoClient
from bson.json_util import dumps,loads
from functools import reduce
from datetime import datetime,timedelta
from sqlalchemy import create_engine,text
import numpy as np
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

# [START instantiate_dag]
@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['hughes'])
# @dag(default_args=default_args, schedule_interval='*/30 * * * *', start_date=datetime(2021, 7, 26, 16, 0), tags=['hughes'])
def puller_hughes():
    # sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

    # import confluent_kafka
    # import kafka
    # from kafka.errors import KafkaError
    # uri = "mongodb://bifrostProdUser:Maniac321.@cluster0-shard-00-00.bvdlk.mongodb.net:27017,cluster0-shard-00-01.bvdlk.mongodb.net:27017,cluster0-shard-00-02.bvdlk.mongodb.net:27017/myFirstDatabase?ssl=true&replicaSet=atlas-nn38a4-shard-0&authSource=admin&retryWrites=true&w=majority"
    # conection = MongoClient(uri)
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
        "mysql_table": "bifrost_terminal",
        "mongo_normalization": "puller",
        "mongo_limit_time": 55,
        "mongo_collection": "hughes",
        "primary_join_cols": {
          "mysql": "siteId",
          "mongo": "deviceID",
          "platform": "deviceID",
          "old": "deviceID"
        },
        "secondary_join_cols": {
          "mysql": [
            "mysql_esn",
            "mysql_statusTerminal"
          ],
          "mongo": [
            "mongo_esn",
            "mongo_terminalStatus"
          ],
          "platform": [
            "platform_esn",
            "platform_terminalStatus"
          ],
          "old": [
            "old_esn",
            "old_terminalStatus"
          ]
        },
        "platform_name": "hughes"
      }
    ]
    config = config[0]
    # db_ = conection["bifrost"]
    # coltn_mdb = db_[config["mongo_collection"]]
    # data_mdb = coltn_mdb.find({'platform':config['platform_id']})


    def generateConcatKeySecondary(df,cols):
        try:
            print(cols,'collll')
            print(df.columns,'columnscolumnscolumnscolumns')
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
            
            
    @task()
    #------------------------------------------------------------------------
    def save_in_redis_data_old(config,data,key_process):
        df = pd.DataFrame(data)
        df.columns = df.columns.str.replace('platform_', '') 
        del df['concat_key_generate']
        del df['concat_key_generate_secondary']
        data = df.to_json(orient="records")
        redis_cn = redis.Redis(host= '192.168.29.20',    port= '6379',    password="bCL3IIuAwv")
        redis_cn.set(key_process,data)
        return {"status":True,"data":""}

    @task()
    #------------------------------------------------------------------------
    def save_in_redis_data_equals_api(config,data,keyname):
        try:
            data = json.dumps(data)
        except:
            data = data
        redis_cn = redis.Redis(host= '192.168.29.20',    port= '6379',    password="bCL3IIuAwv")
        redis_cn.set(keyname,data)
        return {"status":True,"data":""}

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
    def extract_old(key,config):
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
    # @task()
    # def extract_mongo(data_mongo,key,config,rs):
            
    #     list_cur = list(data_mongo)
    #     if len(list_cur)==0:
    #         return []

    #     json_data = dumps(list_cur, indent = 2)
    #     df_datamongo = pd.DataFrame(loads(json_data))
    #     df_datamongo_origin = pd.DataFrame(loads(json_data))
    #     # df_datamongo_origin = pd.DataFrame(json_data)
    #     # print(df_datamongo)
    #     # df_datamongo_origin = pd.DataFrame(json.loads(list_cur))
    #     df_datamongo = df_datamongo[config['mongo_normalization']].apply(pd.Series)
    #     df_datamongo[df_datamongo_origin.columns] = df_datamongo_origin
    #     del df_datamongo[config['mongo_normalization']]
    #     del df_datamongo['_id']
    #     df_datamongo = df_datamongo[df_datamongo.columns].add_prefix('mongo_')
    #     df_datamongo = generateConcatKey(df_datamongo,['mongo_'+config['primary_join_cols']['mongo']])
    #     df_datamongo = generateConcatKeySecondary(df_datamongo,config['secondary_join_cols']['mongo'])
    #     return json.loads(df_datamongo.to_json(orient='records'))
    #     # return {'data': df_old.to_json(orient='records'), 'status':200}


    @task()
    def extract_platform(config):
        try:
            if config['user']!="":
                response = requests.get(config['url'], auth=HTTPBasicAuth(config['user'],config['password']), verify=config['verify'],timeout=config['timeout'])
            else:
                response = requests.get(config['url'], verify=config['verify'],timeout=config['timeout'])
            response = response.text
            response = json.loads(response)
            print("here",response)
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
    def extract_mysql(engine,config):
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

        platform_data = pd.DataFrame(json.loads(comparate['platform_data']))
        if comparate['only_platform']=='empty':
            return {'exist_mysql':[],'not_exist_mysql':[]}

        comparate = pd.DataFrame(json.loads(comparate['only_platform']))
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
    


    # @task()
    # def comparate_primary_mongo(df_mongo,comparate):
    #     df_mongo = pd.DataFrame(df_mongo)
    #     platform_data = pd.DataFrame(json.loads(comparate['platform_data']))
    #     both = pd.DataFrame(json.loads(comparate['both']))
    #     try:
    #         comparate = pd.DataFrame(json.loads(comparate['both']))
    #     except:
    #         comparate = pd.DataFrame(columns=['concat_key_generate'])
    #     both = comparate

    #     if df_mongo.empty:
    #         df_mongo = pd.DataFrame(columns=['concat_key_generate'])
    #     both['exist_mongo'] = np.where(both['concat_key_generate'].isin(list(df_mongo['concat_key_generate'])) , 1, 0)
    #     exist_mongo_p = both[both['exist_mongo']==1]
    #     not_exist_mongo_p = both[both['exist_mongo']==0]
    #     exist_mongo_p = platform_data[platform_data['concat_key_generate'].isin(list(exist_mongo_p['concat_key_generate']))]
    #     not_exist_mongo_p = platform_data[platform_data['concat_key_generate'].isin(list(not_exist_mongo_p['concat_key_generate']))]

    #     if exist_mongo_p.empty:
    #         exist_mongo_p=[]
    #     else:
    #         exist_mongo_p=json.loads(exist_mongo_p.to_json(orient="records"))

    
    #     if not_exist_mongo_p.empty:
    #         not_exist_mongo_p=[]
    #     else:
    #         not_exist_mongo_p=json.loads(not_exist_mongo_p.to_json(orient="records"))
    #         # not_exist_mongo_p=not_exist_mongo_p.to_json(orient="records")

    #     print("exist_mongo_p")
    #     print(exist_mongo_p)
    #     print("not_exist_mongo_p")
    #     print(not_exist_mongo_p)

    #     # return exist_mongo_p.to_json(orient="records")
    #     return {'exist_mongo':exist_mongo_p,'not_exist_mongo':not_exist_mongo_p}


        

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

    # @task()
    # def comparate_secondary_mongo(df_mongo,comparate):
    #     df_mongo = pd.DataFrame(df_mongo)
    #     print("comparatecomparatecomparatecomparatecomparate")
    #     print(comparate)
        
    #     if df_mongo.empty:
    #         df_mongo = pd.DataFrame(columns=['concat_key_generate_secondary'])

    #     try:
    #         comparate = pd.DataFrame(comparate['exist_mongo'])
    #     except:
    #         comparate = pd.DataFrame(columns=['concat_key_generate_secondary'])
    #     # comparate = pd.DataFrame(json.loads(comparate))
    #     both = comparate
    #     # exist_mysql_p = comparate[comparate['exist_mysql']==1]
    #     try:
    #         both['exist_mongo_secondary'] = np.where(both['concat_key_generate_secondary'].isin(list(df_mongo['concat_key_generate_secondary'])) , 1, 0)
    #     except:
    #         return {'exist_mongo_secondary':[],'not_exist_mongo_secondary':[]}

    #     exist_mongo_s = both[both['exist_mongo_secondary']==1]
    #     not_exist_mongo_s = both[both['exist_mongo_secondary']==0]

    #     if exist_mongo_s.empty:
    #         exist_mongo_s = []
    #     else:
    #         exist_mongo_s = json.loads(exist_mongo_s.to_json(orient="records"))

    #     if not_exist_mongo_s.empty:
    #         not_exist_mongo_s = []
    #     else:
    #         not_exist_mongo_s = json.loads(not_exist_mongo_s.to_json(orient="records"))



    #     print("exist_")
    #     print(exist_mongo_s)
    #     print("notexist_")
    #     print(not_exist_mongo_s)
    #     # both = comparate[comparate['_merge_']=='both']
    #     # both['exist_mysql'] = np.where(both['concat_key_generate'].isin(list(df_mysql['concat_key_generate'])) , 1, 0)
    #     return {'exist_mongo_secondary':exist_mongo_s,'not_exist_mongo_secondary':not_exist_mongo_s}
    #     # return ['ok']




        
        # print("compara con mysql primary")
        # both = pd.DataFrame(both)
        # df_mysql = pd.DataFrame(df_mysql)
        # df_plat = pd.DataFrame(df_plat)
        # df_plat_vs_old = dataframe_difference(pd.DataFrame(df_plat['concat_key_generate']),pd.DataFrame(df_old['concat_key_generate']))
        # only_new = df_plat_vs_old['left']
        # only_old = df_plat_vs_old['right']
        # both = df_plat_vs_old['both']
        # return {'both':both.to_json(orient='records'),'left':only_new.to_json(orient='records'),'right':only_old.to_json(orient='records')}

    @task()
    def start():
        return ['ok']
    @task()
    def send_key_to_api(key_process):
        return ['ok']
    @task()
    def finish(response_verify):
        return ['ok']


    # [START main_flow]
    rs = start()
    key_process = str(config["platform_id"])+"-"+str(config["platform_name"])
    old_data = extract_old(key_process,config)
    platform_data = extract_platform(config)
    comp = comparate_old_vs_new(platform_data,old_data)
    mysql_data = extract_mysql(engine,config)
    # mongo_data = extract_mongo(data_mdb,key_process_mongo,config)
    primary_vs_mysql_equals = comparate_primary_mysql_equals(mysql_data,comp)
    secondary_vs_mysql_equals = comparate_secondary_mysql_equals(mysql_data,primary_vs_mysql_equals,comp)
    save_in_redis_result_equals = save_in_redis_data_equals_api(config,secondary_vs_mysql_equals,key_process+'-equals')
    send_key_redis_to_api_equals = send_key_to_api(key_process+'-equals')
    


    primary_vs_mysql_only_platform= comparate_primary_mysql_only_platform(mysql_data,comp)
    secondary_vs_mysql_only_platform = comparate_secondary_mysql_only_platform(mysql_data,primary_vs_mysql_only_platform,comp)
    save_in_redis_result_only_platform = save_in_redis_data_only_platform_api(config,secondary_vs_mysql_only_platform,key_process+'-onlyplatform')
    send_key_redis_to_api_only_platform = send_key_to_api(key_process+'-onlyplatform')
    

    primary_vs_mysql_only_old= comparate_primary_mysql_only_data_old(mysql_data,comp)
    save_in_redis_result_only_old = save_in_redis_data_only_old_api(config,primary_vs_mysql_only_old,key_process+'-onlyold')
    send_key_redis_to_api_only_old = send_key_to_api(key_process+'-onlyold')




    # primary_vs_mongo = comparate_primary_mongo(mongo_data,comp)
    # secondary_vs_mongo = comparate_secondary_mongo(mongo_data,primary_vs_mongo)
    save_in_redis_end = save_in_redis_data_old(config,platform_data,key_process)

    end = finish([{"status":True}])
    rs >> [platform_data,old_data] >> comp >> mysql_data >> [primary_vs_mysql_equals >> secondary_vs_mysql_equals >>  save_in_redis_result_equals >> send_key_redis_to_api_equals,primary_vs_mysql_only_platform >> secondary_vs_mysql_only_platform >> save_in_redis_result_only_platform >> send_key_redis_to_api_only_platform ,  primary_vs_mysql_only_old >> save_in_redis_result_only_old >> send_key_redis_to_api_only_old ] >> save_in_redis_end >> end

    # [END main_flow]


# [START dag_invocation]
puller_hughes = puller_hughes()
# [END dag_invocation]
