
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
import confluent_kafka
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
    'depends_on_past': False,
    'retry_delay': timedelta(seconds=10),
    # 'start_date': yesterday_at_elevenpm,
    # 'email': ['tech.team@industrydive.com'],
    # 'email_on_failure': True,
    # 'email_on_retry': True,
    'retries': 3
}
# [END default_args]
# start_date=days_ago(2)

# [START instantiate_dag]
# @dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['idirect_lima'])
@dag(default_args=default_args, schedule_interval='*/30 * * * *', start_date=datetime(2021, 7, 23, 9, 0), tags=['idirect_lima'])
def puller_idirect():
    # sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

    # import confluent_kafka
    # import kafka
    # from kafka.errors import KafkaError
    uri = "mongodb://bifrostProdUser:Maniac321.@cluster0-shard-00-00.bvdlk.mongodb.net:27017,cluster0-shard-00-01.bvdlk.mongodb.net:27017,cluster0-shard-00-02.bvdlk.mongodb.net:27017/myFirstDatabase?ssl=true&replicaSet=atlas-nn38a4-shard-0&authSource=admin&retryWrites=true&w=majority"
    conection = MongoClient(uri)
    # db_ = []

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


    #------------------------------------------------------------------------


        
    def verifyByGroup(groupid,topics):

        consumer = confluent_kafka.Consumer({'bootstrap.servers': "10.233.51.148:9092",'group.id': groupid})


        print("%-50s  %9s  %9s" % ("Topic [Partition]", "Committed", "Lag"))
        print("=" * 72)
        sum_lag = 0
        sum_msg = 0
        for topic in topics:
            # Get the topic's partitions
            metadata = consumer.list_topics(topic, timeout=10)
            if metadata.topics[topic].error is not None:
                raise confluent_kafka.KafkaException(metadata.topics[topic].error)

            # Construct TopicPartition list of partitions to query
            partitions = [confluent_kafka.TopicPartition(topic, p) for p in metadata.topics[topic].partitions]

            # Query committed offsets for this group and the given partitions
            committed = consumer.committed(partitions, timeout=10)

            for partition in committed:
                # Get the partitions low and high watermark offsets.
                (lo, hi) = consumer.get_watermark_offsets(partition, timeout=10, cached=False)

                if partition.offset == confluent_kafka.OFFSET_INVALID:
                    offset = "-"
                else:
                    offset = "%d" % (partition.offset)

                if hi < 0:
                    lag = "no hwmark"  # Unlikely
                elif partition.offset < 0:
                    # No committed offset, show total message count as lag.
                    # The actual message count may be lower due to compaction
                    # and record deletions.
                    lag = "%d" % (hi - lo)
                else:
                    lag = "%d" % (hi - partition.offset)
                sum_lag += int(lag)
                sum_msg += int(offset)
                
                print("%-50s  %9s  %9s" % (
                    "{} [{}]".format(partition.topic, partition.partition), offset, lag))


        consumer.close()
        return sum_lag

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
            
            
    @task()
    #------------------------------------------------------------------------
    def save_in_redis(config,data):
        df = pd.DataFrame(data)
        df.columns = df.columns.str.strip('platform_') 
        data = df.to_json(orient="records")
        key = str(config["platform_id"])+"-"+str(config["platform_name"])
    #ACA SE TIENE QUE HACER UNA FUNCIÓN QUE VALIDE LA CONEXIÓN CON REDIS, ACTUALMENTE
    # TIENE UN TRYCATCH QUE CAPTURA TODO TIPO DE ERROR CON EL REDIS, PERO DEBERÍA
    # VER UNA ESPECIFICAMENTE PARA VALIDAR CONEXIÓN
        try:
            redis_cn = redis.Redis(host= '10.233.1.101',    port= '6379',    password="tmCN3FwkP7")
            redis_cn.set(key,data)
            return {"status":True,"data":""}
        except:
            print("save in s3")
            response_save_s3 = ""
            # response_save_s3 = saveInS3(key,data)
            return {"status":False,"data":response_save_s3}
        return 

            
    # [START extract]
    @task()
    def extract_old(key,config,rs):
        redis_cn = redis.Redis(host= '10.233.1.101',    port= '6379',    password="tmCN3FwkP7")
        response = redis_cn.get(key)
        try:
            response = json.loads(response)
        except:
            return []
        df_old = pd.DataFrame(response)
        df_old = df_old[df_old.columns].add_prefix('old_')
        # df_old = generateConcatKey(df_old,[config['primary_join_cols']['old']])
        df_old = generateConcatKey(df_old,['old_'+config['primary_join_cols']['old']])
        df_old = generateConcatKeySecondary(df_old,config['secondary_join_cols']['old'])
        return [df_old.to_json(orient='records')]
        # return {'data': df_old.to_json(orient='records'), 'status':200}
    @task()
    def extract_mongo(data_mongo,key,config,rs):
            
        list_cur = list(data_mongo)
        if len(list_cur)==0:
            return []

        json_data = dumps(list_cur, indent = 2)
        df_datamongo = pd.DataFrame(loads(json_data))
        df_datamongo_origin = pd.DataFrame(loads(json_data))
        # df_datamongo_origin = pd.DataFrame(json_data)
        # print(df_datamongo)
        # df_datamongo_origin = pd.DataFrame(json.loads(list_cur))
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
    def send_queque(data,case):
        print(data)
        conf = {'bootstrap.servers': "10.233.51.148:9092"}
        p = Producer(conf)
        p.produce(case,data['not_exist_mongo'])
        p.flush()
        return [case]
    @task()
    def send_queque_kafka(data,case,key):
        print(data)
        # try:
        conf = {'bootstrap.servers': "10.233.51.148:9092"}
        p = Producer(conf)
        try:
            p.produce(case,json.dumps(data[key]))
        except:
            try:
                p.produce(case,data[key])
            except:
                print("error")
        p.flush()
        # except:
            # print("ERROR")
        return [case]
        # return {'data': df_old.to_json(orient='records'), 'status':200}



    @task()
    def extract_platform(config,rs):
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
                response = response[response.columns].add_prefix('platform_')
                # response = generateConcatKey(response,[config['primary_join_cols']['platform']])
                response = generateConcatKey(response,['platform_'+config['primary_join_cols']['platform']])
                response = generateConcatKeySecondary(response,config['secondary_join_cols']['platform'])
                response = response.to_json(orient='records')
                response = json.loads(response)
            except:
                print("ERROR IN route_trunk")
            # response = pd.DataFrame(response) 
            # response = response[response.columns].add_prefix('platform_')
                response = {}

        except requests.exceptions.RequestException as e:
            response = {}
            print("ERROR IN GET DATA PLATFORM")
        # return response.to_json(orient='records')
        return response




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
    def extract_mysql(engine,config,rs):
        query = "SELECT  * FROM "+str(config['mysql_table'])+" where status = 1 and  platformId = "+str(config['platform_id'])
        df_mysql_total = pd.read_sql_query(query, engine)
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
            print("here")
            data_platform=df1.to_json(orient="records")
            return {'platform_data':data_platform,'comparation':[],'both':data_platform,'only_platform':[],'only_old':[]}
        else:
            df2 = pd.DataFrame(json.loads(data_old[0]))

            
        # df1 = pd.DataFrame(df1['concat_key_generate'])
        # df2 = pd.DataFrame(df2['concat_key_generate'])
        comparation = df1.merge(
            df2,
            on="concat_key_generate",
            indicator="_merge_",
            how='outer'
        )
        print("------both")
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
            plat_send=both.to_json(orient="records")
            
        if old.empty:
            old_send="empty"
        else:
            old_send=both.to_json(orient="records")
        print(both_send)
        print("------both")
        data_platform=df1.to_json(orient="records")
        return {'platform_data':data_platform,'comparation':comparation.to_json(orient="records"),'both':both_send,'only_platform':plat_send,'only_old':old_send}


    @task()
    def comparate_primary_mysql(df_mysql,comparate):
        df_mysql = pd.DataFrame(json.loads(df_mysql))
        if df_mysql.empty:
            df_mysql = pd.DataFrame(columns=['concat_key_generate'])

        platform_data = pd.DataFrame(json.loads(comparate['platform_data']))
        comparate = pd.DataFrame(json.loads(comparate['both']))
        both = comparate
    # def comparate_primary_mysql(both,df_mysql,df_plat):
        both['exist_mysql'] = np.where(both['concat_key_generate'].isin(list(df_mysql['concat_key_generate'])) , 1, 0)
        exist_mysql_p = both[both['exist_mysql']==1]
        not_exist_mysql_p = both[both['exist_mysql']==0]
        exist_mysql_p = platform_data[platform_data['concat_key_generate'].isin(list(exist_mysql_p['concat_key_generate']))]
        # not_exist_mysql_p = platform_data[platform_data['concat_key_generate'].isin(list(not_exist_mysql_p['concat_key_generate']))]

        if exist_mysql_p.empty:
            exist_mysql_p=[]
        else:
            exist_mysql_p=json.loads(exist_mysql_p.to_json(orient="records"))


        if not_exist_mysql_p.empty:
            not_exist_mysql_p=[]
            # data_mysql_not_exist = []
        else:
            not_exist_mysql_p=json.loads(not_exist_mysql_p.to_json(orient="records"))
            # data_mysql_not_exist = df_mysql[df_mysql['concat_key_generate'].isin(list(not_exist_mysql_p['concat_key_generate']))]
        
        return {'exist_mysql':exist_mysql_p,'not_exist_mysql':not_exist_mysql_p}
    
    @task()
    def comparate_primary_mongo(df_mongo,comparate):
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
    # def comparate_primary_mysql(both,df_mysql,df_plat):
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
    def comparate_secondary_mysql(df_mysql,comparate):
        try:
            comparate = pd.DataFrame(comparate['exist_mysql'])
        except:
            comparate = pd.DataFrame(columns=['concat_key_generate_secondary'])
        df_mysql = pd.DataFrame(json.loads(df_mysql))
        
        if df_mysql.empty:
            df_mysql = pd.DataFrame(columns=['concat_key_generate_secondary'])

        both = comparate
        # exist_mysql_p = comparate[comparate['exist_mysql']==1]
        both['exist_mysql_secondary'] = np.where(both['concat_key_generate_secondary'].isin(list(df_mysql['concat_key_generate_secondary'])) , 1, 0)

        exist_mysql_s = both[both['exist_mysql_secondary']==1]
        not_exist_mysql_s = both[both['exist_mysql_secondary']==0]
        not_exist_mysql_s_com = both[both['exist_mysql_secondary']==0]
        print("exist_")
        print(exist_mysql_s)
        print("notexist_")
        print(not_exist_mysql_s)
        if exist_mysql_s.empty:
            exist_mysql_s = []
        else:
            exist_mysql_s = json.loads(exist_mysql_s.to_json(orient="records"))

        if not_exist_mysql_s.empty:
            not_exist_mysql_s = []
            data_mysql_not_exist_s = []
        else:
            # not_exist_mysql_s = json.loads(not_exist_mysql_s.to_json(orient="records"))
            data_mysql_not_exist_s = df_mysql[df_mysql['concat_key_generate'].isin(list(not_exist_mysql_s_com['concat_key_generate']))]
            data_mysql_not_exist_s = pd.merge(not_exist_mysql_s_com, data_mysql_not_exist_s, on="concat_key_generate")
            data_mysql_not_exist_s = json.loads(data_mysql_not_exist_s.to_json(orient="records"))
        
        # both = comparate[comparate['_merge_']=='both']
    # def comparate_primary_mysql(both,df_mysql,df_plat):
        # both['exist_mysql'] = np.where(both['concat_key_generate'].isin(list(df_mysql['concat_key_generate'])) , 1, 0)
        return {'exist_mysql_secondary':exist_mysql_s,'not_exist_mysql_secondary':data_mysql_not_exist_s}
        # return ['ok']

    @task()
    def comparate_secondary_mongo(df_mongo,comparate):
        df_mongo = pd.DataFrame(df_mongo)
        print("comparatecomparatecomparatecomparatecomparate")
        print(comparate)
        
        if df_mongo.empty:
            df_mongo = pd.DataFrame(columns=['concat_key_generate_secondary'])

        try:
            comparate = pd.DataFrame(comparate['exist_mongo'])
        except:
            comparate = pd.DataFrame(columns=['concat_key_generate_secondary'])
        # comparate = pd.DataFrame(json.loads(comparate))
        both = comparate
        # exist_mysql_p = comparate[comparate['exist_mysql']==1]
        both['exist_mongo_secondary'] = np.where(both['concat_key_generate_secondary'].isin(list(df_mongo['concat_key_generate_secondary'])) , 1, 0)

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



        print("exist_")
        print(exist_mongo_s)
        print("notexist_")
        print(not_exist_mongo_s)
        # both = comparate[comparate['_merge_']=='both']
    # def comparate_primary_mysql(both,df_mysql,df_plat):
        # both['exist_mysql'] = np.where(both['concat_key_generate'].isin(list(df_mysql['concat_key_generate'])) , 1, 0)
        return {'exist_mongo_secondary':exist_mongo_s,'not_exist_mongo_secondary':not_exist_mongo_s}
        # return ['ok']




        
        # print("compara con mysql primary")
        # both = pd.DataFrame(both)
        # df_mysql = pd.DataFrame(df_mysql)
        # df_plat = pd.DataFrame(df_plat)
        # df_plat_vs_old = dataframe_difference(pd.DataFrame(df_plat['concat_key_generate']),pd.DataFrame(df_old['concat_key_generate']))
        # only_new = df_plat_vs_old['left']
        # only_old = df_plat_vs_old['right']
        # both = df_plat_vs_old['both']
        # return {'both':both.to_json(orient='records'),'left':only_new.to_json(orient='records'),'right':only_old.to_json(orient='records')}


    # [END extract]

    # [START start]
    @task()
    def start():
        """
        #### Load task
        A simple Load task which takes in the result of the Transform task and
        instead of saving it to end user review, just prints it out.
        """
        print("START")
        return ['ok']
    # [END start ]

    # [END]
    # [START finish]
    @task()
    def finish(response_verify):
        """
        #### Load task
        A simple Load task which takes in the result of the Transform task and
        instead of saving it to end user review, just prints it out.
        """
        print("FINISH")
        return ['ok']
    # [END finish]














    # [START verify]
    @task()
    def verify(rs):
        v_mysql = verifyByGroup('mysql', ['insertmysql','updatemysql'])
        v_mongo = verifyByGroup('mongo', ['insertmongo','updatemongotimep','updatemongo'])
        v_total = v_mysql + v_mongo
        if v_total > 0:
            return None
        return []
    # [END finish]


    # [START main_flow]
    rs = start()
    response_verify = verify(rs)
    if response_verify is None:
        finish(response_verify)
        return 'ok'
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
          "mysql": "id_nms",
          "mongo": "ID",
          "platform": "ID",
          "old": "ID"
        },
        "secondary_join_cols": {
          "mysql": [
            "mysql_siteId",
            "mysql_esn",
            "mysql_did"
          ],
          "mongo": [
            "mongo_Name",
            "mongo_SN",
            "mongo_DID"
          ],
          "platform": [
            "platform_Name",
            "platform_SN",
            "platform_DID"
          ],
          "old": [
            "old_Name",
            "old_SN",
            "old_DID"
          ]
        },
        "platform_name": "idirect_lima"
      }
    ]
    config = config[0]
    db_ = conection["bifrost"]
    coltn_mdb = db_["idirect_test_lima"]
    data_mdb = coltn_mdb.find({'platform':2})


    key_process = str(config["platform_id"])+"-"+str(config["platform_name"])
    platform_data = extract_platform(config,response_verify)
    old_data = extract_old(key_process,config,response_verify)
    comp = comparate_old_vs_new(platform_data,old_data)
    #OBTENER LOS BOTH EN EL KAFKA
    send_qq_new_mysql= send_queque_kafka(comp,'insertmysql','only_platform') 
    send_qq_new_mongo= send_queque_kafka(comp,'insertmongo','only_platform') 
    # send_qq_delete_mysql= send_queque_kafka(comp,'deletemysql','only_old') 
    # send_qq_delete_mongo= send_queque_kafka(comp,'deletemongo','only_old') 
    
    mysql_data = extract_mysql(engine,config,response_verify)
    primary_vs_mysql = comparate_primary_mysql(mysql_data,comp)
    send_qq_insert_vsmysql= send_queque_kafka(primary_vs_mysql,'insertmysql','not_exist_mysql') 
    secondary_vs_mysql = comparate_secondary_mysql(mysql_data,primary_vs_mysql)
    send_qq= send_queque_kafka(secondary_vs_mysql,'updatemysql','not_exist_mysql_secondary') 

    key_process_mongo = key_process
    mongo_data = extract_mongo(data_mdb,key_process_mongo,config,response_verify)
    primary_vs_mongo = comparate_primary_mongo(mongo_data,comp)
    send_qq_insert_vsmongo= send_queque_kafka(primary_vs_mongo,'insertmongo','not_exist_mongo') 
  
    secondary_vs_mongo = comparate_secondary_mongo(mongo_data,primary_vs_mongo)
    send_qq_mongo= send_queque_kafka(secondary_vs_mongo,'updatemongo','not_exist_mongo_secondary') 
    send_qq_mongo_timep= send_queque_kafka(secondary_vs_mongo,'updatemongotimep','exist_mongo_secondary') 
    save_in_redis_end = save_in_redis(config,platform_data)
    end = finish([{"status":True}])
    [secondary_vs_mysql,secondary_vs_mongo] >> save_in_redis_end >> end


    # [END main_flow]


# [START dag_invocation]
puller_idirect = puller_idirect()
# [END dag_invocation]

# [END tutorial]
