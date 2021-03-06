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
from airflow.utils.edgemodifier import Label

# from airflow.operators.bash_operator import BashOperator


from airflow.utils.trigger_rule import TriggerRule
from requests.auth import HTTPBasicAuth
import pandas as pd
from pandas.io.json import json_normalize

# subprocess.check_call([sys.executable, "-m", "pip3", "install", "confluent_kafka"])
# subprocess.check_call([sys.executable, "-m", "pip", "install", "kafka"])
# import pymongo
from bson.json_util import dumps, loads
from functools import reduce
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import numpy as np
from sqlalchemy.sql.expression import exists, null
from pymongo import MongoClient
import xmltodict, json

uri = "mongodb://bifrostProdUser:Maniac321.@cluster0-shard-00-00.bvdlk.mongodb.net:27017,cluster0-shard-00-01.bvdlk.mongodb.net:27017,cluster0-shard-00-02.bvdlk.mongodb.net:27017/myFirstDatabase?ssl=true&replicaSet=atlas-nn38a4-shard-0&authSource=admin&retryWrites=true&w=majority"
conection = MongoClient(uri, connect=False)
uri_2 = "mongodb://bifrostProdUser:Manaic321.@192.168.36.24:27017/bifrost"
conection_2 = MongoClient(uri_2, connect=False)


collection_puller = "gilat_tasa_test"
table_mysql_puller = "bifrost_terminal_full"
table_mysql_serviceplan = "mnos_serviceplan"
tag_airflow = "gilat"
platform_name = "gilat_tasa_10min"
platform_id_puller = 52
history_collection_mongo="history_changes_test_gilat"



db_2 = conection_2["bifrost"]

db_ = conection["bifrost"]
coltn_mdb = db_[collection_puller]

r = redis.Redis(host= 'redis-redis-ha-haproxy.redis-dev.svc.cluster.local',    port= '6379',    password="Maniac321")

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
    "owner": "airflow",
    "retry_delay": timedelta(seconds=10),
    "start_date": days_ago(1),
    "max_active_runs": 1,
    # "concurrency": 2,
    'trigger_rule': 'all_done',
    "wait_for_downstream":False,
    "depends_on_past":False,
    "retries": 5,
}
# [END default_args]
# start_date=days_ago(2)
time_send_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# [START instantiate_dag]
@dag(default_args=default_args, schedule_interval=None, tags=[tag_airflow],catchup=False)
# @dag(default_args=default_args, schedule_interval="*/15 * * * *", tags=[tag_airflow])
def puller_gilat_tasa_10min():

    # sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

    # import confluent_kafka
    # import kafka
    # from kafka.errors import KafkaError

    # db_ = []

    # config = open("config.json","r")
    # config = json.loads(config.read())
    # config = config[0]

    engine = create_engine(
        "mysql://admin:Maniac321.@bifrosttiws-instance-1.cn4dord7rrni.us-west-2.rds.amazonaws.com/bifrostprod10dev?charset=utf8",
        connect_args={"connect_timeout": 120},
    )
    config = [
        {
            
            "concat_columns_api": [],
            "route_trunk": "",
            # "route_trunk": "soap:Envelope-soap:Body-ns2:getCPEsByManagedGroupResponse-return-cpes-ns3:CPE",
            "url": "http://192.168.36.50:8082/ws/cpeService?wsdl",
            "headers":{
            'Content-Type': 'text/xml',
            'Authorization': 'Basic YXBpczphcGlzMjAyMA=='
            },
            "payload":"<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:com=\"com.gilat.ngnms.server.services.ws.cfg.face\">\r\n    <soapenv:Header/>\r\n    <soapenv:Body>\r\n        <com:getCPEsByManagedGroup>\r\n            <managedGroupId>2</managedGroupId>\r\n            <lastIndex>0</lastIndex>\r\n        </com:getCPEsByManagedGroup>\r\n    </soapenv:Body>",
            "user": "",
            "password": "",
            "timeout": 120,
            "verify": "False",
            "platform_id": platform_id_puller,
            "mysql_table": table_mysql_puller,
            "mongo_normalization": "puller",
            "mongo_limit_time": 55,
            "mongo_collection": collection_puller,
            "primary_join_cols": {
                "mysql": "siteId",
                "mongo": "description",
                "platform": "description",
                "old": "description",
            },
            "secondary_join_cols": {
                "mysql": ["mysql_statusTerminal","mysql_suscriberId","mysql_crmId","mysql_mac"],
                "mongo": ["mongo_operationalState","mongo_cpeId-subscriberId","mongo_slaName","mongo_macAddress"],
                "platform": ["platform_operationalState","platform_cpeId-subscriberId","platform_slaName","platform_macAddress"],
                "old": ["old_operationalState","old_cpeId-subscriberId","old_slaName","old_macAddress"],
          
                # "mysql": ["mysql_siteId", "mysql_esn", "mysql_did","mysql_modeltype","mysql_inroutegroupId","mysql_networkId","mysql_latitud","mysql_longitud","mysql_crmId","mysql_statusTerminal"],
                # "mongo": ["mongo_Name", "mongo_SN", "mongo_DID","mongo_ModelType","mongo_InrouteGroupID","mongo_NetworkID","mongo_Lat","mongo_Lon","mongo_SERVICEPLANCRMID","mongo_Active"],
                # "platform": ["platform_Name", "platform_SN", "platform_DID","platform_ModelType","platform_InrouteGroupID","platform_NetworkID","platform_Lat","platform_Lon","platform_SERVICEPLANCRMID","platform_Active"],
                # "old": ["old_Name", "old_SN", "old_DID","old_ModelType","old_InrouteGroupID","old_NetworkID","old_Lat","old_Lon","old_SERVICEPLANCRMID","old_Active"],
            },
            "platform_name": platform_name,
        }
    ]
    
    

    config = config[0]

    def getDataOld(principal_key):
        data = coltn_mdb.find({"siteId": str(principal_key)})
        list_cur = list(data)
        # print(list_cur,principal_key)
        #         json_data =json.loads(dumps(list_cur, indent = 2))
        return list_cur

    def dateSaveHistory(data):
        coltn_history_changes = db_2[history_collection_mongo]
        data_old = getDataOld(data["principal_key"])
        element = {
            "data": [],
            "data_old": data_old,
            "changes": data["changes"],
            "type": data["type"],
            "date_p": time_send_now,
            "platform_id": platform_id_puller,
            "principalKey": data["principal_key"],
        }
        coltn_history_changes.insert_one(element)
        return ["ok"]

    def dateSaveHistoryInsertMongo(data_global):
        coltn_history_changes = db_2[history_collection_mongo]
        for data in data_global:
            element = {
                "data": [],
                "data_old": [],
                "changes": data,
                "type": "insert_mongo",
                "date_p": time_send_now,
                "platform_id": platform_id_puller,
                "principalKey": data["siteId"],
            }
            coltn_history_changes.insert(element)
        return ["ok"]

    def dateSaveHistoryInsert(data_global):
        coltn_history_changes = db_2[history_collection_mongo]
        for data in data_global:
            element = {
                "data": [],
                "data_old": [],
                "changes": data,
                "type": "insert_mysql",
                "date_p": time_send_now,
                "platform_id": platform_id_puller,
                "principalKey": data["platform_ID"],
            }
            coltn_history_changes.insert(element)
        return ["ok"]

    def dateSaveHistoryUpdate(data_global):
        coltn_history_changes = db_2[history_collection_mongo]
        # data_old = getDataOld(data_global['principal_key'])
        for data in data_global:
            element = {
                "data": [],
                "data_old": [],
                "changes": data,
                "type": "update_mysql",
                "date_p": time_send_now,
                "platform_id": platform_id_puller,
                "principalKey": data["platform_ID"],
            }
            coltn_history_changes.insert(element)
        return ["ok"]

    def dateSaveHistoryUpdateMongo(data_global):
        coltn_history_changes = db_2[history_collection_mongo]
        # data_old = getDataOld(data_global['principal_key'])
        for data in data_global:
            print("dataaaaaaaa",data)
            element = {
                "data": [],
                "data_old": {
                    "did": data["mongo_DID"],
                    "sn": data["mongo_SN"],
                    "active": str(data["mongo_Active"]),
                    "id": data["mongo_ID"],
                    
                    "modelType": data["mongo_ModelType"],
                    "inrouteGroupID": data["mongo_InrouteGroupID"],
                    "networkID": data["mongo_NetworkID"],
                    "lat": data["mongo_Lat"],
                    "lon": data["mongo_Lon"],
                    "serviceplan": data["mongo_SERVICEPLANCRMID"],
                                       
                },
                "changes": {
                    "did": data["DID"],
                    "sn": data["SN"],
                    "active": str(data["Active"]),
                    "id": data["ID"],
                    
                    "modelType": data["ModelType"],
                    "inrouteGroupID": data["InrouteGroupID"],
                    "networkID": data["NetworkID"],
                    "lat": data["Lat"],
                    "lon": data["Lon"],
                    "serviceplan": data["SERVICEPLANCRMID"],
                    
                    
                },
                "type": "update_mongo",
                "date_p": time_send_now,
                "platform_id": platform_id_puller,
                "principalKey": data["ID"],
            }
            coltn_history_changes.insert(element)
        return ["ok"]

    def generateConcatKeySecondary(df, cols):
        # try:
        df_stnd_key = df[cols].astype(str)
        for col in cols:
            if col == "platform_SN":
                df_stnd_key[col] = df_stnd_key[col].map(
                    lambda eve: eve.replace(".0", "")
                )
                df[col] = df_stnd_key[col]
            if col == "mongo_SN":
                df_stnd_key[col] = df_stnd_key[col].map(
                    lambda eve: eve.replace(".0", "")
                )
                df[col] = df_stnd_key[col]
            df_stnd_key[col] = df_stnd_key[col].map(
                lambda eve: eve.replace("0.0000000000000000", " ")
            )
        df_stnd_key["concat_key_generate_secondary"] = df_stnd_key[cols].agg(
            "-".join, axis=1
        )
        df["concat_key_generate_secondary"] = df_stnd_key[
            "concat_key_generate_secondary"
        ]
        return df
        # except:
            # print("ERROR IN COLUMNS")

    def generateConcatKey(df, cols):
        try:
            df_stnd_key = df[cols].astype(str)
            df_stnd_key["concat_key_generate"] = df_stnd_key[cols].agg("-".join, axis=1)
            df["concat_key_generate"] = df_stnd_key["concat_key_generate"]
            return df
        except:
            print("ERROR IN COLUMNS PRIMARY")

    # @task()
    def valid_exist_puller_runing():
        key_redis = None
        query = f"SELECT * FROM puller_cron_platform where status=1 and status_cron=2  limit 1 "
        df = pd.read_sql_query(query, engine)
        data = json.loads(df.to_json(orient="records"))
        if len(data) == 0:
            key_redis = "start"
        else:
            key_redis = "finish_process"

        return key_redis

    def getDataRedisByKey(key):
        data = r.get(key)
        data = json.loads(data)
        return data

    def getDataMysqlBySiteId(siteId):
        # engine = create_engine("mysql://admin:Maniac321.@bifrosttiws-instance-1.cn4dord7rrni.us-west-2.rds.amazonaws.com/bifrostprod10dev")
        query = f"select * from {table_mysql_puller} where siteId ='{siteId}' and status != 0 and platformId = {platform_id_puller}"
        df = pd.read_sql_query(query, engine)
        try:
            id_response = json.loads(df.to_json(orient="records"))[0]["id"]
            return {"btId": str(id_response), "mysqlFlag": 1}
        except:
            return {"btId": 0, "mysqlFlag": 0}
    # def getAllDataFromTerminals(data):




    def generateColumns(data,config):
        print("init generate columns")
        if len(data)<=0:
            return data
        data_orii = data
        name_column_concat = ''
        for concat in config['concat_columns_api']:
            name_column_concat =concat['name_column'] 
            data[name_column_concat]= ''
            for x in  concat['cols']:
                if x['type'] == 'key':
                    try:
                        multi_routes_concat = x['data'].split('---')
                        try:
                            new =  data[multi_routes_concat[0]].apply(pd.Series)[multi_routes_concat[1]].astype("str").copy()
                            # print(new)
                            
                            new = pd.to_numeric(new, downcast="integer").astype("str")
                            new =  new.map(lambda eve: str(eve).replace(".0",""))
                        except:
                            
                            new = data[multi_routes_concat[0]].astype("str").copy()
                            new = pd.to_numeric(new, downcast="integer").astype("str")
                            new =  new.map(lambda eve: str(eve).replace(".0",""))

                        data[name_column_concat]= data[name_column_concat].str.cat(new, sep ='')
                    except:
                        new = ''
                        data[name_column_concat]=  data[name_column_concat] + new 

                elif x['type'] == 'calculate':
                    new = x['data']
                    evaluate = []
                    data_orii['operation'] = x['data']['operation']
                    for xx in x['data']['columns']:
                        multi_routes_concat_calculate = xx['key'].split('---')
                        try:
                            new_calculate =  data_orii[multi_routes_concat_calculate[0]].apply(pd.Series)[multi_routes_concat_calculate[1]].astype("str").copy()
                        except:
                            new_calculate = data_orii[multi_routes_concat_calculate[0]].astype("str").copy()

                        data_orii['cc'] = data_orii.index
                        data_orii['operation'] =   data_orii.apply(lambda evaluate: evaluate['operation'].replace(xx['mask'],str(new_calculate[evaluate['cc']])),axis=1)
                        data_orii['operation'] =  data_orii['operation'].map(lambda ccal: str(eval(ccal)))
                        
                    new = data_orii['operation']
                    new =  new.map(lambda eve: str(eve).replace(".0",""))

    #                 new = pd.to_numeric(data_orii['operation'], downcast="integer")
                    data[name_column_concat]=  data[name_column_concat] + new
                else:
                    new = x['data']
                    data[name_column_concat]=  data[name_column_concat] + new 
                
        return data

    @task()
    # ------------------------------------------------------------------------
    def save_in_redis_data_old(config, data, key_process):
        df = pd.DataFrame(data)
        df.columns = df.columns.str.replace("platform_", "")
        del df["concat_key_generate"]
        del df["concat_key_generate_secondary"]
        data = df.to_json(orient="records")
        redis_cn = redis.Redis(host= 'redis-redis-ha-haproxy.redis-dev.svc.cluster.local',    port= '6379',    password="Maniac321")
        redis_cn.set("1-"+platform_name, data)
        return {"status": True, "data": ""}

    @task()
    # ------------------------------------------------------------------------
    def save_in_redis_data_platform(data):
        redis_cn = redis.Redis(host= 'redis-redis-ha-haproxy.redis-dev.svc.cluster.local',    port= '6379',    password="Maniac321")
        try:
            data_send = json.dumps(data)
        except:
            data_send = data
        redis_cn.set("puller_"+platform_name, data_send)
        return {"status": True, "data": ""}

    @task()
    # ------------------------------------------------------------------------
    def save_in_redis_data_equals_api(config, data, keyname):

        # redis_cn = redis.Redis(host= '192.168.29.20',    port= '6379',    password="bCL3IIuAwv")
        key_insert = keyname + "-insert"
        key_update = keyname + "-update"
        key_delete = keyname + "-delete"
        try:
            data_insert = json.dumps(data["insert_mysql"])
            r.set(key_insert, data_insert)
            r.expire(key_insert, timedelta(days=1))
        except:
            data_insert = data["insert_mysql"]
            r.set(key_insert, data_insert)
            r.expire(key_insert, timedelta(days=1))

        try:
            data_update = json.dumps(data["update_mysql"])
            r.set(key_update, data_update)
            r.expire(key_update, timedelta(days=1))
        except:
            data_update = data["update_mysql"]
            r.set(key_update, data_update)
            r.expire(key_update, timedelta(days=1))

        try:
            data_delete = json.dumps(data["delete_mysql"])
            r.set(key_delete, data_delete)
            r.expire(key_delete, timedelta(days=1))

        except:
            data_delete = data["delete_mysql"]
            r.set(key_delete, data_delete)
            r.expire(key_delete, timedelta(days=1))

        return {
            "status": True,
            "key_insert": key_insert,
            "key_update": key_update,
            "key_delete": key_delete,
        }

    @task()
    # ------------------------------------------------------------------------
    def save_in_redis_data_only_platform_api(config, data, keyname):
        # redis_cn = redis.Redis(host= '192.168.29.20',    port= '6379',    password="bCL3IIuAwv")
        key_insert = keyname + "-onlyplat-insert"
        key_update = keyname + "-onlyplat-update"
        key_delete = keyname + "-onlyplat-delete"
        try:
            data_insert = json.dumps(data["insert_mysql"])
            r.set(key_insert, data_insert)
            r.expire(key_insert, timedelta(days=1))
        except:
            data_insert = data["insert_mysql"]
            r.set(key_insert, data_insert)
            r.expire(key_insert, timedelta(days=1))

        try:
            data_update = json.dumps(data["update_mysql"])
            r.set(key_update, data_update)
            r.expire(key_update, timedelta(days=1))
        except:
            data_update = data["update_mysql"]
            r.set(key_update, data_update)
            r.expire(key_update, timedelta(days=1))

        try:
            data_delete = json.dumps(data["delete_mysql"])
            r.set(key_delete, data_delete)
            r.expire(key_delete, timedelta(days=1))
        except:
            data_delete = data["delete_mysql"]
            r.set(key_delete, data_delete)
            r.expire(key_delete, timedelta(days=1))

        return {
            "status": True,
            "key_insert": key_insert,
            "key_update": key_update,
            "key_delete": key_delete,
        }

    @task()
    def save_in_redis_data_only_old_api(config, data, keyname):
        key_insert = keyname + "-onlyold-insert"
        key_update = keyname + "-onlyold-update"
        key_delete = keyname + "-onlyold-delete"
        try:
            data_insert = json.dumps(data["insert_mysql"])
            r.set(key_insert, data_insert)
            r.expire(key_insert, timedelta(days=1))
        except:
            data_insert = data["insert_mysql"]
            r.set(key_insert, data_insert)
            r.expire(key_insert, timedelta(days=1))

        try:
            data_update = json.dumps(data["update_mysql"])
            r.set(key_update, data_update)
            r.expire(key_update, timedelta(days=1))
        except:
            data_update = data["update_mysql"]
            r.set(key_update, data_update)
            r.expire(key_update, timedelta(days=1))

        try:
            data_delete = json.dumps(data["delete_mysql"])
            r.set(key_delete, data_delete)
            r.expire(key_delete, timedelta(days=1))
        except:
            data_delete = data["delete_mysql"]
            r.set(key_delete, data_delete)
            r.expire(key_delete, timedelta(days=1))

        return {
            "status": True,
            "key_insert": key_insert,
            "key_update": key_update,
            "key_delete": key_delete,
        }

    @task()
    def extract_old(key, config, valid_puller_runing):
        if valid_puller_runing is None:
            return []
        try:
            redis_cn = redis.Redis(
                host= 'redis-redis-ha-haproxy.redis-dev.svc.cluster.local',    port= '6379',    password="Maniac321"
            )
            response = redis_cn.get("1-"+platform_name)
            response = json.loads(response)
            df_old = pd.DataFrame(response)


            df_old['Lat'] = df_old['Lat'].astype(str)
            df_old['Lon'] = df_old['Lon'].astype(str)
            #TD df_old = df_old.astype(str)
            df_old = df_old[df_old.columns].add_prefix("old_")
            if df_old is None:
                return []
            df_old = generateConcatKey(
                df_old, ["old_" + config["primary_join_cols"]["old"]]
            )
            df_old = generateConcatKeySecondary(
                df_old, config["secondary_join_cols"]["old"]
            )
            if df_old is None:
                return []
            return [df_old.to_json(orient="records")]
        except:
            return []

    @task()
    # ------------------------------------------------------------------------
    def save_in_redis_data_only_platform_mongo_api(config, data, keyname):
        key_insert = keyname + "-onlyplatform-insert"
        key_update = keyname + "-onlyplatform-update"
        key_delete = keyname + "-onlyplatform-delete"
        try:
            data_insert = json.dumps(data["insert_mongo"])
            r.set(key_insert, data_insert)
            r.expire(key_insert, timedelta(days=1))
        except:
            data_insert = data["insert_mongo"]
            r.set(key_insert, data_insert)
            r.expire(key_insert, timedelta(days=1))

        try:
            data_update = json.dumps(data["update_mongo"])
            r.set(key_update, data_update)
            r.expire(key_update, timedelta(days=1))
        except:
            data_update = data["update_mongo"]
            r.set(key_update, data_update)
            r.expire(key_update, timedelta(days=1))

        try:
            data_delete = json.dumps(data["delete_mongo"])
            r.set(key_delete, data_delete)
            r.expire(key_delete, timedelta(days=1))
        except:
            data_delete = data["delete_mongo"]
            r.set(key_delete, data_delete)
            r.expire(key_delete, timedelta(days=1))

        return {
            "status": True,
            "key_insert": key_insert,
            "key_update": key_update,
            "key_delete": key_delete,
        }

    @task()
    # ------------------------------------------------------------------------
    def save_in_redis_data_only_old_mongo_api(config, data, keyname):
        key_insert = keyname + "-onlyold-insert"
        key_update = keyname + "-onlyold-update"
        key_delete = keyname + "-onlyold-delete"
        try:
            data_insert = json.dumps(data["insert_mongo"])
            r.set(key_insert, data_insert)
            r.expire(key_insert, timedelta(days=1))
        except:
            data_insert = data["insert_mongo"]
            r.set(key_insert, data_insert)
            r.expire(key_insert, timedelta(days=1))

        try:
            data_update = json.dumps(data["update_mongo"])
            r.set(key_update, data_update)
            r.expire(key_update, timedelta(days=1))
        except:
            data_update = data["update_mongo"]
            r.set(key_update, data_update)
            r.expire(key_update, timedelta(days=1))

        try:
            data_delete = json.dumps(data["delete_mongo"])
            r.set(key_delete, data_delete)
            r.expire(key_delete, timedelta(days=1))
        except:
            data_delete = data["delete_mongo"]
            r.set(key_delete, data_delete)
            r.expire(key_delete, timedelta(days=1))

        return {
            "status": True,
            "key_insert": key_insert,
            "key_update": key_update,
            "key_delete": key_delete,
        }

    @task()
    # ------------------------------------------------------------------------
    def save_in_redis_data_equals_mongo_api(config, data, keyname):
        key_insert = keyname + "-equals-insert"
        key_update = keyname + "-equals-update"
        key_delete = keyname + "-equals-delete"
        try:
            data_insert = json.dumps(data["insert_mongo"])
            r.set(key_insert, data_insert)
            r.expire(key_insert, timedelta(days=1))
        except:
            data_insert = data["insert_mongo"]
            r.set(key_insert, data_insert)
            r.expire(key_insert, timedelta(days=1))

        try:
            data_update = json.dumps(data["update_mongo"])
            r.set(key_update, data_update)
            r.expire(key_update, timedelta(days=1))
        except:
            data_update = data["update_mongo"]
            r.set(key_update, data_update)
            r.expire(key_update, timedelta(days=1))

        try:
            data_delete = json.dumps(data["delete_mongo"])
            r.set(key_delete, data_delete)
            r.expire(key_delete, timedelta(days=1))
        except:
            data_delete = data["delete_mongo"]
            r.set(key_delete, data_delete)
            r.expire(key_delete, timedelta(days=1))

        return {
            "status": True,
            "key_insert": key_insert,
            "key_update": key_update,
            "key_delete": key_delete,
        }

    @task()
    def extract_mongo(config, valid_puller_runing):
        if valid_puller_runing is None:
            return []
        # from pymongo import MongoClient
        # uri = "mongodb://bifrostProdUser:Maniac321.@cluster0-shard-00-00.bvdlk.mongodb.net:27017,cluster0-shard-00-01.bvdlk.mongodb.net:27017,cluster0-shard-00-02.bvdlk.mongodb.net:27017/myFirstDatabase?ssl=true&replicaSet=atlas-nn38a4-shard-0&authSource=admin&retryWrites=true&w=majority"
        # conection = MongoClient(uri,connect=False)
        # db_ = conection["bifrost"]
        # coltn_mdb = db_['hughes_test']
        data_mdb = coltn_mdb.find(
            {"platform":platform_id_puller,"active":1},
            # {"active": 1},
            {
                "_id": True,
                "siteId": True,
                "puller.operationalState": True,
                "puller.description": True,
                "puller.cpeId": True,
                "puller.slaName": True,
                "puller.macAddress": True,
                "status": True
            },
        )
        
        list_cur = list(data_mdb)
        if len(list_cur) == 0:
            return []

        json_data = dumps(list_cur, indent=2)
        # df_datamongo = pd.DataFrame(data_mdb)
        # df_datamongo_origin = pd.DataFrame(data_mdb)
        # json_data = dumps(list_cur, indent = 2)
        df_datamongo = pd.DataFrame(loads(json_data))
        
        # df_datamongo_origin = pd.DataFrame(
        # df_datamongo_origin = pd.DataFrame(json_data)
        # print(df_datamongo)
        df_datamongo_origin = pd.DataFrame(json.loads(json_data))
        df_datamongo = df_datamongo[config["mongo_normalization"]].apply(pd.Series)
        df_datamongo[df_datamongo_origin.columns] = df_datamongo_origin
        del df_datamongo[config["mongo_normalization"]]
        del df_datamongo["_id"]
        print(df_datamongo.columns)
        df_datamongo['cpeId-subscriberId'] =  df_datamongo["cpeId"].map(
                        lambda eve: json.loads(eve['subscriberId'])
                    )
        print(df_datamongo['cpeId-subscriberId'], 'mongo')
        
        # try:
        # del df_datamongo['concat_key_generate']
        # del df_datamongo['concat_key_generate_secondary']
        # except:
        # print("error delete")
        # df_datamongo['Lat'] = df_datamongo['Lat'].astype(str)
        # df_datamongo['Lon'] = df_datamongo['Lon'].astype(str)
        df_datamongo = generateColumns(df_datamongo,config)
        print(df_datamongo,' generateColumnsresponsegenerateColumnsresponse')
        df_datamongo = df_datamongo[df_datamongo.columns].add_prefix("mongo_")
        df_datamongo = generateConcatKey(
            df_datamongo, ["mongo_" + config["primary_join_cols"]["mongo"]]
        )
        df_datamongo = generateConcatKeySecondary(
            df_datamongo, config["secondary_join_cols"]["mongo"]
        )
        return json.loads(df_datamongo.to_json(orient="records"))
        # return {'data': df_old.to_json(orient='records'), 'status':200}

    @task()
    def save_in_history_mongo(config):
        # uri = "mongodb://bifrostProdUser:Maniac321.@cluster0-shard-00-00.bvdlk.mongodb.net:27017,cluster0-shard-00-01.bvdlk.mongodb.net:27017,cluster0-shard-00-02.bvdlk.mongodb.net:27017/myFirstDatabase?ssl=true&replicaSet=atlas-nn38a4-shard-0&authSource=admin&retryWrites=true&w=majority"
        # conection = MongoClient(uri)
        # db_ = conection["bifrost"]

        # from pymongo import MongoClient
        # uri = "mongodb://bifrostProdUser:Maniac321.@cluster0-shard-00-00.bvdlk.mongodb.net:27017,cluster0-shard-00-01.bvdlk.mongodb.net:27017,cluster0-shard-00-02.bvdlk.mongodb.net:27017/myFirstDatabase?ssl=true&replicaSet=atlas-nn38a4-shard-0&authSource=admin&retryWrites=true&w=majority"
        # conection = MongoClient(uri,connect=False)
        # db_ = conection["bifrost"]
        coltn_mdb = db_["puller_history"]
        time_send = datetime.now()
        formatted_date = time_send.strftime("%Y-%m-%d %H:%M:%S")
        coltn_mdb.replace_one(
            {"platform": platform_name},
            {"platform": platform_name, "date": formatted_date},
            upsert=True,
        )
        return ["OK"]

    @task()
    def save_in_history_mysql(engine):

        time_send = datetime.now()
        formatted_date = time_send.strftime("%Y-%m-%d %H:%M:%S")
        connection_engi = engine.connect()
        sql = f"UPDATE his_puller SET date='{formatted_date}'  WHERE platformId ={platform_id_puller}"
        connection_engi.execute(sql)

        return ["OK"]

    @task()
    def extract_platform(config, valid_puller_runing):
        if valid_puller_runing is None:
            return []
        try:
            if config["user"] != "":
                response = requests.get(
                    config["url"],
                    auth=HTTPBasicAuth(config["user"], config["password"]),
                    verify=config["verify"],
                    timeout=config["timeout"],
                )
                
                response = response.text
                response = json.loads(response)
       
            else:
                
                last_index = 0
                has_mode = True
                json_total = []
                while has_mode:
                    url = "http://192.168.36.50:8082/ws/cpeService?wsdl"

                    payload = "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:com=\"com.gilat.ngnms.server.services.ws.cfg.face\">\r\n    <soapenv:Header/>\r\n    <soapenv:Body>\r\n        <com:getCPEsByManagedGroup>\r\n            <managedGroupId>2</managedGroupId>\r\n            <lastIndex>"+str(last_index)+"</lastIndex>\r\n        </com:getCPEsByManagedGroup>\r\n    </soapenv:Body>"
                    headers = {
                    'Content-Type': 'text/xml',
                    'Authorization': 'Basic YXBpczphcGlzMjAyMA=='
                    }

                    response = requests.request("POST", url, headers=headers, data = payload, verify=False)
                    o = xmltodict.parse(response.text)
                    json_data=json.dumps(o)
                    json_data=json.loads(json_data)
                    has_mode = json_data['soap:Envelope']['soap:Body']['ns2:getCPEsByManagedGroupResponse']['return']['hasMore']
                    tt = json_data['soap:Envelope']['soap:Body']['ns2:getCPEsByManagedGroupResponse']['return']['totalNumber']
                    print(tt,'has_modehas_modehas_mode')
                    if(has_mode == 'true'):
                        json_data=json_data['soap:Envelope']['soap:Body']['ns2:getCPEsByManagedGroupResponse']['return']['cpes']['ns3:CPE']
                        json_total += json_data
                        last_index += len(json_data) 
                        print("while", last_index)
                    else:
                        json_data=json_data['soap:Envelope']['soap:Body']['ns2:getCPEsByManagedGroupResponse']['return']['cpes']['ns3:CPE']
                        json_total += json_data
                        last_index += len(json_data) 
                        print("last while", last_index)
                        has_mode = False
                response = json_total
  
                # response = requests.request("POST", config["url"], headers=config["headers"], data = config["payload"], verify=config["verify"],)
                # o = xmltodict.parse(response.text)
                # response=json.dumps(o)
                # response=json.loads(response)
                # response=response['soap:Envelope']['soap:Body']['ns2:getCPEsByManagedGroupResponse']['return']['cpes']['ns3:CPE']

            if config["route_trunk"] == "":
                # response = pd.DataFrame(response).astype(str)
                # print(response,'responseresponseresponseresponse')
                # print(response.columns,'responseresponseresponseresponse')
                response = dumps(response, indent=2)
                response = pd.DataFrame(loads(response))
                response['cpeId-subscriberId'] =  response["cpeId"].map(
                        lambda eve: json.loads(eve['subscriberId'])
                    )
                # print(resp['cpeId-subscriberId'])
                # resp['cpeId'] = resp['cpeId'].apply(lambda x: {} if pd.isna(x) else x)
                # cpe = pd.json_normalize(json.loads(resp.to_json(orient="records")),record_path =['cpeId'],  meta=['description'], record_prefix='cpe.',errors="raise").astype(str)
                # response = pd.merge(resp, cpe, on = 'description',how='outer')
                # print(response, 'rrr')
                # print(response.columns)

                # response[['latitude','longitude']].astype(str)
                # xaa=response[response['deviceID']=='1600032794']
                # print(xaa[['latitude','longitude']],'aaa')
                response = response[response.columns].add_prefix("platform_")
                #TD response = response.astype(str)
                response = generateConcatKey(
                    response, ["platform_" + config["primary_join_cols"]["platform"]]
                )
                response = generateConcatKeySecondary(
                    response, config["secondary_join_cols"]["platform"]
                )

                response = response.to_json(orient="records")
                response = json.loads(response)
                return response
            try:
                for x in config["route_trunk"].split("-"):
                    try:

                        if x.isnumeric():
                            response = response[int(x)]
                        else:
                            response = response[x]
                    except:
                        response = response

                # print(response,'responseresponseresponseresponse')
                data_send = []


                response = pd.DataFrame(response)
                # response['Lat'] = response['Lat'].astype(str)
                # response['Lon'] = response['Lon'].astype(str)
                # response = generateColumns(response,config)
                print(response,' generateColumnsresponsegenerateColumnsresponse')
                # response = response.astype(str)
                response = response[response.columns].add_prefix("platform_")
                # response = response.fillna(0)
                response = generateConcatKey(
                    response, ["platform_" + config["primary_join_cols"]["platform"]]
                )
                response = generateConcatKeySecondary(
                    response, config["secondary_join_cols"]["platform"]
                )
                # xaa=response[response['platform_deviceID']=='1600032794']
                # print(xaa[['platform_latitude','platform_longitude']],'aaa')
                response = response.to_json(orient="records")
                response = json.loads(response)
            except:
                print("ERROR IN route_trunk")
                response = {}

        except requests.exceptions.RequestException as e:
            response = {}
            print("ERROR IN GET DATA PLATFORM")
        return response

    @task()
    def extract_mysql(engine, config, valid_puller_runing):
        if valid_puller_runing is None:
            return []
        query = (
            "SELECT  "+table_mysql_puller+".id,suscriberId ,CAST(latitud AS CHAR(100)) as 'latitud',CAST(longitud AS CHAR(100)) as 'longitud' ,siteId,esn,statusTerminal,did,CAST(id_nms AS CHAR(100)) as 'id_nms' ,modeltype,inroutegroupId,networkId,ms.name as 'crmId',mac  FROM "
            + str(config["mysql_table"])
            + " left join mnos_serviceplan ms on "+table_mysql_puller+".servicesPlanId=ms.id "
            + " where "+table_mysql_puller+".status != 0 and  "+table_mysql_puller+".platformId = "
            + str(config["platform_id"])
        )
        print(query,'  xddddd')
        df_mysql_total = pd.read_sql_query(query, engine)
        if df_mysql_total.empty:
            return "[{}]"
        # df_mysql_total['id_nms'] = df_mysql_total['id_nms'].
        df_mysql_total = df_mysql_total[df_mysql_total.columns].add_prefix("mysql_")
        # df_mysql_total = generateConcatKey(df_mysql_total,[config['primary_join_cols']['mysql']])
        print(df_mysql_total,'2232332')
        df_mysql_total = generateConcatKey(
            df_mysql_total, ["mysql_" + config["primary_join_cols"]["mysql"]]
        )
        print(df_mysql_total,'---')
        df_mysql_total = generateConcatKeySecondary(
            df_mysql_total, config["secondary_join_cols"]["mysql"]
        )
        df_mysql_total = df_mysql_total.to_json(orient="records")
        return df_mysql_total

    @task()
    def extract_servicesplan(engine, config, valid_puller_runing):
        if valid_puller_runing is None:
            return []
        query = (
            "SELECT  id as 'servicePlanIdTable',name as 'platform_slaName' FROM "+table_mysql_serviceplan + " where status = 1 and  platformId = "+ str(config["platform_id"])
        )
        print(query)
        df_mysql = pd.read_sql_query(query, engine)
        if df_mysql.empty:
            return "[{}]"
        print(df_mysql,'  xddddd')
        df_mysql = df_mysql.to_json(orient="records")
        return json.loads(df_mysql)

    @task()
    def comparate_old_vs_new(data_platform, data_old):
        df1 = pd.DataFrame(data_platform)
        data_plat = pd.DataFrame(data_platform)
        if len(data_old) == 0:
            data_platform = df1.to_json(orient="records")
            return {
                "platform_data": data_platform,
                "comparation": [],
                "both": data_platform,
                "only_platform": [],
                "only_old": [],
            }
        else:
            df2 = pd.DataFrame(json.loads(data_old[0]))
        comparation = df1.merge(
            df2, on="concat_key_generate", indicator="_merge_", how="outer"
        )
        both = comparation[comparation["_merge_"] == "both"]
        plat = comparation[comparation["_merge_"] == "left_only"]
        old = comparation[comparation["_merge_"] == "right_only"]

        if both.empty:
            both_send = "empty"
        else:
            both_send = both.to_json(orient="records")

        if plat.empty:
            plat_send = "empty"
        else:
            plat_send = plat.to_json(orient="records")

        if old.empty:
            old_send = []
        else:
            old_send = old.to_json(orient="records")
        data_platform = data_plat.to_json(orient="records")
        return {
            "platform_data": data_platform,
            "comparation": comparation.to_json(orient="records"),
            "both": both_send,
            "only_platform": plat_send,
            "only_old": old_send,
        }

    @task()
    def comparate_primary_mysql_equals(df_mysql, comparate):
        df_mysql = pd.DataFrame(json.loads(df_mysql))
        if df_mysql.empty:
            df_mysql = pd.DataFrame(columns=["concat_key_generate"])

        platform_data = pd.DataFrame(json.loads(comparate["platform_data"]))
        comparate = pd.DataFrame(json.loads(comparate["both"]))
        both = comparate
        both["exist_mysql"] = np.where(
            both["concat_key_generate"].isin(list(df_mysql["concat_key_generate"])),
            1,
            0,
        )
        exist_mysql_p = both[both["exist_mysql"] == 1]
        not_exist_mysql_p = both[both["exist_mysql"] == 0]
        exist_mysql_p = platform_data[
            platform_data["concat_key_generate"].isin(
                list(exist_mysql_p["concat_key_generate"])
            )
        ]

        if exist_mysql_p.empty:
            exist_mysql_p = []
        else:
            exist_mysql_p = json.loads(exist_mysql_p.to_json(orient="records"))

        if not_exist_mysql_p.empty:
            not_exist_mysql_p = []
        else:
            not_exist_mysql_p = json.loads(not_exist_mysql_p.to_json(orient="records"))

        return {"exist_mysql": exist_mysql_p, "not_exist_mysql": not_exist_mysql_p}

    @task()
    def comparate_primary_mysql_only_platform(df_mysql, comparate):
        df_mysql = pd.DataFrame(json.loads(df_mysql))
        if df_mysql.empty:
            df_mysql = pd.DataFrame(columns=["concat_key_generate"])

        if comparate["only_platform"] == "empty":
            return {"exist_mysql": [], "not_exist_mysql": []}

        platform_data = pd.DataFrame(json.loads(comparate["platform_data"]))

        try:
            comparate = pd.DataFrame(json.loads(comparate["only_platform"]))
        except:
            # try:
            # comparate = pd.DataFrame(comparate['only_platform'])
            # except:
            comparate = pd.DataFrame(columns=["concat_key_generate"])
        only_platform = comparate
        only_platform["exist_mysql"] = np.where(
            only_platform["concat_key_generate"].isin(
                list(df_mysql["concat_key_generate"])
            ),
            1,
            0,
        )
        exist_mysql_p = only_platform[only_platform["exist_mysql"] == 1]
        not_exist_mysql_p = only_platform[only_platform["exist_mysql"] == 0]
        exist_mysql_p = platform_data[
            platform_data["concat_key_generate"].isin(
                list(exist_mysql_p["concat_key_generate"])
            )
        ]

        if exist_mysql_p.empty:
            exist_mysql_p = []
        else:
            exist_mysql_p = json.loads(exist_mysql_p.to_json(orient="records"))

        if not_exist_mysql_p.empty:
            not_exist_mysql_p = []
        else:
            not_exist_mysql_p = json.loads(not_exist_mysql_p.to_json(orient="records"))

        return {"exist_mysql": exist_mysql_p, "not_exist_mysql": not_exist_mysql_p}

    @task()
    def comparate_primary_mysql_only_data_old(df_mysql, comparate):
        df_mysql = pd.DataFrame(json.loads(df_mysql))
        if df_mysql.empty:
            df_mysql = pd.DataFrame(columns=["concat_key_generate"])

        platform_data = pd.DataFrame(json.loads(comparate["platform_data"]))

        if comparate["only_old"] == []:
            return {"update_mysql": [], "insert_mysql": [], "delete_mysql": []}

        comparate = pd.DataFrame(json.loads(comparate["only_old"]))
        only_old = comparate
        only_old["exist_mysql"] = np.where(
            only_old["concat_key_generate"].isin(list(df_mysql["concat_key_generate"])),
            1,
            0,
        )
        exist_mysql_p = only_old[only_old["exist_mysql"] == 1]
        not_exist_mysql_p = only_old[only_old["exist_mysql"] == 0]
        exist_mysql_p = platform_data[
            platform_data["concat_key_generate"].isin(
                list(exist_mysql_p["concat_key_generate"])
            )
        ]

        if exist_mysql_p.empty:
            exist_mysql_p = []
        else:
            exist_mysql_p = json.loads(exist_mysql_p.to_json(orient="records"))

        if not_exist_mysql_p.empty:
            not_exist_mysql_p = []
        else:
            not_exist_mysql_p = json.loads(not_exist_mysql_p.to_json(orient="records"))
        return {"update_mysql": exist_mysql_p, "insert_mysql": [], "delete_mysql": []}

    @task()
    def comparate_primary_mongo_equals(df_mongo, comparate):
        df_mongo = pd.DataFrame(df_mongo)
        platform_data = pd.DataFrame(json.loads(comparate["platform_data"]))
        both = pd.DataFrame(json.loads(comparate["both"]))
        try:
            comparate = pd.DataFrame(json.loads(comparate["both"]))
        except:
            comparate = pd.DataFrame(columns=["concat_key_generate"])
        both = comparate

        if df_mongo.empty:
            df_mongo = pd.DataFrame(columns=["concat_key_generate"])

        both["exist_mongo"] = np.where(
            both["concat_key_generate"].isin(list(df_mongo["concat_key_generate"])),
            1,
            0,
        )
        exist_mongo_p = both[both["exist_mongo"] == 1]
        not_exist_mongo_p = both[both["exist_mongo"] == 0]
        exist_mongo_p = platform_data[
            platform_data["concat_key_generate"].isin(
                list(exist_mongo_p["concat_key_generate"])
            )
        ]
        not_exist_mongo_p = platform_data[
            platform_data["concat_key_generate"].isin(
                list(not_exist_mongo_p["concat_key_generate"])
            )
        ]

        if exist_mongo_p.empty:
            exist_mongo_p = []
        else:
            exist_mongo_p = json.loads(exist_mongo_p.to_json(orient="records"))

        if not_exist_mongo_p.empty:
            not_exist_mongo_p = []
        else:
            not_exist_mongo_p = json.loads(not_exist_mongo_p.to_json(orient="records"))
            # not_exist_mongo_p=not_exist_mongo_p.to_json(orient="records")

        # return exist_mongo_p.to_json(orient="records")
        return {"exist_mongo": exist_mongo_p, "not_exist_mongo": not_exist_mongo_p}

    @task()
    def comparate_primary_mongo_only_platform(df_mongo, comparate):
        df_mongo = pd.DataFrame(df_mongo)
        platform_data = pd.DataFrame(json.loads(comparate["platform_data"]))

        if comparate["only_platform"] == "empty":
            return {"exist_mongo": [], "not_exist_mongo": []}

        # only_platform = pd.DataFrame(json.loads(comparate['only_platform']))
        try:
            comparate = pd.DataFrame(json.loads(comparate["only_platform"]))
        except:
            comparate = pd.DataFrame(columns=["concat_key_generate"])
        only_platform = comparate

        if df_mongo.empty:
            df_mongo = pd.DataFrame(columns=["concat_key_generate"])
        only_platform["exist_mongo"] = np.where(
            only_platform["concat_key_generate"].isin(
                list(df_mongo["concat_key_generate"])
            ),
            1,
            0,
        )
        exist_mongo_p = only_platform[only_platform["exist_mongo"] == 1]
        not_exist_mongo_p = only_platform[only_platform["exist_mongo"] == 0]
        exist_mongo_p = platform_data[
            platform_data["concat_key_generate"].isin(
                list(exist_mongo_p["concat_key_generate"])
            )
        ]
        not_exist_mongo_p = platform_data[
            platform_data["concat_key_generate"].isin(
                list(not_exist_mongo_p["concat_key_generate"])
            )
        ]

        if exist_mongo_p.empty:
            exist_mongo_p = []
        else:
            exist_mongo_p = json.loads(exist_mongo_p.to_json(orient="records"))

        if not_exist_mongo_p.empty:
            not_exist_mongo_p = []
        else:
            not_exist_mongo_p = json.loads(not_exist_mongo_p.to_json(orient="records"))
            # not_exist_mongo_p=not_exist_mongo_p.to_json(orient="records")

        # return exist_mongo_p.to_json(orient="records")
        return {"exist_mongo": exist_mongo_p, "not_exist_mongo": not_exist_mongo_p}

    @task()
    def comparate_primary_mongo_only_old(df_mongo, comparate):
        df_mongo = pd.DataFrame(df_mongo)
        platform_data = pd.DataFrame(json.loads(comparate["platform_data"]))

        if comparate["only_old"] == []:
            return {"update_mongo": [], "insert_mongo": [], "delete_mongo": []}

        only_old = pd.DataFrame(json.loads(comparate["only_old"]))

        try:
            comparate = pd.DataFrame(json.loads(comparate["only_old"]))
        except:
            comparate = pd.DataFrame(columns=["concat_key_generate"])
        only_platform = comparate

        if df_mongo.empty:
            df_mongo = pd.DataFrame(columns=["concat_key_generate"])

        only_old["exist_mongo"] = np.where(
            only_old["concat_key_generate"].isin(list(df_mongo["concat_key_generate"])),
            1,
            0,
        )
        exist_mongo_p = only_old[only_old["exist_mongo"] == 1]
        not_exist_mongo_p = only_old[only_old["exist_mongo"] == 0]
        exist_mongo_p = platform_data[
            platform_data["concat_key_generate"].isin(
                list(exist_mongo_p["concat_key_generate"])
            )
        ]
        not_exist_mongo_p = platform_data[
            platform_data["concat_key_generate"].isin(
                list(not_exist_mongo_p["concat_key_generate"])
            )
        ]

        if exist_mongo_p.empty:
            exist_mongo_p = []
        else:
            exist_mongo_p = json.loads(exist_mongo_p.to_json(orient="records"))

        if not_exist_mongo_p.empty:
            not_exist_mongo_p = []
        else:
            not_exist_mongo_p = json.loads(not_exist_mongo_p.to_json(orient="records"))
        return {"delete_mongo": exist_mongo_p, "update_mongo": [], "insert_mongo": []}

    @task()
    def comparate_secondary_mysql_only_platform(df_mysql, comparate, old):
        glob_comparate = comparate
        try:
            comparate = pd.DataFrame(comparate["exist_mysql"])
        except:
            comparate = pd.DataFrame(columns=["concat_key_generate_secondary"])
        df_mysql = pd.DataFrame(json.loads(df_mysql))

        if df_mysql.empty:
            df_mysql = pd.DataFrame(columns=["concat_key_generate_secondary"])

        both = comparate
        try:
            both["exist_mysql_secondary"] = np.where(
                both["concat_key_generate_secondary"].isin(
                    list(df_mysql["concat_key_generate_secondary"])
                ),
                1,
                0,
            )
        except:
            return {
                "update_mysql": [],
                "insert_mysql": glob_comparate["not_exist_mysql"],
                "delete_mysql": old["only_old"],
            }

        exist_mysql_s = both[both["exist_mysql_secondary"] == 1]
        not_exist_mysql_s = both[both["exist_mysql_secondary"] == 0]
        not_exist_mysql_s_com = both[both["exist_mysql_secondary"] == 0]
        if exist_mysql_s.empty:
            exist_mysql_s = []
        else:
            exist_mysql_s = json.loads(exist_mysql_s.to_json(orient="records"))

        if not_exist_mysql_s.empty:
            not_exist_mysql_s = []
            data_mysql_not_exist_s = []
        else:
            data_mysql_not_exist_s = df_mysql[
                df_mysql["concat_key_generate"].isin(
                    list(not_exist_mysql_s_com["concat_key_generate"])
                )
            ]
            data_mysql_not_exist_s = pd.merge(
                not_exist_mysql_s_com, data_mysql_not_exist_s, on="concat_key_generate"
            )
            data_mysql_not_exist_s = json.loads(
                data_mysql_not_exist_s.to_json(orient="records")
            )
        # print(data_mysql_not_exist_s.columns,'hereeeeeee')
        # print(data_mysql_not_exist_s['concat_key_generate_secondary','platform_deviceID'],'hereeeeeee')
        return {
            "update_mysql": data_mysql_not_exist_s,
            "insert_mysql": glob_comparate["not_exist_mysql"],
            "delete_mysql": old["only_old"],
        }

    @task()
    def comparate_secondary_mysql_equals(df_mysql, comparate, old):
        glob_comparate = comparate
        try:
            comparate = pd.DataFrame(comparate["exist_mysql"])
        except:
            comparate = pd.DataFrame(columns=["concat_key_generate_secondary"])
        df_mysql = pd.DataFrame(json.loads(df_mysql))

        if df_mysql.empty:
            df_mysql = pd.DataFrame(columns=["concat_key_generate_secondary"])

        both = comparate

        try:
            both["exist_mysql_secondary"] = np.where(
                both["concat_key_generate_secondary"].isin(
                    list(df_mysql["concat_key_generate_secondary"])
                ),
                1,
                0,
            )
        except:
            return {
                    "update_mysql": [],
                    "insert_mysql": glob_comparate["not_exist_mysql"],
                    "delete_mysql": old["only_old"],
                }
        exist_mysql_s = both[both["exist_mysql_secondary"] == 1]
        not_exist_mysql_s = both[both["exist_mysql_secondary"] == 0]
        not_exist_mysql_s_com = both[both["exist_mysql_secondary"] == 0]
        if exist_mysql_s.empty:
            exist_mysql_s = []
        else:
            exist_mysql_s = json.loads(exist_mysql_s.to_json(orient="records"))
        # print(exist_mysql_s,'exist_mysql_sexist_mysql_sexist_mysql_s')
        # print(not_exist_mysql_s,'not_exist_mysql_snot_exist_mysql_snot_exist_mysql_snot_exist_mysql_snot_exist_mysql_s')
        if not_exist_mysql_s.empty:
            not_exist_mysql_s = []
            data_mysql_not_exist_s = []
        else:
            data_mysql_not_exist_s = df_mysql[
                df_mysql["concat_key_generate"].isin(
                    list(not_exist_mysql_s_com["concat_key_generate"])
                )
            ]
            data_mysql_not_exist_s = pd.merge(
                not_exist_mysql_s_com, data_mysql_not_exist_s, on="concat_key_generate"
            )
            # print(data_mysql_not_exist_s[['concat_key_generate_secondary_x','concat_key_generate_secondary_y','platform_deviceID']],'hereeeeeee')
            data_mysql_not_exist_s = json.loads(
                data_mysql_not_exist_s.to_json(orient="records")
            )

        # print(data_mysql_not_exist_s.columns,'hereeeeeee')
        # print(data_mysql_not_exist_s['concat_key_generate_secondary','platform_deviceID'],'hereeeeeee')
        print(len(data_mysql_not_exist_s), "  -total")
        return {
            "update_mysql": data_mysql_not_exist_s,
            "insert_mysql": glob_comparate["not_exist_mysql"],
            "delete_mysql": old["only_old"],
        }
        # return ['ok']

    @task()
    def comparate_secondary_mongo_equals(df_mongo, comparate, old):
        glob_comparate = comparate
        df_mongo = pd.DataFrame(df_mongo)

        if df_mongo.empty:
            df_mongo = pd.DataFrame(columns=["concat_key_generate_secondary"])

        try:
            comparate = pd.DataFrame(comparate["exist_mongo"])
        except:
            comparate = pd.DataFrame(columns=["concat_key_generate_secondary"])
        # print(comparate,'comparateeeeee')
        # print(comparate.columns,'comparateeeeee')

        try:
            comparate_not_exist = pd.DataFrame(glob_comparate["not_exist_mongo"])
            comparate_not_exist.columns = comparate_not_exist.columns.str.replace(
                "platform_", ""
            )
            # del comparate_not_exist['concat_key_generate']

            comparate_not_exist = json.loads(
                comparate_not_exist.to_json(orient="records")
            )
            # del comparate_not_exist['concat_key_generate_secondary']
        except:
            comparate_not_exist = []

        both = comparate
        try:
            both["exist_mongo_secondary"] = np.where(
                both["concat_key_generate_secondary"].isin(
                    list(df_mongo["concat_key_generate_secondary"])
                ),
                1,
                0,
            )
        except:
            return {
                "update_mongo": [],
                "insert_mongo": comparate_not_exist,
                "delete_mongo": old["only_old"],
            }

        exist_mongo_s = both[both["exist_mongo_secondary"] == 1]
        not_exist_mongo_s = both[both["exist_mongo_secondary"] == 0]
        not_exist_mongo_s_com = both[both["exist_mongo_secondary"] == 0]
        data_mongo_not_exist_s=[]
        if exist_mongo_s.empty:
            exist_mongo_s = []
        else:
            exist_mongo_s = json.loads(exist_mongo_s.to_json(orient="records"))

        if not_exist_mongo_s.empty:
            not_exist_mongo_s = []
        else:
            not_exist_mongo_s.columns = not_exist_mongo_s.columns.str.replace(
                "platform_", ""
            )
            del not_exist_mongo_s["concat_key_generate"]
            del not_exist_mongo_s["concat_key_generate_secondary"]
            not_exist_mongo_s = json.loads(not_exist_mongo_s.to_json(orient="records"))

            data_mongo_not_exist_s = df_mongo[
                df_mongo["concat_key_generate"].isin(
                    list(not_exist_mongo_s_com["concat_key_generate"])
                )
            ]
            del data_mongo_not_exist_s["concat_key_generate_secondary"]
            data_mongo_not_exist_s = pd.merge(
                not_exist_mongo_s_com, data_mongo_not_exist_s, on="concat_key_generate"
            )
            data_mongo_not_exist_s.columns = data_mongo_not_exist_s.columns.str.replace(
                "platform_", ""
            )
            del data_mongo_not_exist_s["concat_key_generate"]

            data_mongo_not_exist_s = json.loads(
                data_mongo_not_exist_s.to_json(orient="records")
            )

        # try:
        # comparate_not_exist = json.loads(comparate_not_exist.to_json(orient="records"))
        # except:
        # comparate_not_exist = []
        return {
            "update_mongo": data_mongo_not_exist_s,
            "insert_mongo": comparate_not_exist,
            "delete_mongo": old["only_old"],
        }

    @task()
    def comparate_secondary_mongo_only_platform(df_mongo, comparate, old):
        glob_comparate = comparate
        df_mongo = pd.DataFrame(df_mongo)

        if df_mongo.empty:
            df_mongo = pd.DataFrame(columns=["concat_key_generate_secondary"])

        try:
            comparate = pd.DataFrame(comparate["exist_mongo"])
        except:
            comparate = pd.DataFrame(columns=["concat_key_generate_secondary"])
        both = comparate
        try:
            both["exist_mongo_secondary"] = np.where(
                both["concat_key_generate_secondary"].isin(
                    list(df_mongo["concat_key_generate_secondary"])
                ),
                1,
                0,
            )
        except:
            return {
                "update_mongo": [],
                "insert_mongo": glob_comparate["not_exist_mongo"],
                "delete_mongo": old["only_old"],
            }

        exist_mongo_s = both[both["exist_mongo_secondary"] == 1]
        not_exist_mongo_s = both[both["exist_mongo_secondary"] == 0]

        if exist_mongo_s.empty:
            exist_mongo_s = []
        else:
            exist_mongo_s = json.loads(exist_mongo_s.to_json(orient="records"))

        if not_exist_mongo_s.empty:
            not_exist_mongo_s = []
        else:
            not_exist_mongo_s = json.loads(not_exist_mongo_s.to_json(orient="records"))

        return {
            "update_mongo": not_exist_mongo_s,
            "insert_mongo": glob_comparate["not_exist_mongo"],
            "delete_mongo": old["only_old"],
        }

    @task()
    def save_key_in_history_puller_cron(key, type_puller):
        query_update = f"INSERT INTO puller_cron_platform (key_redis,status_cron,platform_id,type) values('{key}','{platform_id_puller}',1,'{type_puller}')"
        connection_engi = engine.connect()
        resp = connection_engi.execute(query_update)
        if resp.rowcount > 0:
            resp = "INSERT OK"
        else:
            resp = "ERROR"
        return resp

    @task()
    def processDataInsertMysqlGilat(keys,data_servicesplan):
        key = keys["key_insert"]
        try:
            data = getDataRedisByKey(key)
        except:
            # if len(data)==0:
            return []
        if len(data) == 0:
            return []
        data_insert_send = pd.DataFrame(data)
        # data_insert_send = pd.DataFrame(keys['insert_mysql'])

        data_insert_send = data_insert_send[
            [
                "platform_description",
                "platform_operationalState",
                "platform_cpeId-subscriberId",
                "platform_slaName",
                "platform_macAddress"
            ]
        ]
        data_insert_send["platformId"] = platform_id_puller
        data_insert_send["status"] = 1
        data_insert_send["fromPuller"] = 1
        data_insert_send["vnoId"] = 158
        data_insert_send["technologyId"] = 2
        data_insert_send["forecastId"] = 77
        data_insert_send["beamId"] = 88
        
        data_insert_send.rename(columns={"platform_description": "siteId"}, inplace=True)
        data_insert_send.rename(columns={"platform_operationalState": "statusTerminal"}, inplace=True)
        data_insert_send.rename(columns={"platform_cpeId-subscriberId": "suscriberId"}, inplace=True)
        data_insert_send.rename(columns={"platform_macAddress": "mac"}, inplace=True)
        # data_insert_send.rename(columns={"platform_Lat": "latitud"}, inplace=True)
        # data_insert_send.rename(columns={"platform_Lon": "longitud"}, inplace=True)
        list_sp = pd.DataFrame(data_servicesplan)
        data_insert_send = data_insert_send.join(list_sp.set_index('platform_slaName'), on='platform_slaName')
        data_insert_send['servicePlanIdTable'] = data_insert_send["servicePlanIdTable"].fillna(1171)
        data_insert_send.loc[data_insert_send.servicePlanIdTable == 1171, ['servicePlanIdTable', 'status']] = '', 3
        data_insert_send.rename(columns={"servicePlanIdTable": "servicesPlanId"}, inplace=True)
        del data_insert_send['platform_slaName']
        print(data_insert_send,'data_insert_senddata_insert_send')
        data_insert_send.to_sql(
            table_mysql_puller, engine, if_exists="append", index=False
        )
        
        # dateSaveHistoryInsert(data)
        return "ok"

    @task()
    def processDataUpdateMysqlGilat(engine, keys,data_servicesplan):
        key = keys["key_update"]
        try:
            data = getDataRedisByKey(key)
        except:
            # if len(data)==0:
            return []

        if len(data) == 0:
            return []
        connection_engi = engine.connect()
        # data = pd.DataFrame(keys['update_mysql'])
        data = pd.DataFrame(data)
        data["updated_at_send"] = time_send_now
        # data["platform_Active"] = data["platform_Active"].astype(str)
        data["platform_status"] = 1
        

        
        # args_send = (
        #     data[
        #         [
        #             "platform_Active",
        #             "platform_SN",
        #             "platform_Name",
        #             "platform_DID",
        #             "updated_at_send",
        #             "platform_ID",
        #             "platform_ModelType",
        #             "platform_InrouteGroupID",
        #             "platform_NetworkID",
        #             "platform_Lat",
        #             "platform_Lon",
                    
                    
        #             "mysql_statusTerminal",
        #             "mysql_esn",
        #             "mysql_did",
                    
        #             "mysql_modeltype",
        #             "mysql_inroutegroupId",
        #             "mysql_networkId",
        #             "mysql_latitud",
        #             "mysql_longitud",

        #             "mysql_id_nms",
                    
                    
        #         ]
        #     ]
        #     .iloc[0:]
        #     .to_dict("record")
        # )
        # print(data.columns,'colls')
        
        # datax = data[
        #         [
        #             "platform_Active",
        #             "platform_SN",
        #             "platform_DID",
        #             "updated_at_send",
                    
        #             "platform_ModelType",
        #             "platform_InrouteGroupID",
        #             "platform_NetworkID",
        #             "platform_Lat",
        #             "platform_Lon",
                    
                    
        #             "platform_Name",
        #             "platform_ID",
        #             "platform_SERVICEPLANCRMID",
        #             "concat_key_generate_secondary_x",
        #             "concat_key_generate_secondary_y"
        #         ]
        #     ]
        print(data,'xx')
        print(data.columns,'xdd')
        data = data[
                [
                    "platform_description",
                    "platform_operationalState",
                    "platform_cpeId-subscriberId",
                    "platform_slaName",
                    "platform_status",
                    "updated_at_send",
                    "platform_macAddress"
                ]
            ]
        
        data.rename(columns={"platform_cpeId-subscriberId": "platform_subscriberId"}, inplace=True)
        
        
        # datax.rename(columns={"platform_SERVICEPLANCRMID": "crmId"}, inplace=True)
        list_sp = pd.DataFrame(data_servicesplan)
        
        # data.rename(columns={"platform_SERVICEPLANCRMID": "platform_slaName"}, inplace=True)
        
        data = data.join(list_sp.set_index('platform_slaName'), on='platform_slaName')
        data['servicePlanIdTable'] = data["servicePlanIdTable"].fillna(1171)
            #   data_insert_send['servicePlanIdTable'] = data_insert_send["servicePlanIdTable"].fillna(1171)
        data.loc[data.servicePlanIdTable == 1171, ['servicePlanIdTable', 'platform_status']] = '', 3
        print(data,'data update')
        args = (data.iloc[0:].to_dict("record"))
        # print(datax[['servicePlanIdTable','platform_Lon','concat_key_generate_secondary_x','concat_key_generate_secondary_y']], 'argsargsargsargsargsargs')
        # args_mysql = data[['mysql_statusTerminal','mysql_esn','mysql_latitud','mysql_longitud',]].iloc[0:].to_dict('record')
        elements = []
        qry=f"             UPDATE {table_mysql_puller}    SET statusTerminal=:platform_operationalState,updated_at=:updated_at_send, fromPuller=1, servicesPlanId=:servicePlanIdTable , suscriberId=:platform_subscriberId,status=:platform_status,mac=:platform_macAddress WHERE siteId=:platform_description and platformId={platform_id_puller}"
        query_update = text(qry)
        connection_engi.execute(query_update, args)
        # dateSaveHistoryUpdate(args_send)
        return ["ok"]

    @task()
    def processDataInsertMongoGilat(keys):

        key = keys["key_insert"]
        try:
            data = getDataRedisByKey(key)
        except:
            return []
        if len(data) == 0:
            return []
        # df = pd.DataFrame(keys['insert_mongo'])
        df = pd.DataFrame(data)
        df.columns = df.columns.str.replace("platform_", "")
        
        try:
            df = df[df.columns.difference(list(df.filter(regex='mongo_').columns))]
            print(df,'dfdfdfdf')
        except:
            print("error")
            
            
            
        data = df.to_json(orient="records")
        data = json.loads(data)

        if len(data) == 0:
            return []
        time_send = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        formatted_date = str(time_send)
        elements = []
        for x in data:
            data_mysql = getDataMysqlBySiteId(x["description"])
            element = {
                "puller": x,
                "status": str(x["operationalState"]),
                "timeC": formatted_date,
                "timeCO": "",
                "btId": data_mysql["btId"],
                "mysqlFlag": data_mysql["mysqlFlag"],
                # "comisioningFlag": 1 if x["terminalStatus"] == "normal" else 0,
                "platform": platform_id_puller,
                "active": 1,
                "siteId": x["description"],
            }
            # elements.append(element)
            coltn_mdb.insert_one(element)
        # coltn_mdb.insert_many(elements)
        # dateSaveHistoryInsertMongo(elements)

        return [keys]

    @task()
    def processDataUpdateMongoGilat(keys):
        key = keys["key_update"]
        try:
            data = getDataRedisByKey(key)
        except:
            return []
        if len(data) == 0:
            return []
        # data = keys['update_mongo']

        
        df = pd.DataFrame(data)
        df.columns = df.columns.str.replace("platform_", "")
        
        df2 = pd.DataFrame(data)
        df2.columns = df2.columns.str.replace("platform_", "")
        try:
            df = df[df.columns.difference(list(df.filter(regex='mongo_').columns))]
            print(df,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')
        except:
            print("error")
            
        # bulk = coltn_mdb.initialize_unordered_bulk_op
        
        data = df.to_json(orient="records")
        data = json.loads(data)
        datax = df2.to_json(orient="records")
        datax = json.loads(datax)
        for x in data:
            print(x,'DATAAAAAAAA')
            # x["Active"] = str(x["Active"])
            coltn_mdb.update_one({"siteId": x["description"],"platform":platform_id_puller},{"$set": {"puller": x, "status": str(x["operationalState"]), "active": 1}})
            # bulk.find({"siteId": x["Name"],"platform":platform_id_puller}).update(
            #     {"$set": {"puller.DID": x["DID"],"puller.SN": x["SN"],"puller.Active": str(x["Active"]), "status": str(x["Active"]), "active": 1}}
            # )

        # bulk.execute()
        # dateSaveHistoryUpdateMongo(datax)
        return [keys]

    @task()
    def processDataDeleteMongoGilat(keys):
        key = keys["key_delete"]
        try:
            data = getDataRedisByKey(key)
        except:
            return []
        if len(data) == 0:
            return []
        bulk = coltn_mdb.initialize_unordered_bulk_op()
        for x in json.loads(data):
            # bulk.find({"active": 1, "siteId": x["old_Name"]}).update(
            #     {"$set": {"active": 0}}
            # )
            
            bulk.find({"siteId": x["old_description"],"platform":platform_id_puller}).update(
                {"$set": {"active": 0}}
            )
            dateSaveHistory(
                {
                    "type": "delete_mongo",
                    "principal_key": x["old_description"],
                    "changes": {"status": 0},
                }
            )

            # bulk.find({"active":1,"siteId": x['old_deviceID']}).update({'$set':{"active":0}})
        # bulk.execute()
        return [keys]

    @task()
    def processDataDeleteMysqlGilat(engine, keys):
        key = keys["key_delete"]
        try:
            data = getDataRedisByKey(key)
        except:
            # if len(data)==0:
            return []
        # if len(data)==0:
        #     return []
        if len(data) == 0:
            return []

        connection_engi = engine.connect()
        # data = keys['delete_mysql']
        # return 'ok'
        # time_send = time_send_now
        # formatted_date = str(time_send)
        for x in json.loads(data):
            sqlesn = (
                "UPDATE "+table_mysql_puller+" SET status =0 WHERE "
                # + str(x["old_Name"]) + "and
                +"siteId="+ str(x["old_description"])
                + " and platformId="+str(platform_id_puller)+" and status!=0"
            )
            print(sqlesn,'sqlesnsqlesnsqlesnsqlesn')
            connection_engi.execute(sqlesn)
            # dateSaveHistory({"type":"delete_mysql","principal_key":x['old_deviceID'],"changes":{'status':0}})
        return ["ok"]

    @task()
    def finish_process():
        return ["ok"]

    @task()
    def finish():
        return ["ok"]

    @task()
    def start():
        return ["ok"]

    # [START main_flow]
    valid_puller_runing = valid_exist_puller_runing()

    checkTask = BranchPythonOperator(
        task_id="valid_puller_runing",
        trigger_rule=TriggerRule.ONE_SUCCESS,
        python_callable=valid_exist_puller_runing,  # Registered method
        provide_context=True,
        # dag=dag
    )
    rs = start()
    key_process = str(config["platform_id"]) + "-" + str(config["platform_name"])
    old_data = extract_old(key_process, config, valid_puller_runing)
    platform_data = extract_platform(config, valid_puller_runing)
    save_in_redis_data_platform_data = save_in_redis_data_platform(platform_data)
    comp = comparate_old_vs_new(platform_data, old_data)
    mysql_data = extract_mysql(engine, config, valid_puller_runing)
    mongo_data = extract_mongo(config, valid_puller_runing)
    extract_servicesplan_data = extract_servicesplan(engine,config, valid_puller_runing)
    # #COMPARATE MYSQL
    time_send = datetime.now()
    formatted_date = time_send.strftime('%Y-%m-%d-%H-%M')
    key_redis_mysql = key_process+'-mysql-'+formatted_date
    key_redis_mongo = key_process+'-mongo-'+formatted_date

    primary_vs_mysql_equals = comparate_primary_mysql_equals(mysql_data,comp)
    secondary_vs_mysql_equals = comparate_secondary_mysql_equals(mysql_data,primary_vs_mysql_equals,comp)

    save_in_redis_result_equals = save_in_redis_data_equals_api(config,secondary_vs_mysql_equals,key_redis_mysql)
    insert_data_mysql_equals = processDataInsertMysqlGilat(save_in_redis_result_equals,extract_servicesplan_data)
    update_data_mysql_equals = processDataUpdateMysqlGilat(engine,save_in_redis_result_equals,extract_servicesplan_data)
    delete_data_mysql_equals = processDataDeleteMysqlGilat(engine,save_in_redis_result_equals)
    # return 'ok'


    primary_vs_mysql_only_platform= comparate_primary_mysql_only_platform(mysql_data,comp)
    secondary_vs_mysql_only_platform = comparate_secondary_mysql_only_platform(mysql_data,primary_vs_mysql_only_platform,comp)
    save_in_redis_result_only_platform = save_in_redis_data_only_platform_api(config,secondary_vs_mysql_only_platform,key_redis_mysql)
    insert_data_mysql_only_platform = processDataInsertMysqlGilat(save_in_redis_result_only_platform,extract_servicesplan_data)
    update_data_mysql_only_platform = processDataUpdateMysqlGilat(engine,save_in_redis_result_only_platform,extract_servicesplan_data)
    delete_data_mysql_only_platform = processDataDeleteMysqlGilat(engine,save_in_redis_result_only_platform)

    primary_vs_mysql_only_old= comparate_primary_mysql_only_data_old(mysql_data,comp)
    save_in_redis_result_only_old = save_in_redis_data_only_old_api(config,primary_vs_mysql_only_old,key_redis_mysql)
    delete_data_mysql_only_old = processDataDeleteMysqlGilat(engine,save_in_redis_result_only_old)

    # ##COMPARATE MONGODB
    print("12")

    primary_vs_mongo_equals = comparate_primary_mongo_equals(mongo_data,comp)
    secondary_vs_mongo_equals = comparate_secondary_mongo_equals(mongo_data,primary_vs_mongo_equals,comp)
    save_in_redis_result_mongo_equals = save_in_redis_data_equals_mongo_api(config,secondary_vs_mongo_equals,key_redis_mongo+'-equals')
    insert_data_mongo_equals = processDataInsertMongoGilat(save_in_redis_result_mongo_equals)
    update_data_mongo_equals = processDataUpdateMongoGilat(save_in_redis_result_mongo_equals)
    delete_data_mongo_equals = processDataDeleteMongoGilat(save_in_redis_result_mongo_equals)
    # delete_data_mongo_equals = processDataDeleteMongoGilat(save_in_redis_result_mongo_equals)

    primary_vs_mongo_only_platform = comparate_primary_mongo_only_platform(mongo_data,comp)
    secondary_vs_mongo_only_platform = comparate_secondary_mongo_only_platform(mongo_data,primary_vs_mongo_only_platform,comp)
    save_in_redis_result_mongo_only_platform = save_in_redis_data_only_platform_mongo_api(config,secondary_vs_mongo_only_platform,key_redis_mongo)
    insert_data_mongo_onlyplatform = processDataInsertMongoGilat(save_in_redis_result_mongo_only_platform)
    update_data_mongo_onlyplatform = processDataUpdateMongoGilat(save_in_redis_result_mongo_only_platform)
    delete_data_mongo_onlyplatform = processDataDeleteMongoGilat(save_in_redis_result_mongo_only_platform)

    primary_vs_mongo_only_data_old = comparate_primary_mongo_only_old(mongo_data,comp)
    save_in_redis_result_mongo_only_old = save_in_redis_data_only_old_mongo_api(config,primary_vs_mongo_only_data_old,key_redis_mongo)
    delete_data_mongo_onlyold = processDataDeleteMongoGilat(save_in_redis_result_mongo_only_old)
    save_in_redis_end = save_in_redis_data_old(config,platform_data,key_process)
    save_in_history_mongo_puller = save_in_history_mongo(config)
    save_in_history_mysql_puller = save_in_history_mysql(engine)

    end = finish()
    end_process = finish_process()

    checkTask >> end_process
    checkTask >> rs
    rs >>Label("Extrae la data de plataforma") >> platform_data >> save_in_redis_data_platform_data
    rs >>Label("Extrae la data de mysql") >> mysql_data
    rs >>Label("Extrae la data de mongodb") >> mongo_data
    rs >>Label("Extrae la data de la imagen anterior") >> old_data
    rs >>Label("Extrae la lista de servicesPlan para obtener su Id") >> extract_servicesplan_data
    # rs >> [platform_data >> save_in_redis_data_platform_data,old_data,extract_servicesplan_data] >> 
    comp,mysql_data >> [primary_vs_mysql_equals >> secondary_vs_mysql_equals >>  save_in_redis_result_equals >> insert_data_mysql_equals,update_data_mysql_equals,delete_data_mysql_equals,primary_vs_mysql_only_platform >> secondary_vs_mysql_only_platform >> save_in_redis_result_only_platform >> insert_data_mysql_only_platform,update_data_mysql_only_platform,delete_data_mysql_only_platform ,  primary_vs_mysql_only_old >> save_in_redis_result_only_old >> delete_data_mysql_only_old ] >> save_in_redis_end >> save_in_history_mysql_puller >> end
    # rs >> [platform_data >> save_in_redis_data_platform_data,old_data,extract_servicesplan_data] >> 
    comp,mongo_data >> [primary_vs_mongo_equals >> secondary_vs_mongo_equals >> save_in_redis_result_mongo_equals >> insert_data_mongo_equals,update_data_mongo_equals,delete_data_mongo_equals , primary_vs_mongo_only_platform >> secondary_vs_mongo_only_platform >> save_in_redis_result_mongo_only_platform >> insert_data_mongo_onlyplatform,update_data_mongo_onlyplatform,delete_data_mongo_onlyplatform, primary_vs_mongo_only_data_old >> save_in_redis_result_mongo_only_old >> delete_data_mongo_onlyold]  >> save_in_redis_end >> save_in_history_mongo_puller >> end

    # [END main_flow]

# [START dag_invocation]
puller_gilat_tasa_10min_ = puller_gilat_tasa_10min()
# [END dag_invocation]
