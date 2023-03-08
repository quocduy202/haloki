# Import thư viện Kafka, json và os
from kafka import KafkaConsumer
import json
import os
from log_processor_haloki import *
import pymongo
import pandas as pd
import numpy as np

# Danh sách tên các topic trên kafka-server (K8s)
topic_name_list = ["log-haloki", "log-oauth"]#, 'log-user'] "log-tour", "log-hotel", "log-flight","log-user", "log-oauth", ]

# Kêt nối với kafka-server(k8s)
consumer = KafkaConsumer (
    bootstrap_servers = '10.10.11.237:9094,10.10.11.238:9094,10.10.11.239:9094'
    , group_id= 'log_collector_1'
    , auto_offset_reset = 'earliest' #auto_offset_reset = 'latest'
    #1, enable_auto_commit=True
    #security_protocol =  'SASL_PLAINTEXT',
    #sasl_mechanism = 'SCRAM-SHA-512',
    #sasl_plain_username='admin-hahalolo',
    #sasl_plain_password='Hahalolo@2021'
    )

# Hàm lấy thông tin từ topic (hứng topic từ K8s) và lưu vào file
def consume_logs(topic_names, list_api):
    # myclient = pymongo.MongoClient("mongodb://data-warehouse:Hahalolo%402022@10.10.12.201:27017,10.10.12.202:27017,10.10.12.203:27017/?replicaSet=test&authSource=ai-data-warehouse")
    myclient = pymongo.MongoClient("mongodb://test-haloki-data-analyst:Hahalolo%402022@10.10.11.100:27017/?authSource=test-haloki-data-analyst")
    mydb = myclient["test-haloki-data-analyst"]
    # myclient = pymongo.MongoClient("mongodb://localhost:27017")
    # mydb = myclient["datalake"]
    consumer.subscribe(topic_names)

    for message in consumer:
        msg = json.loads(message.value.decode("utf-8"))
        # print(msg)
        # print(msg['log'])
        # xét điều kiện log từ API với dạng LOG-REQ-RESP
        try:
            try:
                try:
                    if "LOG-REQ-RESP-HALOKI" in msg['log'] :
                        # print(msg['log'])
                        result, table = process_log(msg["log"], list_api_haloki) # msg["log"] là kiểu str, xử lý log
                        # result = process_log(msg["log"], list_api)
                        if result != None:
                            print(result)
                            print("Logs collected!\n=============================")
                            haloki = mydb[table]
                            haloki.insert_one(result)
                except KeyError: pass
            except TypeError: pass
        except OverflowError: pass
# # Chạy hàm main
if __name__== "__main__":
        os.system("cls")
        list_api_haloki = [
             
            # "/acc/hlkAccountInf/v1" # refresh trang (đang xem làm gì để trả thêm)

            # ,"/sendM/hlkSendMCreat/v1","/sendM/hlkSendMUdp/v1" ,"/sendM/hlkSendMDel/v1","/sendM/hlkSendMInf/v1", "/sendM/hlkSendMsInf/v1", "/sendM/hlkSendMVerify/v1", 
            # "/sendM/hlkSendMVerifyAvailBal/v1", 
            "/sendM/hlkSendMSav/v1", "/sendM/hlkSendMCreat/v2" #Send Money

            ,"/hlkTranf/hlkTransferCreate/v1",
            #   "/hlkTran/fhlkTransferUdp/v1", "/hlkTranf/hlkTransferCompl/v1","/hlkTranf/hlkTransferDel/v1", 
            # "/hlkTranf/hlkTranfRateExchInf/v1", "/tranfBnk/hlkTransferBnkSav/v1", "/hlkTranf/hlkTransfersInf/v1","/hlkTranf/hlkTransferInf/v1","/tranfBnk/hlkTransferRecipientsInf/v1", "/tranfBnk/hlkRecipientBnksInf/v1" # Transfer Money

            "/identity/hlkIdentityCreate/v1",
            #   "/identity/hlkIdentityCrm/v1", "/identity/hlkIdentityUdp/v1", "/identity/hlkIdentityInf/v1", "/identity/hlkIdentityHis/v1", 
            "/identity/hlkIsIdentifyVerifed/v1",
            #   "/identity/hlkDocTypeInf", "/identity/hlkIdentityEdt/v1" # Account Infomation 

            "/link/hlkLinkTokenCreate/v1", 
            # "/link/hlkLinkTokenUdp/v1", "/link/hlkBankAccountUdp/v1","/link/hlkBankAccountStatUdp/v1", "/link/hlkBankAccountStatUdp/v1",
            # "/link/hlkBankAccountCreate/v1", "/link/hlkBankAccountsInf/v1", "/link/hlkBankAccountInf/v1", "/link/hlkBankAccountRmv/v1", "/link/hlkResetLogin/v1", 
            # "/link/hlkBankAccountSrch/v1"
            # #Link Bank Account

            # ,"/recp/hlkRecipientsInf/v1" #Recipient

            # , "/hlkTrans/hlkTransSendMsInf/v1", "/hlkTrans/hlkTranSendMInf/v1", "/hlkTrans/hlkTranfSendMsDashInf/v1" #Transaction History

            # , "/acc/hlkUsrUdp/v1" # update thông tin trong phần setting

            "/oauth/token" #login

        ]
        consume_logs(topic_name_list, list_api_haloki)
        































