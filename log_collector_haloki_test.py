# Import thư viện Kafka, json và os
from kafka import KafkaConsumer
import json
import os
# from log_processor_haloki_test import *
from log_processor_haloki_test import *
import pymongo

# Danh sách tên các topic trên kafka-server (K8s)
topic_name_list = ['log-haloki']#, ["log-haloki", "log-tour", "log-hotel", "log-flight","log-user", "log-oauth",  'log-user', 'log-oauth']

# Kêt nối với kafka-server(k8s)
consumer = KafkaConsumer (
    bootstrap_servers = '10.10.11.237:9094,10.10.11.238:9094,10.10.11.239:9094'
    , group_id= 'log_collector_Duy'
    , auto_offset_reset = 'earliest' #auto_offset_reset = 'latest'
    #1, enable_auto_commit=True
    #security_protocol =  'SASL_PLAINTEXT',
    #sasl_mechanism = 'SCRAM-SHA-512',
    #sasl_plain_username='admin-hahalolo',
    #sasl_plain_password='Hahalolo@2021'
    )

# Hàm lấy thông tin từ topic (hứng topic từ K8s) và lưu vào file
def consume_logs(topic_names, list_api):
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
                if "LOG-REQ-RESP-HALOKI" in msg['log'] :
                    # print(msg['log'])
                    result, table = process_log(msg["log"], list_api_haloki) # msg["log"] là kiểu str, xử lý log
                    # result = process_log(msg["log"], list_api)
                    if result != None:
                        print(result)
                        print("Logs collected!\n=============================")
                        haloki = mydb[table]
                        haloki.insert_one(result)
            except ValueError: pass
        except TypeError: pass

# # Chạy hàm main
if __name__== "__main__":
        os.system("cls")
        list_api_haloki = [
            "/acc/hlkAccountInf/v1" # refresh trang (đang xem làm gì để trả thêm) 
            ,"/sendM/hlkSendMCreate/v1","/sendM/hlkSendMUdp/v1" ,"/sendM/hlkSendMDel/v1","/sendM/hlkSendMInf/v1", "/sendM/hlkSendMsInf/v1", "/link/hlkBankAccountSrch/v1" #Send Money
            ,"/hlkTranf/hlkTransferCreate/v1", "/hlkTranf/hlkTransferUdp/v1", "/hlkTranf/hlkTransferInf/v1","/hlkTranf/hlkTransfersInf/v1", "/hlkTranf/hlkTransferDel/v1", "/hlkTranf/hlkTranfRateExchInf/v1" # Transfer Money
            ,"/identity/hlkIdentityInf/v1" # Account Infomation 
            ,"/link/hlkBankAccountsInf/v1", "/identity/hlkIsIdentifyVerifed/v1", "/link/hlkBankAccountCreate/v1" #Link Bank Account
            ,"/recp/hlkRecipientsInf/v1" #Recipient
            , "/hlkTrans/hlkTransSendMsInf/v1" #Transaction History
            , "/acc/hlkUsrUdp/v1" # update thông tin trong phần setting
            ]
        consume_logs(topic_name_list, list_api_haloki)