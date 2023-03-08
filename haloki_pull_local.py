# import libraries
import re
import os
from pymongo import MongoClient
from pprint import pprint

# Create a function to format user-info's documents
def format_user_info():
    """
        A function to format user-info's documents
    """
    if database == 1:
        # Connect to server test
        rootClient = MongoClient("mongodb://test-haloki-data-analyst:Hahalolo%402022@10.10.11.100:27017/?authSource=test-haloki-data-analyst")
        rootDB = rootClient['test-haloki-data-analyst']
        rootCol = rootDB['sessionView_test']

        # Connect to localhost
        myClient = MongoClient('mongodb://localhost:27017')
        myDB = myClient['datalake']
        myCol_formated = myDB['sessionView_test']
    elif database == 2:
        # Connect to server test
        rootClient = MongoClient("mongodb://test-haloki-data-analyst:Hahalolo%402022@10.10.11.100:27017/?authSource=test-haloki-data-analyst")
        rootDB = rootClient['test-haloki-data-analyst']
        rootCol = rootDB['sessionLog_test']

        # Connect to localhost
        myClient = MongoClient('mongodb://localhost:27017')
        myDB = myClient['datalake']
        myCol_formated = myDB['sessionLog_test']
    else:
        # Connect to server test
        rootClient = MongoClient("mongodb://test-haloki-data-analyst:Hahalolo%402022@10.10.11.100:27017/?authSource=test-haloki-data-analyst")
        rootDB = rootClient['test-haloki-data-analyst']
        rootCol = rootDB['reqTrans_test']

        # Connect to localhost
        myClient = MongoClient('mongodb://localhost:27017')
        myDB = myClient['datalake']
        myCol_formated = myDB['reqTrans_test']
    # Read all records on db
    count = 1
    if database == 1:
        for doc in list(rootCol.find()):
            try:
                # Extract neccesary fields
                # timestamp = doc["timestamp"]
                # user_id = doc['user_id']
                # id_haloki = doc["id_haloki"]
                # fname = doc["name"]
                # country_c = doc['country']
                # status = doc['status']
                # gender = doc['gender']
                # device = doc['device']
                # location = doc['location']
                # mney = doc['money']
                # currency = doc['currency']
                # acc_create_date = doc["account create date"]
                # blocking_acc = doc['block status']
                # account_type = doc['account type']
                # # aggregate fields
                # data = {
                #     "timestamp": timestamp
                #     ,"user_id": user_id
                #     ,"id_haloki":id_haloki
                #     ,"name":fname
                #     ,"country": country_c
                #     ,"status":status
                #     ,"gender": gender
                #     ,"device": device
                #     ,"location": location
                #     ,"money":mney
                #     ,"currency": currency
                #     ,"account create date": acc_create_date
                #     ,"block status": blocking_acc
                #     ,"account type": account_type
                # }
                timesview = doc["timesview"]
                user_id = doc['user_id']
                address = doc["address"]
                ipaddress = doc["ipaddress"]
                device_info = doc['device']
                header = doc['header']
                endpoint = doc['endpoint']
                page = doc['page']
                # aggregate fields
                data = {
                    "timesview": timesview
                    ,"user_id": user_id
                    ,"address":address
                    ,"ipaddress":ipaddress
                    ,"device": device_info
                    ,"header":header
                    ,"endpoint":endpoint
                    ,'page': page
                }
                # insert to db
                myCol_formated.insert_one(data)
                print('Finished!')
            except: print("Failed to insert")
    elif database == 2:
        for doc in list(rootCol.find()):
            try:
                # Extract neccesary fields
                timestamp = doc["timestamp"]
                account_id = doc["account_id"]
                bank_id = doc['bank_id']
                description = doc["description"]
                account_name = doc["account_name"]
                acct_full_name = doc['acct_full_name']
                account_mask = doc['account_mask']
                account_number = doc['account_number']
                bank_name = doc['bank_name']
                available_balance = doc['available_balance']
                current_balance = doc['current_balance']
                sendM = doc['SendM']
                total_amount_transaction = doc["total_amount_transaction"]
                status = doc["status"]
                send_date = doc["send_date"]
                update_date = doc["update_date"]
                id_sender = doc["id_sender"]
                id_transaction = doc["id_transaction"]



                data = {
                    "timestamp": timestamp
                    ,"account_id": account_id
                    ,"bank_id": bank_id
                    ,"description": description
                    ,"account_name": account_name
                    ,"acct_full_name": acct_full_name
                    ,"account_mask": account_mask
                    ,"account_number": account_number
                    ,"bank_name": bank_name
                    ,"available_balance": available_balance
                    ,"current_balance": current_balance
                    ,"SendM": sendM
                    ,"total_amount_transaction": total_amount_transaction
                    ,"status": status
                    ,"send_date": send_date
                    ,"update_date": update_date
                    ,"id_sender": id_sender
                    ,"id_transaction": id_transaction
                }
                myCol_formated.insert_one(data)
                print('Finished!')
            except: print("Failed to insert")
    elif database == 3:
        for doc in list(rootCol.find()):
            try:
                # Extract neccesary fields
                # id_user = doc["id_user"]
                # id_haloki = doc['id_haloki']
                # date_transfer = doc["date_transfer"]
                # trans_amount = doc["trans_amount"]
                # currency_transfer = doc['currency_transfer']
                # exchange_amount = doc['exchange_amount']
                # id_receiver = doc['id_receiver']
                # receiver_name = doc['receiver_name']
                # receiver_country = doc['receiver_country']
                # receiver_bank_account_number = doc['receiver_bank_account_number']
                # receiver_bank_id = doc['receiver_bank_id']
                # trans_id = doc["trans_id"]
                # trans_status = doc["trans_status"]
                # subject_type = doc["subject_type"]
                # income_outcome = doc["income_outcome"]

                address = doc["address"]
                device = doc['device']
                endpoint = doc["endpoint"]
                header = doc["header"]
                ipaddress = doc['ipaddress']
                reqContent = doc['reqContent']
                respContent = doc['respContent']
                status = doc['status']
                timeReq = doc['timeReq']
                timeResp = doc['timeResp']
                timesview = doc['timesview']
                type = doc["type"]
                user_id = doc["user_id"]
 
                data = {
                    "address": address
                    , "device": device
                    , "endpoint" : endpoint
                    , "header" : header
                    , 'ipaddress' : ipaddress
                    , "reqContent" : reqContent
                    , "respContent" : respContent
                    , "status" : status
                    , "timeReq" : timeReq
                    , "timeResp" : timeResp
                    , "timesview" : timesview
                    , "type" : type
                    , "user_id" : user_id
                }
                myCol_formated.insert_one(data)
                print('Finished!')
            except: print("Failed to insert")
        # print('Finished!')

        # limit the number of records (for testing only)
    #     if count == num_records+1: 
    #         print('Finished!')
    #         break
    # return True

# run main
if __name__ == '__main__':
        os.system('cls')
        print("Chose database: \n1. sessionView \n2. sessionLog \n3. reqTrans")
        database = int(input())
        format_user_info()
        
        # rerun = input("Do you want to continue? (y/n): ")
        # if rerun == "n":
        #     print("Thanks for using this program!")
