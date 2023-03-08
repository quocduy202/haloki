# Import thư viện cần thiết
import re
import json
from user_agents import parse
from datetime import datetime
import requests
import pandas as pd
from pandas import ExcelWriter
# import httpagentparser
import pandas as pd

bank_account_lst = [
    '/link/hlkLinkTokenCreate/v1'
    ,'/link/hlkBankAccountCreate/v1'
    ,'/link/hlkBankAccountRmv/v1'
    ,'/link/hlkLinkTokenUdp/v1'
    ,'/link/hlkBankAccountUdp/v1'
    ]

sendM_lst = [
    '/sendM/hlkSendMSav/v1'
    ,'/sendM/hlkSendMCreat/v2'
    ,'/sendM/hlkSendMHook/v1'
    ,'/sendM/hlkSyzSendMUdp/v1'
    ]

identify_lst = [
    '/identity/hlkIdentityCreate/v1'
    ,'/identity/hlkIdentityUdp/v1'
    ,'/identity/hlkIdentityEdt/v1'
    ,'/identity/hlkIdentityHook/v1'
    ]

plaid_api_bankaccount = {"/link/hlkLinkTokenCreate/v1" : ["/link/token/create"]
            , "/link/hlkBankAccountCreate/v1" : ["/item/public_token/exchange", "/products/auth", "/api/institution"]
            , "/link/hlkBankAccountRmv/v1" : ["/item/remove"]
            , "/link/hlkLinkTokenUdp/v1" : ["/link/token/create"]
            , "/link/hlkBankAccountUdp/v1" : ["/products/auth", "/api/institution"]}

plaid_api_sendM = {"/sendM/hlkSendMSav/v1" : ["/accounts/balance/get"]
        , "/sendM/hlkSendMCreat/v2" : ["/accounts/balance/get", "/processor/stripe/bank_account_token/create", "/accounts/balance/get"]
            , "/sendM/hlkSendMHook/v1" : ["/accounts/balance/get"]
            , "/sendM/hlkSyzSendMUdp/v1" : ["/accounts/balance/get"]}

plaid_api_identity = {"/identity/hlkIdentityCreate/v1" : ["link/token/create", "/identity_verification/create"]
            , "/identity/hlkIdentityUdp/v1" : ["link/token/create", "identity_verification/retry"]
            , "/identity/hlkIdentityEdt/v1" : ["link/token/create", "/identity_verification/create"]
            , "/identity/hlkIdentityHook/v1" : ["identity_verification/get"]}

# update hàm xử lý dấu
def no_accent_vietnamese(s):
    s = re.sub('[áàảãạăắằẳẵặâấầẩẫậ]', 'a', s)
    s = re.sub('[éèẻẽẹêếềểễệ]', 'e', s)
    s = re.sub('[óòỏõọôốồổỗộơớờởỡợ]', 'o', s)
    s = re.sub('[íìỉĩị]', 'i', s)
    s = re.sub('[úùủũụưứừửữự]', 'u', s)
    s = re.sub('[ýỳỷỹỵ]', 'y', s)
    s = re.sub('đ', 'd', s)
    return s

def get_location(ip_address):
    if ip_address == "":
        location = None
        return location
    while True:
        count = 0
        if count >= 10:
            break
        try:
            # response = requests.post("http://ip-api.com/batch", json=[{"query": ip_address}]).json()
            response = requests.post("http://echoip.hahalolo.com/json", json=[{"query": ip_address}]).json()
            break
        except:
            count += 1
            continue
            # print(response)
    if response == None:
        location = None
        return location
    location = {}
    location["country"] = no_accent_vietnamese(response["country"])
    location["city"] = no_accent_vietnamese(response["city"])
    location["region"] = no_accent_vietnamese(response["region_name"])
    return location
    # print(location)

# Hàm xử lý log
def process_log(topic_name_list, list_api):
    global ipaddress, timeview, location, charge_fee
    """
        Hàm xử lý log từ API trên K8s
    """
    # Phân tích log
    log_regex = re.search(
        #r'(?P<timestamp>\d{4}\-\d{2}\-\d{2}\s+\d{2}\:\d{2}\:\d{2}\.\d{3})\s+(?P<level>\w+)\s+\d+\s+---\s+(?P<processor>\[.*?\])\s+(?P<serverLayer>[^ *]*)\s+: :::LOG-REQ-RESP:::\{\"\w+\"\:\"(?P<query>.*?)\",\"\w+\"\:\"(?P<uri>.*?)\",\"\w+\"\:\"(?P<pathInfo>.*?)\",\"\w+\"\:\"(?P<url>.*?)\",\"\w+\"\:\"(?P<header>\[.*?\])\",\"\w+\"\:\"(?P<method>.*?)\",\"\w+\"\:\"(?P<className>.*?)\",\"\w+\"\:\"(?P<serverName>.*?)\",\"\w+\"\:\"(?P<user>.*?)\",(?P<body>.*),"timeReq"\:(?P<timeReq>.*),"timeResp"\:(?P<timeResp>.*)}'

        # r'(?P<timestamp>\d{4}\-\d{2}\-\d{2}\s+\d{2}\:\d{2}\:\d{2}\.\d{3})\s+(?P<level>\w+)\s+\d+\s+---\s+(?P<processor>\[.*?\])\s+(?P<serverLayer>[^ *]*)\s+: :::LOG-REQ-RESP:::\{\"\w+\"\:\"(?P<query>.*?)\",\"\w+\"\:\"(?P<uri>.*?)\",\"\w+\"\:\"(?P<pathInfo>.*?)\",\"\w+\"\:\"(?P<url>.*?)\",\"\w+\"\:(?P<header>\{.*?\}),\"\w+\"\:\"(?P<method>.*?)\",\"\w+\"\:\"(?P<className>.*?)\",\"\w+\"\:\"(?P<serverName>.*?)\",\"\w+\"\:\"(?P<user>.*?)\",(?P<body>.*)\}\s+'
        r'(?P<timestamp>\d{4}\-\d{2}\-\d{2}\s+\d{2}\:\d{2}\:\d{2}\.\d{3})\s+(?P<level>\w+)\s+\d+\s+---\s+(?P<processor>\[.*?\])\s+(?P<serverLayer>[^ *]*)\s+: :::LOG-REQ-RESP-HALOKI:::\{\"\w+\"\:\"(?P<query>.*?)\",\"\w+\"\:\"(?P<uri>.*?)\",\"\w+\"\:\"(?P<pathInfo>.*?)\",\"\w+\"\:\"(?P<url>.*?)\",\"\w+\"\:(?P<header>\{.*?\}),\"\w+\"\:\"(?P<method>.*?)\",\"\w+\"\:\"(?P<className>.*?)\",\"\w+\"\:\"(?P<serverName>.*?)\",\"\w+\"\:\"(?P<user>.*?)\",(?P<body>.*)\}', topic_name_list
    ) # Tìm kiếm các trường dữ liệu lớn nhất trong file log

    if log_regex is None:
            log_regex = re.search(
        #r'(?P<timestamp>\d{4}\-\d{2}\-\d{2}\s+\d{2}\:\d{2}\:\d{2}\.\d{3})\s+(?P<level>\w+)\s+\d+\s+---\s+(?P<processor>\[.*?\])\s+(?P<serverLayer>[^ *]*)\s+: :::LOG-REQ-RESP:::\{\"\w+\"\:\"(?P<query>.*?)\",\"\w+\"\:\"(?P<uri>.*?)\",Python/cleaning/topic/local_process.py\"\w+\"\:\"(?P<pathInfo>.*?)\",\"\w+\"\:\"(?P<url>.*?)\",\"\w+\"\:\"(?P<header>\[.*?\])\",\"\w+\"\:\"(?P<method>.*?)\",\"\w+\"\:\"(?P<className>.*?)\",\"\w+\"\:\"(?P<serverName>.*?)\",\"\w+\"\:\"(?P<user>.*?)\",(?P<body>.*),"timeReq"\:(?P<timeReq>.*),"timeResp"\:(?P<timeResp>.*)}'
        r'(?P<timestamp>\d{4}\-\d{2}\-\d{2}\s+\d{2}\:\d{2}\:\d{2}\.\d{3})\s+(?P<level>\w+)\s+\d+\s+---\s+(?P<processor>\[.*?\])\s+(?P<serverLayer>[^ *]*)\s+: :::LOG-REQ-RESP:::\{\"\w+\"\:\"(?P<query>.*?)\",\"\w+\"\:\"(?P<uri>.*?)\",\"\w+\"\:\"(?P<pathInfo>.*?)\",\"\w+\"\:\"(?P<url>.*?)\",\"\w+\"\:\"(?P<header>\[.*?\])\",\"\w+\"\:\"(?P<method>.*?)\",\"\w+\"\:\"(?P<className>.*?)\",\"\w+\"\:\"(?P<serverName>.*?)\",\"\w+\"\:\"(?P<user>.*?)\",(?P<body>.*)\}'
        , topic_name_list
    ) # Tìm kiếm các trường dữ liệu lớn nhất trong file log
    # print(log_regex)
    if log_regex != None:
        # print(topic_name_list)

        timestamp = log_regex.group(1)
        level = log_regex.group(2)
        processor = log_regex.group(3)
        server_layer = log_regex.group(4)
        query = log_regex.group(5)
        uri = log_regex.group(6)
        path_info = log_regex.group(7)
        url = log_regex.group(8)
        header = log_regex.group(9)
        method = log_regex.group(10)
        className = log_regex.group(11)
        serverName = log_regex.group(12)
        user_info = log_regex.group(13)
        body_time = log_regex.group(14)
        

        """
        check uri available in list_api in log_collector

        Parse log thành các element cần thiết
        """
        
        if uri in list_api:

            # load json from body_time
            # print(uri)
            json_obj = "{" + body_time + "}"
            json_obj = json.loads(json_obj)
            # print(json_obj)
            timeReq, timeResp = json_obj['timeReq'], json_obj['timeResp']

            #sessionView
            timestamp = datetime.strptime(timeReq, "%Y-%m-%dT%H:%M:%S.%fZ")
            user_id = re.search(r".*pn100\D*(?P<pn100>\w+).*", user_info)



            # check uri
            if uri in bank_account_lst:
                # print(uri)
                module = "Bank Account"
                plaid_api = plaid_api_bankaccount[uri]
                if plaid_api == "/link/hlkLinkTokenCreate/v1":
                    charge_fee = 0.10
                elif plaid_api == "/link/hlkBankAccountCreate/v1":
                    charge_fee = 1.50
                elif plaid_api == "/link/hlkLinkTokenUdp/v1":
                    charge_fee = 0.10
                elif plaid_api == "/link/hlkBankAccountUdp/v1":
                    charge_fee = 1.50
            elif uri in sendM_lst:
                # print(uri)
                module = "Send Money"
                plaid_api = plaid_api_sendM[uri]
                if plaid_api == "/sendM/hlkSendMSav/v1":
                    charge_fee = 0.10
                elif plaid_api == "/sendM/hlkSendMCreate/v1":
                    charge_fee = 0.20
                elif plaid_api == "/sendM/hlkSendMHook/v1":
                    charge_fee = 0.10
                elif plaid_api == "/sendM/hlkSyzSendMUdp/v1":
                    charge_fee = 0.10
            elif uri in identify_lst:
                module = "Identify Verification"
                plaid_api = plaid_api_identity[uri]
                charge_fee = 2.00
            result = {
                "timestamp": timestamp
                # ,"user_id": user_id # null variable
                ,"hlk_API": uri
                ,"Module": module
                ,"plaid_api" : plaid_api
                ,"plaid_fee" : charge_fee
            }
            print(result)
            table = "count_api_test"
            return result, table
        return None, None