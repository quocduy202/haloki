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
    global ipaddress, timeview, location
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
        
        if uri in list_api:
            # load json from body_time
            # print(uri)
            json_obj = "{" + body_time + "}"
            json_obj = json.loads(json_obj)
            # print(json_obj)
            timeReq, timeResp = json_obj['timeReq'], json_obj['timeResp']

            #sessionView
            timeview_page = datetime.strptime(timeReq, "%Y-%m-%dT%H:%M:%S.%fZ")

            user_id = re.search(r".*pn100\D*(?P<pn100>\w+).*", user_info)

            if user_id == None:
                user_id = None
            else: user_id = user_id.group(1)

            header_json = json.loads(header)      
            ipaddress = header_json.get('ipaddress', None)
            if ipaddress is not None:
                location = get_location(ipaddress[0])
                # print(location)
            else: location = None
        
            # user_agent_str = header_json['user-agent'][0]
            user_agent_str = header_json.get('user-agent', None)
            user_agent = parse(user_agent_str[0])
            device_info = {}
            device_info['browser'] = user_agent.browser.family
            device_info['os'] = user_agent.os.family
            device_info['brand'] = user_agent.device.brand
            if user_agent.is_mobile:
                device_info['type'] = 'mobile'
            elif user_agent.is_tablet:
                device_info['type'] = 'tablet'
            #user_agent.is_touch_capable
            elif user_agent.is_pc:
                device_info['type'] = 'pc'
            elif user_agent.is_bot:
                device_info['type'] = 'bot'
            if 'type' in device_info:
                device_info['type'] = device_info['type']
            else: device_info['type'] = None
            if uri == "/identity/hlkIdentityHook/v1":
                result = None
            elif uri == "/acc/hlkUsrUdp/v1":
                result = {
                    "timesview": timeview_page
                    ,"user_id": user_id
                    ,"country":location['country']
                    ,"region":location['region']
                    ,"city":location['city']
                    ,"ipaddress":ipaddress[0]
                    # ,"device": device_info
                    ,"browser": device_info["browser"]
                    ,"os": device_info["os"]
                    ,"brand": device_info["brand"]
                    ,"type": device_info["type"]
                    ,"header":header
                    ,"endpoint":uri
                    ,'page': "Update Profile (Settings)"
                }
                table = 'sessionView_test'
                return result, table
            elif uri == '/acc/hlkAccountInf/v1': 
                result = {
                    "timesview": timeview_page
                    ,"user_id": user_id
                    ,"country":location['country']
                    ,"region":location['region']
                    ,"city":location['city']
                    ,"ipaddress":ipaddress[0]
                    # ,"device": device_info
                    ,"browser": device_info["browser"]
                    ,"os": device_info["os"]
                    ,"brand": device_info["brand"]
                    ,"type": device_info["type"]
                    ,"header":header
                    ,"endpoint":uri
                    ,'page': "Refresh Page (f5)"
                }
                table = 'sessionView_test'
                return result, table
            elif uri == "/identity/hlkIdentityInf/v1":
                result = {
                    "timesview": timeview_page
                    ,"user_id": user_id
                    ,"country":location['country']
                    ,"region":location['region']
                    ,"city":location['city']
                    ,"ipaddress":ipaddress[0]
                    # ,"device": device_info
                    ,"browser": device_info["browser"]
                    ,"os": device_info["os"]
                    ,"brand": device_info["brand"]
                    ,"type": device_info["type"]
                    ,"header":header
                    ,"endpoint":uri
                    ,'page': "Account Information"
                }
                table = 'sessionView_test'
                return result, table
            elif uri == "/link/hlkBankAccountsInf/v1" or uri == "/identity/hlkIsIdentifyVerifed/v1":
                result = {
                    "timesview": timeview_page
                    ,"user_id": user_id
                    ,"country":location['country']
                    ,"region":location['region']
                    ,"city":location['city']
                    ,"ipaddress":ipaddress[0]
                    # ,"device": device_info
                    ,"browser": device_info["browser"]
                    ,"os": device_info["os"]
                    ,"brand": device_info["brand"]
                    ,"type": device_info["type"]
                    ,"header":header
                    ,"endpoint":uri
                    ,'page': "Bank Accounts"
                }
                table = 'sessionView_test'
                return result, table
            elif uri == "/recp/hlkRecipientsInf/v1":
                result = {
                    "timesview": timeview_page
                    ,"user_id": user_id
                    ,"country":location['country']
                    ,"region":location['region']
                    ,"city":location['city']
                    ,"ipaddress":ipaddress[0]
                    # ,"device": device_info
                    ,"browser": device_info["browser"]
                    ,"os": device_info["os"]
                    ,"brand": device_info["brand"]
                    ,"type": device_info["type"]
                    ,"header":header
                    ,"endpoint":uri
                    ,'page': "Recipients"
                }
                table = 'sessionView_test'
                return result, table
            elif uri == "/hlkTrans/hlkTransSendMsInf/v1" or uri == "hlkTrans/hlkTranSendMInf/v1":
                result = {
                    "timesview": timeview_page
                    ,"user_id": user_id
                    ,"country":location['country']
                    ,"region":location['region']
                    ,"city":location['city']
                    ,"ipaddress":ipaddress[0]
                    # ,"device": device_info
                    ,"browser": device_info["browser"]
                    ,"os": device_info["os"]
                    ,"brand": device_info["brand"]
                    ,"type": device_info["type"]
                    ,"header":header
                    ,"endpoint":uri
                    ,'page': "Transaction History"
                }
                table = 'sessionView_test'
                return result, table
            elif uri ==  "/sendM/hlkSendMsInf/v1" or uri == "/link/hlkBankAccountSrch/v1": #"/identity/hlkIsIdentifyVerified/v1"
                result = {
                    "timesview": timeview_page
                    ,"user_id": user_id
                    ,"country":location['country']
                    ,"region":location['region']
                    ,"city":location['city']
                    ,"ipaddress":ipaddress[0]
                    # ,"device": device_info
                    ,"browser": device_info["browser"]
                    ,"os": device_info["os"]
                    ,"brand": device_info["brand"]
                    ,"type": device_info["type"]
                    ,"header":header
                    ,"endpoint":uri
                    ,'page': "SendMoney Page"
                }
                table = 'sessionView_test'
                return result, table
            else: # transfer
                # if uri == ""
                result = {
                    "timesview": timeview_page
                    ,"user_id": user_id
                    ,"country":location['country']
                    ,"region":location['region']
                    ,"city":location['city']
                    ,"ipaddress":ipaddress[0]
                    # ,"device": device_info
                    ,"browser": device_info["browser"]
                    ,"os": device_info["os"]
                    ,"brand": device_info["brand"]
                    ,"type": device_info["type"]
                    ,"header":header
                    ,"endpoint":uri
                    ,'page': "TransferMoney Page"
                }
                    # Trả về kết quả chuyển từ dict sang json
                table = 'sessionView_test'
                return result, table
            return None, None