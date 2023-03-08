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

# bank_account_lst = [
#     '/link/hlkLinkTokenCreate/v1'
#     ,'/link/hlkBankAccountCreate/v1'
#     ,'/link/hlkBankAccountRmv/v1'
#     ,'/link/hlkLinkTokenUdp/v1'
#     ,'/link/hlkBankAccountUdp/v1'
#     ]

# sendM_lst = [
#     '/sendM/hlkSendMSav/v1'
#     ,'/sendM/hlkSendMCreat/v2'
#     ,'/sendM/hlkSendMHook/v1'
#     ,'/sendM/hlkSyzSendMUdp/v1'
#     ]

# identify_lst = [
#     '/identity/hlkIdentityCreate/v1'
#     ,'/identity/hlkIdentityUdp/v1'
#     ,'/identity/hlkIdentityEdt/v1'
#     ,'/identity/hlkIdentityHook/v1'
#     ]

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
        
        # print(uri)

        """
        check uri available in list_api in log_collector

        Parse log thành các element cần thiết
        """
        
        if uri in list_api:

           #json_obj include bodyReq, bodyResp, TimeReq, TimeResp, these object have correct json format.
            json_obj = '{' + body_time + '}'  
            #Try-except to solve log pattern wrong but true with above regex.
            try:
                json_obj = json.loads(json_obj)
            except:
                #wrong format json
                #solve {"bodyReq": , ...}
                print('wrong format json: ', uri)
                print(json_obj)
                idx = json_obj.find('"bodyReq":,')
                json_obj = json_obj[:idx] + '"bodyReq":"",' + json_obj[idx+11:]
                try:
                    json_obj = json.loads(json_obj)
                except:
                    print(uri, "not in the format, not collected")
                    return None, None
            bodyReq, bodyResp, timeReq, timeResp = json_obj['bodyReq'], json_obj['bodyResp'], json_obj['timeReq'], json_obj['timeResp']

            timestamp_req = datetime.strptime(timeReq, '%Y-%m-%dT%H:%M:%S.%fZ')
            timestamp_resp = datetime.strptime(timeResp, '%Y-%m-%dT%H:%M:%S.%fZ')
            try:
                header = json.loads(header)
            except:
                print(uri, 'wrong json format!')
                return None, None
            if header.get('api_app_key') != None:
                platform = 'mobi'
            elif "Postman" in header['user-agent'][0]: # Xét điều kiện user agent để lấy dữ liệu platform
                platform = "postman"
            else:
                platform = 'web'
            if re.search(r".*pn100\D*(?P<pn100>\w+).*", user_info) == None:
                user_id = user_info
            else:
                user_id = re.search(r".*pn100\D*(?P<pn100>\w+).*", user_info).group(1)
            user_agent_str = header['user-agent'][0]
            user_agent = parse(user_agent_str)
            device_info = {}

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
            if header.get('ipaddress') != None:
                # print(header.get('ipaddress'))
                location = get_location(header['ipaddress'][0])
            else:
                location = None

            
            #################################################################

            if uri == "/identity/hlkIsIdentifyVerifed/v1":
                status = bodyResp["status"]["success"]
                if status == "false" or status == False:
                    result = {
                        "userid": user_id,
                        "verify_status": "non-verify",
                        # "error": bodyResp["status"]["error"]
                    }
                    
                    table = "verifyTest"
                    return result, table
                # return None, None
                else:
                    verify_status = bodyResp["elements"][0]["status"]
                    if verify_status == "R":
                        verify_status = "review"
                    else: verify_status = "pass"

                    result = {
                        "userid": user_id,
                        "verify_status": verify_status,
                        # "error": bodyResp["status"]["error"]
                    }
                    table = "verifyTest"
                    return result, table
            else:
                return None, None    
        return None, None