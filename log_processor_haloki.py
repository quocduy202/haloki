# Import thư viện cần thiết
import re
import json
from user_agents import parse
from datetime import datetime
import requests
import pandas as pd
from pandas import ExcelWriter

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
    global location
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
            print(response)
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
    global date_created, location, device_info
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

        # timestamp = log_regex.group(1)
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
            # print(bodyResp)

            #General information of behaviour
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

            ipaddress = header.get('ipaddress', None)
            if ipaddress is not None:
                location = get_location(ipaddress[0])
                # print(ipaddress, location)
            else: location = None
            if 'city' and 'region' in location:
                location['city'] = location['city']
                location['region'] = location['region']
            else: device_info['type'], location["region"] = None

            # user_agent_str = header['user-agent'][0]
            user_agent_str = header.get('user-agent', None)
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
            # print(device_info['type'])

            ############################### HALOKI #################################################################################
            """
            SEND MONEY
            """
            print(bodyResp)
            # tạo/update request giao dịch
            if uri == "/sendM/hlkSendMSav/v1" or uri == "/sendM/hlkSendMCreat/v2":
                print(bodyResp)
                status = bodyResp["status"]["success"]
                if status == True or status == "True" or status == "true":
                    data = bodyResp["elements"][0]
                    date_created = datetime.fromtimestamp(data["nd310"]/1000).strftime('%Y-%m-%d %H:%M:%S')
                    status_send = data["nv302"]
                    info = data["n310"]
                    total_fee = data["nn315"]
                    money = data["nn305"]
                    ach_fee = data["nn306"]
                    hlk_fee = data["nn307"]
                    total_amount = data["nn308"]
                    id_ = data["id"]                          ### Sửa lại ID chỗ này - Sai trường nhưng không ảnh hưởng nhiều

                    result = {
                        "User ID": user_id,
                        "Date Created": date_created,
                        "Endpoint": uri,
                        "Status": status_send,
                        "Info Bank": info,
                        "Total Fee": total_fee,
                        "Money Amount": money,
                        "ACH Fee": ach_fee,
                        "HLK Fee": hlk_fee,
                        "Total Amount": total_amount,
                        "Time Req": timestamp_req,
                        "Time Resp": timestamp_resp
                        # "ID Transaction": id_
                    }
                    # print(result)
                    table = "TestSend"
                    return result, table
                return None, None
            
            elif uri == "/hlkTranf/hlkTransferCreate/v1":
                print(bodyResp)
                status = bodyResp["status"]["success"]
                if status == True or status == "True" or status == "true":
                    data = bodyResp["elements"][0]
                    
                    ### RECEIVER INFO ###
                    infos = data["n401"]
                    id_receiver = data["pun100"]

                    date_created = datetime.fromtimestamp(data["dl146"]/1000).strftime('%Y-%m-%d %H:%M:%S')
                    sender_infos = data["n200"]

                    status_trans = data["nv402"]
                    date_trans = data["nd410"]
                    wallet_type = data["nn403"]
                    if wallet_type == 0:
                        wallet_type = "personal"
                    else: wallet_type = "business"

                    money_received = data["nn405"]
                    available_backup = data["nn407"] # lấy trước định nghĩa sau
                    trans_value = data["nn408"]
                    money_status = data["nn412"]
                    if money_status == 1:
                        money_status = "income"
                    else: money_status = "outcome"
                    targ_receive = data["nn413"]
                    if targ_receive == 0:
                        targ_receive = "user"
                    elif targ_receive == 1:
                        targ_receive = "admin"
                    else: targ_receive = "batch job"

                    result = {
                        "Date Created": date_created,
                        "ID Receiver": id_receiver,
                        "Infos Receiver": infos,
                        "Sender Infos": sender_infos,
                        "Status Trans": status_trans,
                        "Date Trans": date_trans,
                        "Wallet Type": wallet_type,
                        "Money Received": money_received,
                        "Available Back Up": available_backup,
                        "Money Amount": trans_value,
                        "Money Status": money_status,
                        "Target Received": targ_receive,
                        "Time Req": timestamp_req,
                        "Time Resp": timestamp_resp
                    }
                    # print(result)
                    table = "TestTransfer"
                    return result, table
                return None, None
            
            elif uri == "/oauth/token":
                # print(bodyResp)
                # print(location['city'])
                # print(device_info['type'])
                status = bodyResp["status"]["success"]
                if status == True or status == "True" or status == "true":
                    data = bodyResp["elements"][0]
                    # print(data)
                    firstname = data["nv103"]
                    lastname = data["nv104"]
                    fullname = data["nv105"]
                    birthday = data["nd106"]
                    gender = data["nv107"]
                    if gender == "M":
                        gender = "male"
                    elif gender == "F":
                        gender = "female"
                    else: gender = "other"

                    # location = location["city"]
                    username = data["nv108"]
                    countrycode = data["nv111"]
                    phonecode = data["nv113"]
                
                    result = {
                        "user_id": user_id,
                        "firstname": firstname,
                        "lastname": lastname,
                        "fullname": fullname,
                        "birthday": birthday,
                        "gender": gender,
                        "username": username,
                        "country": countrycode,
                        "phonecode": phonecode,
                        "Time Req": timestamp_req,
                        "Time Resp": timestamp_resp,
                        "City": location['city'],
                        "Region": location['region'],
                        "Device": device_info['type']
                    }
                    # print(result)
                    table = "loginTest"
                    return result, table
                return None, None
            
            elif uri == "/identity/hlkIsIdentifyVerifed/v1":
                status = bodyResp["status"]["success"]
                if status == "false" or status == False:
                    result = {
                        "userid": user_id,
                        "verify_status": "non-verify",
                        # "error": bodyResp["status"]["error"]
                    }
                    # print(result)
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
                    # print(result)
                    table = "verifyTest"
                    return result, table
        return None, None
        # Bởi vì 4 trường bên dưới được định dạng chuẩn theo json nên chúng ta sẽ không cần phải định dạng lại và parse thẳng theo hàm json.load

        #lượng trung bình money khi sendM
        # màn hình, API nào 