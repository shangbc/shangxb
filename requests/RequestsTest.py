# Time     2021/11/08 10::08
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 默认

import datetime
import requests
import json
import sys
import uuid
import logging

# 设置请求头
# headers = {
#     # "User - Agent": "Mozilla / 5.0(Windows NT 10.0;Win64;x64;rv: 68.0) Gecko / 20100101 Firefox / 68.0",
#     "Content-Type": "application/json;charset=utf-8"
# }
id = str(uuid.uuid1()) .replace("-", "")

timeStr = datetime .datetime .now() .strftime("%Y%m%d%H%M%S")

headers = {
    "domain": "CMCOP",
    "routevalue": "998",
    "version": "v1",
    "sign": "571",
    "appid": "571",
    "APICode": "CIP00165",
    "messageId": id,
    "timeStamp": timeStr
}


# 获取连接
def getRequestContent(requestUrl, requestData):
    try:
        print(requests .status_codes .codes)
        # 设置连接信息
        requestsContent = requests .post(url=requestUrl, data=json .dumps(requestData), headers=headers)
        # 提取接口返回的数据
        requestsTextStr = requestsContent .text
        # 返回的是一个字符串的数据,转换为字典类型
        requestsTextJson = json .loads(requestsTextStr)
        return requestsTextJson
    except Exception as error:
        raise error


# 获取接口返回的数据
def getRequestContext():
    try:
        url = "http://10.254.50.252:3911/v3"
        # data = {
        #     "endDate": "",
        #     "startDate": "",
        #     "pageSize": 10,
        #     "pageNum": 1,
        #     "orderType": "",
        #     "bpId": "",
        #     "number": "13587851710"
        # }
        data = {
            "number": "13587851710"
        }
        dataJsons = getRequestContent(url, data)
        logging .info(dataJsons)
    except Exception as error:
        raise error


if __name__ == '__main__':
    getRequestContext()