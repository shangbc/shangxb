# Time     2021/11/08 10::08
# Auhtor
# ide      PyCharm
# Verion   1.0
# function 默认



import uuid
import datetime
import time
import requests
import json
import logging
import ProgramConfigurationOnly


# 设置请求头
# headers = {
#     # "User - Agent": "Mozilla / 5.0(Windows NT 10.0;Win64;x64;rv: 68.0) Gecko / 20100101 Firefox / 68.0",
#     "Content-Type": "application/json;charset=utf-8"
# }

# 获取连接
def getRequestContent(requestUrl, requestData, headers):
    try:
        # 设置连接信息
        requestsContent = requests .post(url=requestUrl, data=json .dumps(requestData), headers=headers, timeout=5)
        # 提取接口返回的数据
        requestsTextStr = requestsContent .text
        # 返回的是一个字符串的数据,转换为字典类型
        requestsTextJson = json .loads(requestsTextStr)
        return requestsTextJson
    except Exception as error:
        raise error


def getKey(myDict, key, defaule=None):
    try:
        if defaule is not None:
            if key in myDict and myDict[key] is not None:
                return myDict[key]
            else:
                return defaule
        else:
            if key in myDict:
                return myDict[key]
            else:
                raise Exception('必填字段无值：' + key)
    except Exception as error:
        raise error


# 获取接口返回的数据
def getRequestContext():
    try:
        url = "http://esbhttp.zj.chinamobile.com:20110/callback"
        # data = {
        #     "endDate": "",
        #     "startDate": "",
        #     "pageSize": 10,
        #     "pageNum": 1,
        #     "orderType": "",
        #     "bpId": "",
        #     "number": "13587851710"
        # }
        nowTime = datetime .datetime .now() .strftime('%Y%m%d%H%M%S')
        id = str(uuid.uuid1()).replace("-", "")
        timeStr = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        returnId = int(round(time .time() * 1000000)) .__str__()
        nowDate = datetime.datetime.now()

        headers = {
            "domain": "CMCOP",
            "routevalue": "998",
            "version": "v1",
            "sign": "571",
            "appid": "571",
            "APICode": "CIP00182",
            "messageId": id,
            "timeStamp": timeStr
        }
        data = {
                    "goodsId": "2022999900008648",
                    "activeTime": "20220215094424",
                    "origin": "02",
                    "prGoodsId": "600000768819",
                    "oprType": "01",
                    "goodsType": "02",
                    "number": "19521542581",
                    "expireTime": "20991231000000",
                    "activeStatus": "0",
                    "busiSeq": '571CIP00182' + nowDate.strftime('%Y%m%d%H%M%S')+str(nowDate.microsecond),
                    "oprDesc": "新用户入网与订套餐",
                    "effectTime": "20220215094424",
                    "numType": "1",
                    "provinceRelationId": "65005468295138",
                    "oprTime": "20220215094424"
                }
        logging.info(data)
        dataJsons = getRequestContent(url, data, headers)
        logging.info(dataJsons)
        return dataJsons
    except Exception as error:
        logging.error(error)
        raise error

getRequestContext()