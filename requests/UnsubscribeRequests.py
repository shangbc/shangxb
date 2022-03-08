# Time     2021/11/08 10::08
# Auhtor
# ide      PyCharm
# Verion   1.0
# function 默认

from ToolTemplate import ToolTemplate
from MethodTools import getConfigFile
from RequestUtil import getRequestContent
from dateutil .relativedelta import relativedelta
from apscheduler .schedulers .blocking import BlockingScheduler
from apscheduler .triggers .cron import CronTrigger

import uuid
import datetime
import time
import requests
import pandas as pd
import traceback
import threading
import queue
import json


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
def getRequestContext(requestData):
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
        nowTime = datetime .datetime .now() .strftime('%Y%m%d%H%M%S')
        id = str(uuid.uuid1()).replace("-", "")
        timeStr = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        returnId = int(round(time .time() * 1000000)) .__str__()

        headers = {
            "domain": "CMCOP",
            "routevalue": "998",
            "version": "v1",
            "sign": "571",
            "appid": "571",
            "APICode": "CIP00155",
            "messageId": id,
            "timeStamp": timeStr
        }
        data = {
                    "buyerComments": getKey(requestData, 'BUYERCOMMENTS', returnId),
                    "buyerNickname": getKey(requestData, 'BUYERNICKNAME', returnId),
                    "buyerReturnTime": getKey(requestData, 'BUYERRETURNTIME', nowTime),
                    "contactPhone": getKey(requestData, 'PHONE', nowTime),
                    "extChannelReturnTime": getKey(requestData, 'EXTCHANNELRETURNTIME', nowTime),
                    "extReturnId": getKey(requestData, 'EXTRETURNID', returnId),
                    "feedbackUrl": getKey(requestData, 'FEEDBACKURL', ""),
                    "number": getKey(requestData, 'PHONE'),
                    "orderId": getKey(requestData, 'ORDERID'),
                    "orderIdType": getKey(requestData, 'ORDERIDTYPE', "1"),
                    "payment": getKey(requestData, 'PAYMENT', 0),
                    "refundAmount": getKey(requestData, 'REFUNDAMOUNT', 0),
                    "refundReason": getKey(requestData, 'REFUNDREASON', ""),
                    "returnGoodsInfo": [
                        {
                            "goodsId": getKey(requestData, 'GOODSID'),
                            "goodsName": getKey(requestData, 'GOODSNAME'),
                            "price": getKey(requestData, 'PRICE', 0),
                            "returnQuantity": getKey(requestData, 'RETURNQUANTITY', 1),
                        }
                    ],
                    "returnType": getKey(requestData, 'RETURNTYPE'),
                    "shopCode": getKey(requestData, 'SHOPCODE', ""),
                    "shopName": getKey(requestData, 'SHOPNAME', ""),
                    "subOrderId": getKey(requestData, 'SUBORDERID', ""),
                    "totalFee": getKey(requestData, 'TOTALFEE', 0),
                }
        loggers .info(data)
        dataJsons = getRequestContent(url, data, headers)
        loggers .info(dataJsons)
        return dataJsons
    except Exception as error:
        loggers .error(error)
        raise error


productionStatus = True
consumptionStatus = False

threadLock = threading .Lock()
queueData = queue .Queue()


def release():
    if threadLock .locked():
        threadLock .release()


def acquire():
    threadLock .acquire()


# 消费者
class ConsumptionClass(threading .Thread):
    def __init__(self, index):
        threading .Thread .__init__(self)
        self .setName(index)

    def run(self):
        UpperRequest()


# 生产者
class ProductionClass(threading .Thread):
    def __init__(self, index):
        threading .Thread .__init__(self)
        self .setName(index)

    def run(self):
        getUpperData()


def UpperRequest():
    try:
        global productionStatus, consumptionStatus
        while True:
            try:
                db = None
                data = None
                if consumptionStatus:
                    db = tool .getDbSourct()
                    acquire()
                    if not queueData .empty():
                        data = queueData .get()
                    else:
                        productionStatus = True
                        consumptionStatus = False
                        continue

                    release()

                    upperCode = '00'
                    upperDesc = ''
                    try:
                        # {'respCode': '00000', 'respDesc': 'success', 'result': {'bizCode': '0000', 'bizDesc': '成功'}}
                        result = getRequestContext(data)
                        loggers.info('CIP00155上发成功')
                        if ('respCode' in result and result['respCode'] == '00000') and \
                                ('result' in result and result and result['result']['bizCode'] == '0000'):
                            upperCode = '00'
                        else:
                            upperCode = '99'
                        upperDesc = str(result)

                    except Exception as error:
                        upperCode = '99'
                        upperDesc = error .__str__()
                        loggers.info('CIP00155上发失败')
                        loggers .error(error)

                    dataDict = {
                        'status': 'E' if upperCode == '00' or data['UPPER_NUMS'] >= 2 else 'U',
                        'upper_code': upperCode,
                        'upper_desc': upperDesc,
                    }
                    rowid = data['ROWID']
                    updateSql = """
                                update {environment}.unsubscribe_upper_task t
                                set t.status = :status,
                                    t.upper_code = :upper_code,
                                    t.upper_desc = :upper_desc,
                                    t.upper_nums = t.upper_nums + 1,
                                    t.upper_time = sysdate,
                                    t.upper_time_next = sysdate + 2/1440
                                where rowid = '{rowid}'
                                """ .format(environment=environment, rowid=rowid)
                    db .update('cboss', updateSql, dataDict)

                else:
                    loggers.info("当前生产者正在处理，消费者者休眠60s")
                    release()
                    time.sleep(60)

            except Exception as error:
                if db is not None:
                    db .rollback()
                loggers .error(error)
            else:
                if db is not None:
                    db .commit()
            finally:
                if db is not None:
                    db .close()
                release()
    except Exception as error:
        loggers .error(error)


def getUpperData():
    try:
        global productionStatus, consumptionStatus
        while True:
            try:
                db = tool.getDbSourct()
                if productionStatus:
                    acquire()
                    upperDateSql = """
                                   select t.*, rowid from {environment}.unsubscribe_upper_task t
                                    where t.upper_time_next <= sysdate
                                          and t.upper_nums < 3
                                          and t.status = 'U'
                                   """ .format(environment=environment)
                    upperDateData = db .select('cboss', upperDateSql)
                    for data in upperDateData:
                        queueData .put(data)

                    if queueData .empty():
                        loggers.info("当前无数据需要处理，生产者休眠60s")
                        time .sleep(60)
                        continue

                    productionStatus = False
                    consumptionStatus = True
                    continue

                else:
                    loggers.info("当前消费者正在处理，生产者休眠60s")
                    release()
                    time .sleep(60)
            except Exception as error:
                db .rollback()
                loggers .error(error)
            else:
                db .commit()
            finally:
                db .close()
                release()
    except Exception as error:
        loggers .error(error)


def exce():
    productionTheradList = []
    consumptionTheradList = []

    for index in range(productionTheradNums):
        productionTherad = ProductionClass("production" + index .__str__())
        productionTheradList .append(productionTherad)
    for index in range(consumptionTheradNums):
        consumptionTherad = ConsumptionClass("consumption" + index .__str__())
        consumptionTheradList .append(consumptionTherad)

    for productionTherad in productionTheradList:
        productionTherad .start()
    for consumptionTherad in consumptionTheradList:
        consumptionTherad .start()

    for productionTherad in productionTheradList:
        productionTherad .join()
    for consumptionTherad in consumptionTheradList:
        consumptionTherad .join()

if __name__ == '__main__':
    configFilepath = 'E:/程序/pyhton/shangxb/configStatic/UnsubscribeRequestsConfig'
    # configFilepath = '/app/cbapp/program/config/UnsubscribeRequestsConfig'
    configData = getConfigFile(configFilepath)

    programName = dict(configData.items('PROGRAM'))['programName']
    loggingFileName = dict(configData.items('LOGGING'))['fileName']
    crontab = dict(configData.items('CRONTAB'))['crontab']
    environment = dict(configData.items('CONFIG'))['environment']
    timing = dict(configData.items('CONFIG'))['timing']
    productionTheradNums = int(dict(configData.items('CONFIG'))['productionTheradNums'])
    consumptionTheradNums = int(dict(configData.items('CONFIG'))['consumptionTheradNums'])

    tool = ToolTemplate(dataSourctName=programName, logginFileName=loggingFileName)
    loggers = tool.getLoggers()
    configDict = tool.getConfigDict()

    # 切换日志级别
    loggers .setLogger('info')

    # 判断是否开启执行器
    if timing == 'true':
        scheduler = BlockingScheduler()
        scheduler .add_job(exce, CronTrigger .from_crontab(crontab))
        scheduler .start()
    else:
        exce()