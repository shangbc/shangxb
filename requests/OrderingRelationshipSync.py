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
import queue
import threading
import logging
from ToolTemplate import ToolTemplate
from MethodTools import getConfigFile
from RequestUtil import getRequestContent
from RequestUtil import getRequestContent
from dateutil .relativedelta import relativedelta
from apscheduler .schedulers .blocking import BlockingScheduler
from apscheduler .triggers .cron import CronTrigger



# 创建队列
dataQueue = queue .Queue()

# 线程锁
thradingLock = threading .Lock()

# 标识
allBoolen = True
consumptionBoolen = True


# 线程类
class ConsumptionThrading(threading.Thread):
    def __init__(self, index):
        threading.Thread.__init__(self)
        self.setName("Threading" + str(index))

    def run(self):
        upperRequest()


def upperRequest():
    global consumptionBoolen
    dbTherad = tool.getDbSourct()
    while consumptionBoolen:
        try:
            upperData = None
            thradingLock .acquire()
            if not dataQueue .empty():
                upperData = dataQueue .get()
            else:
                consumptionBoolen = False
                thradingLock .release()
                continue
            thradingLock .release()

            billId = upperData['BILL_ID']
            requectDesc, message = getRequestContext(billId)

            requectDescStr = str(requectDesc)
            respCode = requectDesc['respCode'] if 'respCode' in requectDesc else '9999'
            bizCode = requectDesc['result']['bizCode'] if 'result' in requectDesc else '9999'
            updateDate(dbTherad, billId, respCode, bizCode, requectDescStr, message)

            loggers .info(upperData)
        except Exception as error:
            loggers .error(error)

    dbTherad .close()


# 获取接口返回的数据
def getRequestContext(number):
    try:
        orderId = str(uuid.uuid1()).replace("-", "")
        createTime = datetime .datetime .now() .strftime('%Y%m%d%H%M%S')
        id = str(uuid.uuid1()).replace("-", "")
        timeStr = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

        url = "http://10.254.50.252:3911/v3"
        headers = {
            "domain": "CMCOP",
            "routevalue": "998",
            "version": "v1",
            "sign": "571",
            "appid": "571",
            "APICode": "CIP00080",
            "messageId": id,
            "timeStamp": timeStr
        }

        data = {
                "shopCode": "Sa0000220",
                "buyerNickname": number,
                "orderId": orderId,
                "createTime": createTime,
                "needInvoice": "2",
                "opId": "",
                "shopName": "省公司",
                "needDistribution": "2",
                "subOrderList": [
                    {
                        "subOrderId": orderId,
                        "subtotalFee": 0,
                        "subscriberInfo": {
                            "number": number,
                            "numberType": "1",
                            "numberOprType": "12"
                        },
                        "orderStatus": "00",
                        "activityInfo": {
                            "activityCode": "230120560461324288",
                            "activityName": "权益超市黄金会员（原子产品-订购）订购",
                            "activityExpTime": "20991231235959",
                            "activityType": "10",
                            "activityEffTime": createTime,
                            "activityCodeType": "0"
                        },
                        "adjustFee": 0,
                        "goodsInfo": {
                            "amount": 1,
                            "goodsProvince": "浙江省",
                            "corGoodsId": "5710400200002770001",
                            "goodsId": "2021999800022476",
                            "price": 0,
                            "goodsCity": "杭州市",
                            "goodsTitle": "权益超市黄金会员（原子产品-订购）"
                        }
                    }
                ],
                "orgId": "",
                "buyerAccountType": "MOBILE",
                "paymentInfo": {
                    "chargeType": "1",
                    "payment": 0,
                    "paymentTime": createTime
                }
            }
        dataJsons = getRequestContent(url, data, headers)
        # requectDesc = "{'respCode': '00000', 'respDesc': 'success', 'result': {'bizCode': '0000', 'bizDesc': 'success'}}"
        # dataJsons = eval(requectDesc)
        return dataJsons, str(data)
    except Exception as error:
        raise error


def updateDate(dbTherad, billId, respCode, bizCode, requectDesc, message):
    try:
        upperSql = """
                      update bomc.WLL20211214_HUANGJINHUIYUAN t
                      set t.do_flag = 99
                      where t.bill_id = '{billId}'
                      """ .format(billId=billId)
        insertDict = {
            'bill_id': billId,
            'message': message,
            'respCode': respCode,
            'bizCode': bizCode,
            'requect_desc':requectDesc,
            'upper_time': datetime .datetime .now()
        }
        dbTherad .update('pd', upperSql)
        dbTherad .insertMany('pd', 'bomc.Ordering_Relationship_Synclog', [insertDict])

    except Exception as error:
        dbTherad .rollback()
        raise error
    else:
        dbTherad .commit()


def getUpperData():
    try:
        # getUpperSql = """
        #               select * from  bomc.WLL20211214_HUANGJINHUIYUAN t
        #               where do_flag=1
        #                     and rownum <= 11000
        #                     and rownum > 10000
        #               """
        getUpperSql = """
                      select *
                      from (select t.*, rownum as rn
                              from bomc.WLL20211214_HUANGJINHUIYUAN t
                             where do_flag = 1) a
                      where a.rn <= 11000
                            and a.rn > 10000
                      """
        upperData = db .select('pd', getUpperSql)
        return upperData

    except Exception as error:
        raise error


def exce():
    try:
        global allBoolen, consumptionBoolen
        while allBoolen:
            try:
                upperDataList = getUpperData()
                if len(upperDataList) == 0:
                    allBoolen = False

                consumptionBoolen = True

                loggers .info('数据存放进队列中')
                thradingLock .acquire()
                for upperData in upperDataList:
                    dataQueue .put(upperData)
                thradingLock .release()
                loggers.info('数据存放进队列成功')
                loggers .info(dataQueue .qsize())

                thradingList = []
                for index in range(10):
                    myThrading = ConsumptionThrading(index)
                    thradingList .append(myThrading)

                loggers .info('线程启动')
                for thradings in thradingList:
                    thradings .start()

                for thradings in thradingList:
                    thradings .join()
                loggers.info('线程结束')

                del thradingList

            except Exception as error:
                loggers .error(error)
    except Exception as error:
        loggers .error(error)


if __name__ == '__main__':
    # configFilepath = 'E:/程序/pyhton/shangxb/configStatic/OrderingRelationshipSyncConfig'
    configFilepath = '/app/cbapp/program/config/OrderingRelationshipSyncConfig'
    configData = getConfigFile(configFilepath)

    programName = dict(configData.items('PROGRAM'))['programName']
    loggingFileName = dict(configData.items('LOGGING'))['fileName']
    crontab = dict(configData.items('CRONTAB'))['crontab']
    environment = dict(configData.items('CONFIG'))['environment']
    timing = dict(configData.items('CONFIG'))['timing']

    tool = ToolTemplate(dataSourctName=programName, logginFileName=loggingFileName)
    db = tool.getDbSourct()
    loggers = tool.getLoggers()
    configDict = tool.getConfigDict()

    # 切换日志级别
    loggers.setLogger('debug')

    exce()