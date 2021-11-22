# Time     2021/10/19 09:15:19
# Auhtor   ShangXb
# ide      PyCharm
# Verion   1.0
# function 物联网批量销户文件生成
from ToolTemplate import ToolTemplate
from MethodTools import getConfigFile
import hashlib
import datetime
import time
import gzip
import os
import threading
import queue

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

bxrProdDict = None

dataQueue = queue .Queue()
writeQueu = queue .Queue()


# 获取用户映射
def getRegionUser():
    regionUserSql = "select * from bossmdy.region_users"
    regionUserDataList = db .select('cboss', regionUserSql)

    regionUserDict = {}
    for regionUserData in regionUserDataList:
        if regionUserData['SERVICE_NAME'] not in regionUserDict:
            regionUserDict[regionUserData['SERVICE_NAME']] = {}
        regionDict = {
            regionUserData['REGION_NAME']: regionUserData
        }
        regionUserDict[regionUserData['SERVICE_NAME']] .update(regionDict)
    return regionUserDict


# 获取指定的产品数据
def getBxrProdData():
    global bxrProdDict
    bxrProdSql = "select t.* from bossmdy.bxr_prod t"
    bxrProdDict = db .select('cboss', bxrProdSql)
    return bxrProdDict


# 获取实例数据
def insOfferData(bxrProdData):
    try:
        # 清空当前中间表
        # db .truncate('cboss', 'truncate table bossmdy.ins_offer_file_generation')

        bxrProdStr = ',' .join([bxrProd['PROD_ID'] for bxrProd in bxrProdData])

        # 获取地市与用户的映射
        regionUSerCrm = regionUSer['crm']
        for region, sourct in regionUSerCrm .items():
            try:
                soUser = sourct['USER_NAME']
                sourctName = sourct['DBSOURCE_NAME']
                table = '{soUser}.ins_offer_{region}' .format(soUser=soUser, region=str(region))
                selOfferSql = """
                              select count(*) as nums from {table} t
                              where t.offer_id in ({offerId})
                              """\
                    .format(table=table,offerId=bxrProdStr)
                selOfferDate = db .select(sourctName, selOfferSql)
                loggers .info(selOfferDate)
                numsLen = int(selOfferDate[0]['NUMS'])

                if numsLen == 0:
                    continue

                maxLen = 300000
                startLen = 0
                endLen = 0
                while endLen < numsLen:
                    endLen += maxLen
                    if endLen > numsLen:
                        endLen = numsLen

                    insOfferSql = """
                                  select a.offer_inst_id,a.cust_id,a.user_id,a.region_id, a.file_state from 
                                      (select t.offer_inst_id,t.cust_id,t.user_id,t.region_id, 0 as file_state , rownum as nums from {table} t
                                      where t.offer_id in ({offerId})) a
                                  where a.nums <= {endLen}
                                        and a.nums > {startLen}
                                  """\
                        .format(table=table, offerId=bxrProdStr, endLen=str(endLen), startLen=str(startLen))
                    insOfferData = db .select(sourctName, insOfferSql)
                    db .insertMany('cboss', 'bossmdy.ins_offer_file_generation', insOfferData)
                    startLen += maxLen
                    del insOfferData
            except Exception as error:
                db .rollback()
                loggers .error(error)
            else:
                db .commit()
    except Exception as error:
        raise error


def getAcquisitionData():
    global productionState, theadingState, threadingLock
    fileMaxCounts = fileConfig['maxCounts']
    thedingDb = tool.getDbSourct()

    try:
        threadingLock .acquire()
        fileGenerationSql = "select * from bossmdy.ins_offer_file_generation t where rownum <= {fileMaxCounts} and t.file_state = 0".format(
            fileMaxCounts=str(fileMaxCounts))
        fileGenerationData = thedingDb.select('cboss', fileGenerationSql)

        if len(fileGenerationData) == 0:
            productionState = False
            theadingState = False
            return

        offerInstIdDict = [{'offerInstId': offerInstId['OFFER_INST_ID']} for offerInstId in fileGenerationData]
        updataSql = "update bossmdy.ins_offer_file_generation t set t.file_state = 1 where t.offer_inst_id = :offerInstId"

        thedingDb.executeMany('cboss', updataSql, offerInstIdDict)

        for data in fileGenerationData:
            dataQueue .put(data)

    except Exception as error:
        loggers.error(error)
        thedingDb.rollback()
    else:
        thedingDb.commit()
    finally:
        theadingState = False
        thedingDb .close()
        threadingLock.release()


# 校验数据
def fileGenerate():
    global serial, theadingState, nowTime, consumptionState, productionState, threadingLock, createTimeStr
    thedingDb = tool .getDbSourct()

    while consumptionState:
        if not theadingState:
            try:
                fileGeneration = None
                threadingLock.acquire()
                if dataQueue.empty() and not productionState:
                    consumptionState = False
                    threadingLock.release()
                    continue
                if not dataQueue.empty():
                    fileGeneration = dataQueue .get()
                else:
                    loggers .info('队列数据为空，切换生产者')
                    theadingState = True
                    threadingLock.release()
                    continue
                threadingLock.release()
                regionId = fileGeneration['REGION_ID']
                userId = fileGeneration['USER_ID']

                soUser = regionUSer['crm'][int(regionId)]['USER_NAME']
                sourctName = regionUSer['crm'][int(regionId)]['DBSOURCE_NAME']

                insUserTableName = "{soUser}.ins_user_{region}".format(soUser=soUser, region=str(regionId))
                insUserSql = """
                             select t.* from {tableName} t
                             where  t.create_date < to_date('2020-09-10 00:00:00','yyyy-mm-dd hh24:mi:ss')
                                    and t.user_id = {userId}
                             """.format(tableName=insUserTableName, userId=userId)
                insUserDate = thedingDb.select(sourctName, insUserSql)
                if len(insUserDate) == 0:
                    continue

                indivCustomerTableName = "{soUser}.insx_owner_info_{region}".format(soUser=soUser, region=str(regionId))
                indivCustomerSql = "select * from {tableName} a where a.user_id = {userId}".format(
                    tableName=indivCustomerTableName, userId=userId)
                indivCustomerData = thedingDb.select(sourctName, indivCustomerSql)
                if len(indivCustomerData) == 0:
                    continue

                indivCustomerData = indivCustomerData[0]
                custName = indivCustomerData['OWNER_PERSON']
                custName = hashlib.md5(custName.encode(encoding='UTF-8')).hexdigest().upper()

                custCode = indivCustomerData['OWNER_CERT_CODE']
                custCode = hashlib.md5(custCode.encode(encoding='UTF-8')).hexdigest().upper()

                seqSql = "select bossmdy.Filegenerationseq.nextval from dual"
                seqData = thedingDb.select('cboss', seqSql)
                seqStr = str(seqData[0]['nextval']).rjust(9, '0')

                seq = '571' + createTimeStr + seqStr
                oprTime = nowTime.strftime('%Y%m%d%H%M%S')
                ckeckingDate = nowTime.strftime('%Y%m%d')
                iDValue = insUserDate[0]['bill_id']
                homeProv = '571'
                customerName = custName
                iDCardType = '00'
                iDCardNum = custCode
                status = '02'

                fileDesc = seq + '|' + oprTime + '|' + ckeckingDate + '|' + iDValue + '|' + homeProv + '|' + \
                           customerName + '|' + iDCardType + '|' + iDCardNum + '|' + status + '\r\n'

                writeQueu.put(fileDesc)
                loggers .info('当前writeQueu的数据量为:' + str(writeQueu .qsize()))
                if writeQueu .qsize() >= 30000:
                    threadingLock .acquire()
                    writeFile()
                    threadingLock .release()
            except Exception as error:
                loggers.error(error)
        else:
            loggers.info('生产者还在获取数据，休眠10s')
            time.sleep(10)

    thedingDb .commit()
    thedingDb .close()


def writeFile():
    global serial
    # 需要加锁
    filePath = fileConfig['filePath']
    # fileName = 'QWCX_batchCancelSync_20211027091521_BOSS571.C001'
    fileName = fileConfig['fileName']
    fileDescList = []

    for index in range(30000):
        if not writeQueu .empty():
            fileDescList .append(writeQueu .get())
        else:
            break

    if len(fileDescList) == 0:
        return

    fileName = fileName .format(createTime=createTimeStr, serial=str(serial).rjust(3,'0'))
    fileTop = createTimeStr + '#' + str(serial).rjust(3, '0') + '#' + str(len(fileDescList)).rjust(10,'0') + '\r\n'
    serial += 1
    file = open(filePath+fileName, 'wb')

    file .write(bytes(fileTop, encoding="utf8"))
    fileDescList[-1] = fileDescList[-1] .replace('\r\n', '')
    for fileDesc in fileDescList:
        file .write(bytes(fileDesc, encoding="utf8"))

    file .flush()
    file .close()

    loggers .info('文件生成成功：'+ fileName)


# 生产者
class ProductionTheading(threading.Thread):
    def __init__(self, index):
        threading .Thread .__init__(self)
        self .setName("Threading" + str(index))

    def run(self):
        global productionState, theadingState
        while productionState:
            if theadingState:
                getAcquisitionData()
            else:
                loggers .info('消费者还在处理数据，休眠60s')
                time .sleep(60)


# 消费者
class ConsumptionTheading(threading.Thread):
    def __init__(self, index):
        threading .Thread .__init__(self)
        self .setName("Threading" + str(index))

    def run(self):
        fileGenerate()


def exce():
    # 获取最新的数据
    global serial, createTimeStr, theadingState, nowTime
    retrieval = configDict['RETRIEVAL']

    if retrieval .upper() == 'TRUE':
        loggers .debug('获取最新的数据')
        bxrProdData = getBxrProdData()
        insOfferData(bxrProdData)

    createTimeStr = datetime .datetime .now() .strftime('%Y%m%d%H%M%S')
    nowTime = datetime.datetime.now()
    serial = 1
    theadingState = True

    # fileGenerate()
    productionThreadingList = []
    consumptionThreadingList = []
    for index in range(1):
        productionhreading= ProductionTheading(index)
        productionThreadingList .append(productionhreading)
    for index in range(1, 7):
        consumptionThreading = ConsumptionTheading(index)
        consumptionThreadingList .append(consumptionThreading)

    for productionhreading in productionThreadingList:
        productionhreading .start()
    for consumptionThreading in consumptionThreadingList:
        consumptionThreading.start()

    for productionhreading in productionThreadingList:
        productionhreading .join()
    for consumptionThreading in consumptionThreadingList:
        consumptionThreading .join()

    del productionThreadingList,consumptionThreadingList

    loggers .info(writeQueu .qsize())
    writeFile()


if __name__ == '__main__':

    # configFilepath = '/home/shangbc/PycharmProjects/pythonProject1/configStatic/IotAcquisitionFileConfig'
    configFilepath = '/app/cbapp/program/config/IotAcquisitionFileConfig'
    # configFilepath = 'E:/程序/pyhton/shangxb/configStatic/IotAcquisitionFileConfig'
    configData = getConfigFile(configFilepath)

    programName = dict(configData .items('PROGRAM'))['programName']
    loggingFileName = dict(configData .items('LOGGING'))['fileName']
    fileConfig = dict(configData .items('File'))
    crontab = dict(configData.items('CRONTAB'))['crontab']

    serial = 1
    createTimeStr = None
    nowTime = None
    theadingState = True
    productionState = True
    consumptionState = True
    threadingLock = threading.Lock()

    tool = ToolTemplate(dataSourctName=programName, logginFileName=loggingFileName)
    db = tool.getDbSourct()
    loggers = tool.getLoggers()
    configDict = tool .getConfigDict()

    regionUSer = getRegionUser()
    loggers .info(regionUSer)

    scheduler = BlockingScheduler()

    scheduler.add_job(exce, CronTrigger.from_crontab(crontab))

    # scheduler .start()
    exce()


