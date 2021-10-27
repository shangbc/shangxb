# Time     2021/10/19 09:15:19
# Auhtor   ShangXb
# ide      PyCharm
# Verion   1.0
# function 物联网批量销户文件生成
from ToolTemplate import ToolTemplate
from MethodTools import getConfigFile
import hashlib
import datetime
import gzip
import os

bxrProdDict = None


# 获取用户映射
def getRegionUser():
    regionUserSql = "select * from cboss.region_users"
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
    bxrProdSql = "select t.* from bxr_prod t"
    bxrProdDict = db .select('cboss', bxrProdSql)
    return bxrProdDict


# 获取实例数据
def insOfferData(bxrProdData):
    try:
        # 清空当前中间表
        db .truncate('cboss', 'truncate table cboss.ins_offer_file_generation')

        bxrProdStr = ',' .join([bxrProd['PROD_ID'] for bxrProd in bxrProdData])

        # 获取地市与用户的映射
        regionUSerCrm = regionUSer['crm']
        for region, sourct in regionUSerCrm .items():
            try:
                soUser = sourct['USER_NAME']
                sourctName = sourct['DBSOURCE_NAME']
                table = '{soUser}.ins_offer_{region}' .format(soUser=soUser, region=str(region))

                insOfferSql = """
                              select t.*, 0 as file_state from {table} t
                              where t.offer_id = any({offerId})
                              """\
                    .format(table=table,offerId=bxrProdStr)
                insOfferData = db .select(sourctName, insOfferSql)
                db .insertMany('cboss', 'cboss.ins_offer_file_generation', insOfferData)
            except Exception as error:
                db .rollback()
                loggers .error(error)
            else:
                db .commit()
    except Exception as error:
        raise error


def fileGenerate():
    thedingDb = tool .getDbSourct()
    fileGenerationSql = "select * from cboss.ins_offer_file_generation t where rownum <= 1000 and t.file_state = 0"
    fileGenerationData = thedingDb .select('cboss', fileGenerationSql)

    offerInstIdDict = [{'offerInstId': offerInstId['OFFER_INST_ID']} for offerInstId in fileGenerationData]
    updataSql = "update cboss.ins_offer_file_generation t set t.file_state = 1 where t.offer_inst_id = :offerInstId"

    thedingDb .executeMany('cboss', updataSql, offerInstIdDict)

    nowTime = datetime .datetime .now()

    fileDescList = []
    fileCount = 0
    for fileGeneration in fileGenerationData:
        try:
            regionId = fileGeneration['REGION_ID']
            userId = fileGeneration['USER_ID']
            custId = fileGeneration['CUST_ID']

            soUser = regionUSer['crm'][int(regionId)]['USER_NAME']
            sourctName = regionUSer['crm'][int(regionId)]['DBSOURCE_NAME']

            insUserTableName = "{soUser}.ins_user_{region}" .format(soUser=soUser, region=str(regionId))
            insUserSql = """
                         select t.* from {tableName} t
                         where  t.create_date < to_date('2020-09-10 00:00:00','yyyy-mm-dd hh24:mi:ss')
                                and t.user_id = {userId}
                         """ .format(tableName=insUserTableName, userId=userId)
            insUserDate = thedingDb .select(sourctName, insUserSql)
            if len(insUserDate) == 0:
                continue

            indivCustomerTableName = "{soUser}.cm_indiv_customer_{region}" .format(soUser=soUser, region=str(regionId))
            indivCustomerSql = "select * from {tableName} a where a.indiv_cust_id = {custId}" .format(tableName=indivCustomerTableName, custId=custId)
            indivCustomerData = thedingDb .select(sourctName, indivCustomerSql)
            if len(indivCustomerData) == 0:
                continue

            indivCustomerData = indivCustomerData[0]
            custName = indivCustomerData['CUST_NAME']
            custName = hashlib .md5(custName.encode(encoding='UTF-8')) .hexdigest() .upper()

            custCode = indivCustomerData['CUST_CERT_CODE']
            custCode = hashlib .md5(custCode.encode(encoding='UTF-8')) .hexdigest() .upper()

            seqSql = "select cboss.Filegenerationseq.nextval from dual"
            seqData = thedingDb .select('cboss', seqSql)
            seqStr = str(seqData[0]['nextval']) .rjust(9, '0')

            seq = '571' + nowTime .strftime('%Y%m%d%S%M%S') + seqStr
            oprTime = nowTime .strftime('%Y%m%d%S%M%S')
            ckeckingDate = nowTime .strftime('%Y%m%d')
            iDValue = insUserDate[0]['bill_id']
            homeProv = '571'
            customerName = custName
            iDCardType = '99'
            iDCardNum = custCode
            status = '02'

            fileDesc = seq + '|' + oprTime + '|' + ckeckingDate + '|' + iDValue + '|' + homeProv + '|' + \
                       customerName + '|' + iDCardType + '|' + iDCardNum + '|' + status + '\r'

            fileDescList .append(fileDesc)
        except Exception as error:
            loggers .error(error)
        else:
            fileCount += 1
    filePath = 'C:\\Users\\Lenovo\\Desktop\\'
    fileName = 'QWCX_batchCancelSync_20211027091521_BOSS571.C001'
    file = open(filePath+fileName, 'wb')
    fileTop = nowTime .strftime('%Y%m%d%S%M%S') + '#571#' + str(fileCount).rjust(10, '0') + '\r'

    file .write(bytes(fileTop, encoding="utf8"))

    for fileDesc in fileDescList:
        file .write(bytes(fileDesc, encoding="utf8"))

    file .flush()
    file .close()

    gzipFileName = fileName + '.gz'

    gzFile = open(filePath+fileName,'rb')
    gz = gzip.GzipFile(filename="", mode="wb", compresslevel=9, fileobj=open(filePath+gzipFileName, 'wb'))
    gz .write(gzFile.read())
    gzFile .close()
    gz .close()

    os .remove(filePath+fileName)

    thedingDb .commit()


def exce():
    # 获取最新的数据
    retrieval = configDict['RETRIEVAL']

    if retrieval .upper() == 'TRUE':
        loggers .debug('获取最新的数据')
        bxrProdData = getBxrProdData()
        insOfferData(bxrProdData)

    fileGenerate()


if __name__ == '__main__':

    configFilepath = 'E:/程序/pyhton/shangxb/configStatic/IotAcquisitionFileConfig'
    # configFilepath = '/app/cbapp/program/config/FtpProcessMonitoring'
    configData = getConfigFile(configFilepath)

    programName = dict(configData .items('PROGRAM'))['programName']
    loggingFileName = dict(configData .items('LOGGING'))['fileName']

    tool = ToolTemplate(dataSourctName=programName, logginFileName=loggingFileName)
    db = tool.getDbSourct()
    loggers = tool.getLoggers()
    configDict = tool .getConfigDict()

    regionUSer = getRegionUser()
    loggers .info(regionUSer)

    exce()


