# Time     2021/11/18 09::28
# Auhtor
# ide      PyCharm
# Verion   1.0
# function 默认
import sys
import datetime
import pymysql
import uuid
import copy
import cx_Oracle
import pandas as pd
import threading
import queue
import re
from multidict import CIMultiDict
from ProgramConfigurationOnly import Config
from ProgramConfigurationOnly import OraclePolls

programCode = "DefaultData"

user = "bossmdy"


# 获取用户映射
def getRegionUser():
    regionUserSql = "select * from bossmdy.region_users t where t.service_name = 'crm'"
    regionUserDataList = ora .selectSqlExecute('cboss', regionUserSql)

    # regionUserDict = {}
    # for regionUserData in regionUserDataList:
    #     if regionUserData['SERVICE_NAME'] not in regionUserDict:
    #         regionUserDict[regionUserData['SERVICE_NAME']] = {}
    #     regionDict = {
    #         regionUserData['REGION_NAME']: regionUserData
    #     }
    #     regionUserDict[regionUserData['SERVICE_NAME']] .update(regionDict)
    return regionUserDataList


def getDataSourceMapping():
    try:
        global conDict
        getSourceSql = """
                       select t.* from {user}.program_sourct_config t
                       where t.config_state = 'U'
                             and t.python_code = '{programCode}'
                       """ .format(user=user, programCode=programCode)

        sourceDateList = ora .selectSqlExecute('cboss', getSourceSql)

        if len(sourceDateList) == 0:
            raise Exception('program_sourct_config配置错误')

        sourceDict = {}
        for sourceDate in sourceDateList:
            sourceDict .update({
                sourceDate['CONFIG_CODE']: sourceDate['CONFIG_VAULE']
            })

        if 'DATASOURCE' not in sourceDict:
            raise Exception('program_sourct_config配置中没有datasource节点')

        conDict = sourceDict

        sourceNameList = "'" + "','" .join(sourceDict['DATASOURCE'].split(';')) + "'"

        getMapSql = """
                    select t.*,rowid from {user}.Data_source_mapping t
                    where t.map_name in ({sourceNameList})
                          and t.state = 'U'
                    """ .format(user=user, sourceNameList = sourceNameList)
        mapDataList = ora .selectSqlExecute('cboss', getMapSql)

        mapList = []
        for mapData in mapDataList:
            mapDict = {
                'userName': mapData['MAP_NAME'],
                'linkName': mapData['LINK_NAME'],
                'state': '0' if mapData['SOURCE_TYPE'] .upper() == 'ORACLE' else '1'
            }
            mapList .append(mapDict)

        return mapList
    except Exception as error:
        logging .error(error)
        raise Exception('加载新的数据源映射失败')


def getNumDaberta():
    try:
        targetSql = """              
                   select * from bomc.REAL_NAME_USER a
                   where a.bill_id in (select bill_id from bxr_billid2)
                          and a.ext3 = 0
                          and a.create_date  < trunc(sysdate,'yy')
                    """
        targetData = ora.selectSqlExecute('pd', targetSql)

        return targetData
    except Exception as error:
        logging.error(error)


def exce():
    try:
        numDaberta = getNumDaberta()

        if len(numDaberta) == 0:
            logging .info('当前无数据需要上发')
            return

        for data in numDaberta:
            billId = data['BILL_ID']
            insUserData = None
            nextval = None
            regionId = None
            dbsourceNames = None
            userNames = None
            for region in regionUser:
                dbsourceName = region['DBSOURCE_NAME']
                userName = region['USER_NAME']
                regionName = region['REGION_NAME']
                insUserSql = """
                             select * from {userName}.ins_user_{regionName} t
                             where t.bill_id =  '{billId}'
                             """.format(userName=userName, billId=billId, regionName=regionName)
                insData = ora .selectSqlExecute(dbsourceName, insUserSql)

                if len(insData) == 0:
                    continue

                nextvalSql = "select so1.yzwh_sync_busi$seq.nextval from dual"
                nextvalData = ora.selectSqlExecute('crma', nextvalSql)
                insUserData = insData[0]
                nextval = nextvalData[0]['NEXTVAL']
                dbsourceNames = dbsourceName
                userNames = userName
                regionId = regionName
                break
            if insUserData is None:
                continue

            # logging .info(nextval)
            logging .info(regionId)
            insDict = {
                'ORD_ID': regionId .__str__() + nextval .__str__(),
                'BUSINESS_ID': '500000020012',
                'CUST_ID': insUserData['CUST_ID'],
                'BILL_ID': insUserData['BILL_ID'],
                'USER_ID': insUserData['USER_ID'],
                'OLD_CUST_NAME': data['NAME'],
                'OLD_CERT_TYPE': '1',
                'OLD_CERT_CODE': data['id_card'],
                'NEW_CUST_NAME': None,
                'NEW_CERT_TYPE': None,
                'NEW_CERT_CODE': None,
                'CUST_FLAG': '1',
                'OPER_TYPE': '1',
                'CREATE_DATE': datetime .datetime .now(),
                'DONE_DATE': datetime .datetime .now(),
                'DONE_CODE': '20210929',
                'OP_ID': '10058805',
                'ORG_ID': '0',
                'REGION_ID': regionId,
                'REMARKS': '一证五户开户',
                'N_EXT1': '2',
                'N_EXT2': None,
                'N_EXT3': None,
                'V_EXT1': None,
                'V_EXT2': None,
                'V_EXT3': None,
                'D_EXT1': None,
                'D_EXT2': None,
                'D_EXT3': None,
                'P_SEQ': None
            }

            logging .info(insDict)
            tableName = "{userNames}.yzwh_sync_busi_{regionId}".format(userNames=userNames,regionId=regionId)
            ora .batchInsertAll(dbsourceNames, tableName, [insDict])

            ora .dataCommit()
            #break



    except Exception as error:
        logging .error(error)
        ora .dataRollback()
    finally:
        ora .dataClose()


if __name__ == '__main__':
    try:
        # 读取配置EquityRepairAutidNew
        config = Config(programCode)
        staticConfig = config.getStaticConfig()
        logging = config.getLogging()
        ora = config.getDataSource()
        conDict = staticConfig.getKeyDict()

        # 加载新的数据源配置映射
        mapList = getDataSourceMapping()
        nowTime = datetime.datetime.now()
        targetState = True

        # 获取地市数据列表
        regionUser = getRegionUser()

        exce()
    except Exception as error:
        logging .error(error)
    finally:
        if ora is not None:
            ora .dataClose()