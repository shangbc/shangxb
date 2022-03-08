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
from multidict import CIMultiDict
from ProgramConfigurationOnly import Config
from ProgramConfigurationOnly import OraclePolls

programCode = "ProductionTarget"

user = "bossmdy"

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



def exce():
    try:
        targetSql = """
                    select * from  bossmdy.target_log_desc t
                    where t.end_time <= trunc(sysdate)
                    """
        targetData = ora .selectSqlExecute('cboss', targetSql)

        if len(targetData) == 0:
            logging .info('当前没有数据需要处理')

        ora .batchInsertAll('cboss', 'bossmdy.target_log_desc_his', targetData)

        targetdel = """
                    delete from  bossmdy.target_log_desc t
                    where t.end_time <= trunc(sysdate)
                    """

        ora .sqlExecute('cboss', targetdel)

    except Exception as error:
        ora .dataRollback()
        logging .error(error)
    else:
        ora .dataCommit()
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

        exce()
    except Exception as error:
        logging .error(error)
    finally:
        if ora is not None:
            ora .dataClose()