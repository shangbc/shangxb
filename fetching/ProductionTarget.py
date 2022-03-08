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
threaingMax = 10

targetQueue = queue .Queue()
targetLock = threading .Lock()

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


def getTargetIndexData():
    global targetState, nowTime
    db = OraclePolls(mapList, logging)
    while targetState:
        try:
            targetLock.acquire()
            if targetQueue .empty():
                targetState = False
                break
            # targetLock .acquire()
            target = targetQueue .get()
            # targetLock .release()

            logging.info(target)

            sqlDesc = target['SQL_DESC']
            logging .info(type(sqlDesc))
            if isinstance(sqlDesc, cx_Oracle.LOB):
                sqlDesc = sqlDesc .read()
            lastTime = target['LAST_TIME']
            timeRange = int(target['TIME_RANGE'])
            dataSource = target['DATA_SOURCE']
            lastTime = pd .to_datetime(lastTime, '%Y-%m-%d %H:M:S')

            startTime = lastTime
            endTime = lastTime + datetime .timedelta(minutes=timeRange)

            if endTime > nowTime:
                logging .info("最后执行时间大于当前时间，等待下次执行")
                continue
            else:
                # targetLock .acquire()
                targetNext = copy .deepcopy(target)
                targetNext['LAST_TIME'] = endTime
                targetQueue .put(targetNext)
                targetState = True
                # targetLock .release()

            # logging .info(sqlDesc)
            logging .info("开始时间为：" + startTime .__str__())
            logging.info("结束时间为：" + endTime.__str__())

            logging .info(lastTime)

            endTimeStr = "to_date('{endTime}', 'yyyy-mm-dd hh24:mi:ss')" .format(endTime=endTime .strftime('%Y-%m-%d %H:%M:%S'))
            startTimeStr = "to_date('{startTime}', 'yyyy-mm-dd hh24:mi:ss')".format(startTime=startTime.strftime('%Y-%m-%d %H:%M:%S'))
            sqlDict = {
                'endTime': endTimeStr,
                'startTime': startTimeStr,
                'yyyymm': endTime .strftime('%Y%m')
            }

            sqlDataList = db .selectSqlExecute(dataSource, sqlDesc, sqlDict)

            if len(sqlDataList) == 0:
                continue
            executeDescList = []
            for sqlData in sqlDataList:
                executeDesc = {
                    'identification': target['IDENTIFICATION'],
                    'identification_explain': target['IDENTIFICATION_EXPLAIN'],
                    'threshold': target['THRESHOLD'],
                    'start_time': startTime,
                    'end_time': endTime,
                    'indexs': sqlData['INDEXS'],
                    'create_time': datetime .datetime .now()
                }
                executeDescList .append(executeDesc)
                logging .info(executeDesc)

            db .batchInsertAll('cboss', '{user}.target_log_desc' .format(user=user), executeDescList)


            updateSql = """
                        update  {user}.target_sql_config t
                        set t.last_time = [:last_time]
                        where t.identification = '[:identification]'
                              and t.threshold = '[:threshold]'
                        """ .format(user=user)
            updateDict = {
                'last_time': "to_date('{endTime}', 'yyyy-mm-dd hh24:mi:ss')" .format(endTime=endTime .strftime('%Y-%m-%d %H:%M:%S')),
                'identification': target['IDENTIFICATION'],
                'threshold': target['THRESHOLD']
            }
            db .sqlExecute('cboss', updateSql, updateDict)
        except Exception as error:
            db .dataRollback()
            logging .error(error)

        else:
            db .dataCommit()
        finally:
            targetLock .release()

    db .dataClose()


def getTargetSqlData():
    getTargeSql = """
                  select t.* from {user}.target_sql_config t
                  where t.state = 'U'
                  """ .format(user=user)
    getTargeData = ora .selectSqlExecute('cboss', getTargeSql)
    if len(getTargeData) == 0:
        return None

    return getTargeData


# 线程类
class TargetTreading(threading .Thread):
    def __init__(self, index):
        threading .Thread .__init__(self)
        self .setName("Threading" + str(index))

    def run(self):
        getTargetIndexData()


def exce():
    try:
        targeData = getTargetSqlData()
        if targeData is None:
            logging .info("当前配置target_sql_config表中无数据，无需处理")
            return


        targenums = len(targeData)

        # 存放到队列当中
        for data in targeData:
            targetQueue .put(data)

        threadNums = targenums if targenums <= threaingMax else threaingMax

        threadList = []
        logging .info(threadNums)
        for index in range(threadNums):
            targettreading = TargetTreading(index)
            threadList .append(targettreading)

        for thread in threadList:
            thread .start()

        for thread in threadList:
            thread .join()

    except Exception as error:
        logging .error(error)


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