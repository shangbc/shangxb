# Time     2022/01/05 09::00
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 默认

import pandas
import datetime
import os
import re

from ToolTemplate import ToolTemplate
from MethodTools import getConfigFile
from RequestUtil import getRequestContent
from dateutil .relativedelta import relativedelta
from apscheduler .schedulers .blocking import BlockingScheduler
from apscheduler .triggers .cron import CronTrigger


def getExecFileDate(excelFile):
    try:
        fileName = os .path .basename(excelFile)
        sheetName = '失败详细信息'

        headDict = {
            '发起方标识': 'CHARACTERISTIC',
            '发起方名称': 'INITIATOR_NAME',
            '手机号码': 'MOBILE',
            '归属省': 'PROVINCE',
            '发起方的交易流水号': 'SERIAL',
            '类型': 'INITIATOR_TYPE',
            '省BOSS返回给积分平台的应答码': 'RESPONSE',
            '省BOSS给积分平台的应答描述': 'RESPONSE_DESC',
            '考核原因': 'ERR_CAUSE',
            '积分平台发起的原始报文': 'INTEGRATE_MESSAGE',
            '省BOSS返回给积分平台报文': 'BOSS_MESSAGE',
            '交易失败后的记录时间': 'TIMES'
        }

        excel = pandas .read_excel(excelFile, sheetName)
        excel = excel .rename(columns=headDict)

        excel['CREATE_TIME'] = datetime .datetime .now()
        excel['FILE_NAME'] = fileName

        excel = excel.astype(object).where(pandas.notnull(excel), None)

        excel['times'] = pandas.to_datetime(excel['TIMES'])
        excelDictList = excel.to_dict(orient="records")

        return excelDictList
    except Exception as error:
        loggers .error(error)


def getFileName():
    try:
        compileStr = re .compile("^571_[0-9]{8}_fail.xls")
        filePath = '/app/ftpuser/incoming/upms'

        fileList = []

        # 当前目录的文件
        for file in os .listdir(filePath):
            if not compileStr .search(file):
                continue
            fileDict = {
                'path': filePath,
                'name': file
            }
            fileList .append(fileDict)

        filePath = os .path .join(filePath, 'his')
        # 历史目录的文件
        for file in os.listdir(filePath):
            if not compileStr.search(file):
                continue
            fileDict = {
                'path': filePath,
                'name': file
            }
            fileList .append(fileDict)

        return fileList

    except Exception as error:
        loggers .error(error)


def getFileLog(fileName, db):
    try:
        selectSql = """
                    select * from bossmdy.integral_file_desc t
                    where t.file_name = :fileName
                          and t.status = '解析成功'
                    """
        selectData = db .select('cboss', selectSql, {'fileName':fileName})

        boolen = True if len(selectData) > 0 else False
        return boolen
    except Exception as error:
        raise error


def getIntegralReconDesc(fileName, db):
    try:
        selectSql = """
                    select t.file_name, t.err_cause, count(1) as err_number, '0' as status, sysdate as create_time from bossmdy.integral_recon_desc t
                    where t.file_name = :fileName
                    group by t.file_name, t.err_cause
                    """
        selectData = db .select('cboss', selectSql, {'fileName':fileName})
        return selectData
    except Exception as error:
        raise error


def exce():
    try:
        global tool, loggers, configDict
        tool = ToolTemplate(dataSourctName=programName, logginFileName=loggingFileName)
        loggers = tool.getLoggers()
        configDict = tool.getConfigDict()

        # 切换日志级别
        loggers .setLogger('info')

        db = tool .getDbSourct()

        fileList = getFileName()

        for file in fileList:
            try:
                name = file['name']
                path = file['path']

                errDesc = None

                boolen = getFileLog(name, db)

                if boolen:
                    continue

                # 捕获文件解析及入库的报错
                try:
                    excelDict = getExecFileDate(os .path .join(path, name))

                    if len(excelDict) != 0:
                        db.insertMany('cboss', 'bossmdy.integral_recon_desc', excelDict)

                    descData = getIntegralReconDesc(name, db)

                    if len(descData) > 0:
                        db.insertMany('cboss', 'bossmdy.integral_recon_warning', descData)

                except Exception as error:
                    errDesc = error
                    db .rollback()
                else:
                    db .commit()


                fileLogDict = {
                    'file_name': name,
                    'status': '解析成功' if errDesc is None else '解析失败',
                    'explain_desc': str(errDesc),
                    'times': datetime .datetime .now()
                }

                db .insertMany('cboss', 'bossmdy.integral_file_desc', [fileLogDict])

            except Exception as error:
                db .rollback()
                loggers .error(error)
            else:
                db .commit()

    except Exception as error:
        loggers .error(error)


if __name__ == '__main__':
    configFilepath = 'E:/程序/pyhton/shangxb/configStatic/IntegralReconciliationConfig'
    # configFilepath = '/app/cbapp/program/config/IntegralReconciliationConfig'
    configData = getConfigFile(configFilepath)

    programName = dict(configData.items('PROGRAM'))['programName']
    loggingFileName = dict(configData.items('LOGGING'))['fileName']
    crontab = dict(configData.items('CRONTAB'))['crontab']
    environment = dict(configData.items('CONFIG'))['environment']
    timing = dict(configData.items('CONFIG'))['timing']

    tool, loggers, configDict= None, None, None

    # 判断是否开启执行器
    if timing == 'true':
        scheduler = BlockingScheduler()
        scheduler .add_job(exce, CronTrigger .from_crontab(crontab))
        scheduler .start()
    else:
        exce()