# Time     2021/12/20 10::50
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 文件巡检
from MethodTools import getConfigFile
from apscheduler.schedulers.background import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from ToolTemplate import ToolTemplate
from croniter import croniter
from croniter import croniter_range
from dateutil .relativedelta import relativedelta
from SFtpUtil import SFtpUtil
from FtpUtil import FtpUtil
from FtpConnectionUtil import FtpConnectionUtil

import threading
import logging
import queue
import datetime
import re
import uuid

# 文件处理队列
dataQueue = queue .Queue()
# 线程锁
threadLock = threading .Lock()

# 时间间隔判断
intervalTimeBoolen = {
    'month': 1,
    'day': 2,
    'hours': 3,
    'minutes': 4
}

# 替换配置表中的指定占位符
matchingStrDict = {
    '$DATE': '[0-9]{4,14}',
    '$HOMEPROV': '[0-9]{3}',
    '$NNN': '[0-9]{3}'
}


# 清除变量所占用的内存数据
def clearVariable(variableName):
    try:
        if variableName is not None:
            del variableName
    except Exception as error:
        loggers .error(error)


# 任务配置
def getFtpFileDef():
    try:
        selectSql = """
                    select * from cboss.ftp_file_def t
                    where t.busi_sts = '1'
                          and file_type in ('202101')
                    """
        ftpFileDef = db .select('cboss', selectSql)
        return ftpFileDef
    except Exception as error:
        loggers .error(error)


# 执行配置
def getFileRetrievalConfig(fileType, threadDb):
    try:
        selectSql = """
                    select * from cboss.file_retrieval_config t
                    where t.file_type = {fileType}
                    """ .format(fileType=fileType)
        fileRetrievalConfig = threadDb .select('cboss', selectSql)
        return fileRetrievalConfig
    except Exception as error:
        loggers .error(error)


# 获取一个执行周期之内
def upperCycleTime(crontabStr):
    try:
        if not croniter .is_valid(crontabStr):
            raise Exception('crontab表达错误：' + crontabStr)

        nowTime = datetime .datetime .now()
        crontab = croniter(crontabStr, nowTime)

        # 获取下一次执行的时间
        nextTime = crontab .get_next(datetime.datetime)

        # 计算执行间隔
        interval = crontab .get_next(datetime.datetime) - nextTime
        intervals = interval * 30

        # 获取上一次执行区间的开始时间
        lastTime = [times for times in croniter_range(nowTime-intervals, nowTime, crontabStr)]
        lastBeginTime = lastTime[-2]
        lastEndTime = lastTime[-1]

        return lastBeginTime, lastEndTime, nextTime, interval

    except Exception as error:
        loggers .error(error)


# 根据间隔判断是否跨年、月和日
def intervalJudge(interval):
    days = interval .days
    seconds = interval .seconds

    # 按照闰年12个月份中天数最小的来计算是否跨月
    if days != 0 and days >= 28:
        return 'month'

    if days != 0:
        return 'day'

    if seconds >= 60 * 60:
        return 'hours'

    return 'minutes'


# 判断开始的时间
def timeRegion(lastBeginTime, lastEndTime, nextTime, crossRegion):
    try:
        beginTime= None
        nowTime = datetime .datetime .now()
        if crossRegion == 'minutes' or crossRegion == 'hours':
            beginTime = lastBeginTime

        if crossRegion == 'day' \
                and lastEndTime >= datetime .datetime(year=nowTime.year, month=nowTime.month, day=nowTime.day)\
                and nextTime <= nowTime + datetime .timedelta(days=1):
            beginTime = datetime .datetime(year=nowTime.year, month=nowTime.month, day=nowTime.day)

        if crossRegion == 'month'\
                and lastEndTime >= datetime .datetime(year=nowTime.year, month=nowTime.month, day=nowTime.day)\
                and nextTime <= nowTime + relativedelta(months=1):
            beginTime = datetime .datetime(year=nowTime.year, month=nowTime.month)

        return beginTime

    except Exception as error:
        loggers .error(error)


def getFtpFileInterface(fileType, beginTime, threadDb):
    try:
        selectSql = """
                    select * from cboss.ftp_file_interface t
                    where t.create_date >= :beginTime
                           and t.file_type = :fileType
                    """
        selectDict = {
            'fileType': fileType,
            'beginTime': beginTime
        }

        selectData = threadDb .select('cboss', selectSql, selectDict)

        return selectData

    except Exception as error:
        raise error


def getFtpGroupDef(groupId, threadDb):
    try:
        selectSql = """
                    select * from cboss.ftp_group_def t
                    where t.group_id = :groupId
                    """
        selecctData = threadDb .select('cboss', selectSql, {'groupId': groupId})
        return selecctData
    except Exception as error:
        loggers .error(error)


def getFileLogs(fileName, fileType, threadDb):
    try:
        selectSql = """
                    select * from cboss.ftp_file_interface t
                    where t.file_name = :fileName
                          and t.file_type = :fileType
                    """
        selectDict = {
            'fileName': fileName,
            'fileType': fileType
        }

        selectData = threadDb .select('cboss', selectSql, selectDict)

        return selectData

    except Exception as error:
        raise error


def fileCheck(fileList, fileType, timeZone, threadDb):
    try:
        fileNotDonwList = []
        fileNotDonwNum = 0
        for file in fileList:
            try:
                fileName = file['fileName']
                fileTime = file['fileTime']

                if timeZone is not None and timeZone != 0:
                    fileTime += datetime .timedelta(hours=timeZone)

                # 超过两小时的文件进行处理，反之跳过
                if fileTime + datetime .timedelta(minutes=2) > datetime .datetime .now():
                    continue

                downInfo = getFileLogs(fileName, fileType, threadDb)

                if len(downInfo) > 0:
                    continue

                fileNotDonwNum += 1
                fileNotDonwList .append(file)
            except Exception as error:
                loggers .error(error)

        return fileNotDonwList, fileNotDonwNum

    except Exception as error:
        loggers .error(error)


# 下载日志
def downLogs(taskType, fileType, groupId, fileName,dateFormat, remotePath, timeZone, threadDb):
    try:
        # 获取ftp信息
        ftpGroupDef = getFtpGroupDef(groupId, threadDb)

        if len(ftpGroupDef) == 0:
            return

        ftpGroupDef = ftpGroupDef[0]
        host = ftpGroupDef['FTPIP']
        port = int(ftpGroupDef['FTPPORT'])
        username = ftpGroupDef['FTPUSER']
        password = ftpGroupDef['FTPPASSWD']

        hostConn = None

        if str(taskType).upper() == 'SFTP':
            hostConn = SFtpUtil(host, port, username, password)
        elif str(taskType).upper() == 'FTP':
            hostConn = FtpUtil(host, port, username, password)
        else:
            raise Exception('taskType配置错误：' + fileType)

        # 将配置表中的fileName替换为正常的正则表达式
        for key, value in matchingStrDict.items():
            fileName = fileName .replace(key, value)
        loggers .debug('正则表达式为：' + fileName)

        fileNameCompare = re .compile(fileName)

        fileList = FtpConnectionUtil.exceGetSearchDir(hostConn, remotePath, fileNameCompare)

        # 排查文件是否有下载记录和是否超过了2个小时没有下载
        fileNotDonwList, fileNotDonwNum = fileCheck(fileList, fileType, timeZone, threadDb)

        return fileNotDonwList, fileNotDonwNum
    except Exception as error:
        raise error


# 获取cboss ftp的配置信息
def getCbossFileConfig(threadDb, host, user):
    try:
        selectSql = """
                    select t.* from cboss_ftp_config t
                    where t.ftp_user = :ftpUser
                           and t.host = :host
                    """
        selectDict = {
            'host': host,
            'ftpUser': user
        }
        selectData = threadDb .select('cboss', selectSql, selectDict)
        return selectData
    except Exception as error:
        raise error


# 获取上传的记录
def upperLogs(host, user, fileType, fileName,dateFormat, remotePath, timeZone, threadDb):
    try:
        # 获取ftp信息
        cbossFileConfig = getCbossFileConfig(threadDb, host, user)

        if len(cbossFileConfig) == 0:
            return

        cbossFileConfig = cbossFileConfig[0]
        host = cbossFileConfig['host']
        port = 21
        username = cbossFileConfig['FTP_USER']
        password = cbossFileConfig['FTP_PASS']

        hostConn = FtpUtil(host, port, username, password)

        # 将配置表中的fileName替换为正常的正则表达式
        for key, value in matchingStrDict.items():
            fileName = fileName .replace(key, value)
        loggers .debug('正则表达式为：' + fileName)

        fileNameCompare = re .compile(fileName)

        fileList = FtpConnectionUtil.exceGetSearchDir(hostConn, remotePath, fileNameCompare)

        # 排查文件是否有下载记录和是否超过了2个小时没有下载
        fileNotDonwList, fileNotDonwNum = fileCheck(fileList, fileType, timeZone, threadDb)

        return fileNotDonwList, fileNotDonwNum
    except Exception as error:
        raise error


# 横表记录保存
def transverseLogSave(threadDb, onlyId, fileType, taskType, fileAssessment, fileBusiness, interval, fileExecNums,
                      fileNums=0, fileNotexecNums=0, errorLevel=0, statusDesc='正常'):
    try:
        transverseLog = {
            'only_id': onlyId,
            'file_type': fileType,
            'task_type': taskType,
            'File_assessment': fileAssessment,
            'file_business': fileBusiness,
            'file_interval': interval,
            'file_nums': fileNums,
            'file_exec_nums': fileExecNums,
            'file_notexec_nums': fileNotexecNums,
            'error_level': errorLevel,
            'status_desc': statusDesc,
            'create_time': datetime.datetime.now()
        }
        threadDb .insertMany('cboss', 'cboss.file_inspect_transverse_log', [transverseLog])
    except Exception as error:
        threadDb .rollback()
        raise error
    else:
        threadDb .commit()


# 纵表记录保存
def portraitLogSave(threadDb, fileNotExecLogList):
    try:
        threadDb .insertMany('cboss', 'cboss.file_inspect_portrait_log', fileNotExecLogList)
    except Exception as error:
        threadDb.rollback()
        raise error
    else:
        threadDb.commit()


def generalProcess(fileFileDef, fileRetrieval, threadDb):
    try:
        loggers .info(fileFileDef)
        loggers .info(fileRetrieval)
        # 文件配置
        groupId = fileFileDef['GROUP_ID']           # 组编号
        fileType = fileFileDef['FILE_TYPE']         # 文件编号
        fileName = fileFileDef['FILE_NAME']         # 文件匹配方式
        fileWay = fileFileDef['FILE_WAY']           # 上传下载标识
        dateFormat = fileFileDef['DATE_FORMAT']     # 日期串格式
        localPath = fileFileDef['LOCAL_PATH']       # 本地路径
        remotePath = fileFileDef['REMOTE_PATH']      # 远端路径

        # 检索配置
        host = fileRetrieval['host'] if 'host' in fileRetrieval else None # 上传文件所在的主机
        ftpUser = fileRetrieval['ftp_user'] if 'ftp_user' in fileRetrieval else None # 上传文件所在的用户
        taskType = fileRetrieval['TASK_TYPE']       # 传输类型
        intervals = fileRetrieval['FILE_INTERVAL']   # crontab
        fileNums = fileRetrieval['FILE_NUMS']       # 文件个数
        timeZone = fileRetrieval['time_zone']       # 时区跨度
        fileAssessment = fileRetrieval['File_assessment']  # 是否考核文件
        fileBusiness = fileRetrieval['file_business'] # 所属业务

        # 获取一个唯一编号
        onlyId = str(uuid .uuid1()) .replace('-', '')

        # 获取上一个周期的时间范围
        lastBeginTime, lastEndTime, nextTime, interval = upperCycleTime(intervals)

        # 判断时间间隔是跨天、跨月、还是跨年
        crossRegion = intervalJudge(interval)

        # 计算获取记录的时间区间
        beginTime = timeRegion(lastBeginTime, lastEndTime, nextTime, crossRegion)

        # 如果获取到的开始时间为空，证明还没有到文件检索指定的时间，直接跳过当前文件
        if beginTime is None:
            loggers .info('目前还未到指定的时间，跳过当前数据')
            return

        # 获取表ftp_file_interface中的记录数
        ftpFileInterface = getFtpFileInterface(fileType, beginTime, threadDb)

        # 判断是否有指定的文件数
        # 在不设置正常文件数的情况下有记录即正常，直接返回
        fileLogNums = len(ftpFileInterface)
        if fileNums is None and fileLogNums > 0:
            transverseLogSave(threadDb, onlyId, fileType, taskType, fileAssessment, fileBusiness, intervals,
                              fileNums=fileNums, fileExecNums=fileLogNums)
            return

        # 记录正常，直接返回
        if fileNums is not None and fileLogNums > 0 and fileLogNums == fileNums:
            transverseLogSave(threadDb, onlyId, fileType, taskType, fileAssessment, fileBusiness, intervals,
                              fileNums=fileNums, fileExecNums=fileLogNums)
            return

        # 文件缺失下载记录，直接检索相关的目录是否存在相关的文件没有下载
        fileErrorList, fileErrorNum = None, None
        if fileWay == '1':
            fileNotDonwList, fileNotDonwNum = downLogs(taskType, fileType, groupId, fileName,dateFormat, remotePath, timeZone, threadDb)
            fileErrorList, fileErrorNum = fileNotDonwList, fileNotDonwNum
        else:
            fileNotDonwList, fileNotDonwNum = upperLogs(host, ftpUser, fileType, fileName, dateFormat, localPath,
                                                       timeZone, threadDb)
            fileErrorList, fileErrorNum = fileNotDonwList, fileNotDonwNum

        if fileErrorNum is not None and fileErrorNum > 0:
            transverseLogSave(threadDb, onlyId, fileType, taskType, fileAssessment, fileBusiness, intervals,
                              fileNums=fileNums, fileExecNums=fileLogNums ,errorLevel=2,
                              fileNotexecNums=fileErrorNum, statusDesc='文件未下载')
        else:
            transverseLogSave(threadDb, onlyId, fileType, taskType, fileAssessment, fileBusiness, intervals,
                              fileNums=fileNums, fileExecNums=fileLogNums, errorLevel=1, statusDesc='文件未生成')

        # 记录信息
        if fileErrorList is not None and len(fileErrorList) > 0:
            fileNotExecLogList = []
            for fileError in fileErrorList:
                fileErrorDict = {
                    'only_id': onlyId,
                    'file_type': fileType,
                    'file_name': fileError['fileName'],
                    'exec_status': '0',
                    'exec_desc': '未上传' if fileWay == '0' else '未下载',
                    'file_create_time': fileError['fileTime'],
                    'create_time': datetime .datetime .now()
                }
                fileNotExecLogList .append(fileErrorDict)
            portraitLogSave(threadDb, fileNotExecLogList)

    except Exception as error:
        loggers .error(error)


# 巡检流程
def fileRetrieval():
    try:
        global tool, dataQueue, threadLooben
        threadDb = tool .getDbSourct()
        fileFileDef = None
        while threadLooben:
            try:
                threadLock .acquire()
                if not dataQueue .empty():
                    fileFileDef = dataQueue .get()
                else:
                    threadLooben = False
                    threadLock .release()
                    continue
                threadLock .release()

                fileType = fileFileDef['file_type']

                # 获取检索的相关配置
                fileRetrievalConfig = getFileRetrievalConfig(fileType, threadDb)

                # 结果
                retrievalDesc = None

                # 在配置表中找不到数据，执行下个数据
                if len(fileRetrievalConfig) == 0:
                    continue

                # 获取巡检配置
                fileRetrievalConfig = fileRetrievalConfig[0]

                # 主体流程
                generalProcess(fileFileDef, fileRetrievalConfig, threadDb)

            except Exception as error:
                loggers .error(error)
    except Exception as error:
        loggers .error(error)
    finally:
        if threadDb is not None:
            threadDb .close()


# 线程类
class FileRetrievalThread(threading.Thread):
    def __init__(self, index):
        threading .Thread .__init__(self)

        # 设置线程名
        self .setName('thread-' + index.__str__())

    def run(self):
        fileRetrieval()


def exce():
    try:
        global tool, db, loggers, configDict, regionUser, productData, level, dataQueue, threadLoob

        # 获取连接配置
        tool = ToolTemplate(dataSourctName=programName, logginFileName=loggingFileName)
        db = tool .getDbSourct()
        loggers = tool .getLoggers()
        configDict = tool .getConfigDict()

        # 切换日志级别
        loggers .setLogger(level)

        # 线程锁状态重置
        threadLooben = True

        loggers .debug('当前设置的最大线程数为：' + threadNunMax .__str__())

        # 获取配置表ftp_file_def中的配置，并存放到队里中
        for ftpFileDef in getFtpFileDef():
            dataQueue .put(ftpFileDef)

        # 创建线程
        threadList = []
        for index in range(threadNunMax):
            fileRetrievalThread = FileRetrievalThread(index)
            threadList .append(fileRetrievalThread)
            loggers .debug('thread-{index}创建成功'.format(index=index))

        # 启动线程
        for fileRetrievalThread in threadList:
            fileRetrievalThread .start()
            loggers.debug('{threadName}启动成功'.format(threadName=fileRetrievalThread.getName()))

        # 等待所有线程结束后继续往下执行
        for fileRetrievalThread in threadList:
            fileRetrievalThread .join()
            loggers.debug('{threadName}结束'.format(threadName=fileRetrievalThread.getName()))

        loggers .info('执行结束')
    except Exception as error:
        loggers. error(error)
    finally:
        # 清除变量
        clearVariable(threadList)

        # 清空队列
        dataQueue .queue .clear()


if __name__ == '__main__':
    configFilepath = 'E:/程序/pyhton/shangxb/configStatic/FileRetrievalConfig'
    # configFilepath = '/app/cbapp/program/config/FileRetrievalConfig'
    configData = getConfigFile(configFilepath)

    programName = dict(configData.items('PROGRAM'))['programName']
    loggingFileName = dict(configData.items('LOGGING'))['fileName']
    crontab = dict(configData.items('CRONTAB'))['crontab']
    environment = dict(configData.items('CONFIG'))['environment']
    timing = dict(configData.items('CONFIG'))['timing']
    level = dict(configData.items('CONFIG'))['level']
    threadNunMax = int(dict(configData.items('CONFIG'))['thread_nun_max'])

    tool, db, loggers, configDict, threadLooben = None, None, None, None, True

    # 判断是否开启执行器
    if timing == 'true':
        scheduler = BlockingScheduler()
        scheduler .add_job(exce, CronTrigger.from_crontab(crontab))
        logging .info('调度器添加成功')
        scheduler .start()
    else:
        exce()
