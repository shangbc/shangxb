# Time     2021/09/27 16::56
# Auhtor   ShangXb
# ide      PyCharm
# Verion   1.0
# function ftp/sftp文件未下载处理
from SFtpUtil import SFtpUtil
from FtpUtil import FtpUtil
from MethodTools import getConfigFile, pathJoin
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from FtpConnectionUtil import FtpConnectionUtil

from ToolTemplate import ToolTemplate
import re
import os
import time
import datetime
import threading
import queue
import multiprocessing
import shutil
import logging

matchingStrDict = {
    '$DATE': '[0-9]{4,14}',
    '$HOMEPROV': '[0-9]{3}',
    '$NNN': '[0-9]{3}'
}


# 获取需要监控的文件
def getFileDownloadData(db):
    sqlDict = {
        'host': ip
    }
    fileDownloadSql = """
                      select * from bossmdy.ftp_download_config t
                      where t.host = :host
                            and t.state = 'U'
                      """
    fileDownloadData = db.select('cboss', fileDownloadSql, sqlDict)
    return fileDownloadData


def getFtpFileDef(db, fileType):
    dictStr = {
        'file_type': fileType
    }
    ftpFileDefSql = """
                      select * from cboss.ftp_file_def t
                      where t.file_type = :file_type
                      """
    ftpFileDefData = db.select('cboss', ftpFileDefSql, dictStr)
    return ftpFileDefData


def getFtpGroupDef(db, groupId):
    dictStr = {
        'group_id': groupId
    }
    ftpGroupDefSql = """
                      select * from cboss.ftp_group_def t
                      where t.group_id = :group_id
                      """
    ftpGroupDefData = db.select('cboss', ftpGroupDefSql, dictStr)
    return ftpGroupDefData


def getFtpFileDownLogs(db, fileType, fileWay, Interval):
    dictStr = {
        'file_type': fileType,
        'Interval': str(Interval),
        'fileWay': fileWay
    }
    ftpFileDefSql = """
                    select * from cboss.ftp_file_interface t
                    where t.file_type = :file_type
                          and t.file_way = :fileWay
                          and t.create_date > sysdate - :Interval/1440
                    """
    ftpFileDefData = db.select('cboss', ftpFileDefSql, dictStr)
    return ftpFileDefData


# 获取操作记录记录
def getFtpFileDownLog(db, fileType, fileName, fileWay):
    dictStr = {
        'file_type': fileType,
        'file_name': fileName,
        'file_way': fileWay
    }
    ftpFileDefSql = """
                        select * from cboss.ftp_file_interface t
                        where t.file_name = :file_name
                              and t.file_way = :file_way
                              and t.file_type = :file_type
                        """
    ftpFileDefData = db.select('cboss', ftpFileDefSql, dictStr)
    return ftpFileDefData


# 本地文件检索
def getLocaFile(locaPath, matchingRe=None, inteval=None):
    fileList = []
    matchingReCompile = re.compile(matchingRe) if matchingRe is not None else None
    for file in os.listdir(locaPath):
        try:
            if matchingReCompile is not None and not matchingReCompile.search(file):
                continue
            fileName = pathJoin(locaPath, file)
            if os .path .isdir(fileName):
                continue
            fileTime = time.ctime(os.path.getmtime(fileName))
            fileTime = datetime.datetime.strptime(fileTime, "%a %b %d %H:%M:%S %Y")
            if inteval is not None and datetime.datetime.now() - datetime.timedelta(minutes=inteval * 2) < fileTime:
                continue
            fileList.append({
                'fileName': file,
                'fileTime': fileTime
            })
        except Exception as error:
            logging .error(error)
    return fileList


# 线程类
class FileExceThreading(threading.Thread):
    def __init__(self, number, tool):
        threading.Thread.__init__(self)
        self .setName('Threading-'+ str(number))
        self.__threadName = 'Threading-{number}'.format(number=number)
        self.__tool = tool

    def run(self):
        global threadState
        loggers = self.__tool.getLoggers()
        db = self.__tool.getDbSourct()
        while threadState:
            threadLock.acquire()
            if not threadQueue.empty():
                fileData = threadQueue.get()
                threadLock.release()
                try:
                    fileType = fileData['FILE_TYPE']
                    taskType = fileData['TASK_TYPE']
                    ignoreLog = fileData['IGNORELOG']
                    fullScan = fileData['FULLSCAN']
                    fileInterval = fileData['FILE_INTERVAL']
                    startInterval = fileData['START_INTERVAL']

                    # 获取当前fileType对应的配置
                    defData = getFtpFileDef(db, fileType)
                    groupId = defData[0]['GROUP_ID']
                    matchingRe = defData[0]['FILE_NAME']
                    localPath = defData[0]['LOCAL_PATH']
                    remotePath = defData[0]['REMOTE_PATH']
                    deleteState = defData[0]['EXT2']
                    hisPath = defData[0]['EXT3']
                    fileWay = int(defData[0]['FILE_WAY'])
                    loggers.info(defData)
                    loggers.info('当前处理的file_type：' + fileType.__str__())
                    loggers.info(matchingRe)

                    # 获取当前文件类型的下载记录
                    # fullScan：U无视当前下载记录，直接检索目录
                    if fullScan == 'E':
                        downLogData = getFtpFileDownLogs(db, fileType, str(fileWay), fileInterval)
                        if len(downLogData) > 0:
                            loggers.info(str(fileType) + '：文件任务执行正常')
                            continue
                    elif fullScan == 'U':
                        loggers.info(str(fileType) + '：文件任务记录，直接检索目标目录')
                    else:
                        raise Exception('fullScan配置错误')

                    # 获取ftp/sftp的配置详情
                    groupData = getFtpGroupDef(db, groupId)
                    ftpIp = groupData[0]['FTPIP']
                    ftpPort = int(groupData[0]['FTPPORT'])
                    ftpUser = groupData[0]['FTPUSER']
                    ftpPasswd = groupData[0]['FTPPASSWD']

                    loggers.info("该文件传输类型为：{}".format(taskType.upper()))

                    # 根据ftp/sftp配置生成对应的实例
                    ftp = None
                    if taskType.upper() == 'FTP':
                        ftp = FtpUtil(ftpIp, ftpPort, ftpUser, ftpPasswd)
                    elif taskType.upper() == 'SFTP':
                        ftp = SFtpUtil(ftpIp, username=ftpUser, password=ftpPasswd, port=ftpPort)
                    else:
                        raise Exception('ftp/sftp配置错误')

                    # 将指定的匹配规则替换为正则表达
                    for oldStr, newStr in matchingStrDict.items():
                        matchingRe = matchingRe.replace(oldStr, newStr)
                    loggers.info('正则表达式为：' + matchingRe)

                    # 获取文件列表
                    fileList = []
                    if fileWay == 1:
                        fileList = FtpConnectionUtil.exceGetSearchDir(ftp, remotePath, matchingRe, int(startInterval))
                    else:
                        fileList = getLocaFile(localPath, matchingRe, int(startInterval))

                    if len(fileList) == 0:
                        loggers.info(str(fileType) + '：当前任务无需要{}的文件，任务执行正常'.format('下载' if fileWay == 1 else '上传'))
                        continue
                    loggers.info('当前获取到的文件数为：' + str(len(fileList)))

                    # 对文件开始处理
                    for file in fileList:
                        try:
                            loggers.debug(file)
                            fileName = file['fileName']
                            fileTime = file['fileTime']
                            loggers.info(fileName)

                            # 判断文件是否符合时间要求
                            if datetime.datetime.now() - datetime.timedelta(minutes=startInterval) <= fileTime:
                                loggers.debug(fileName + "：当前文件无需操作")
                                continue

                            # 判断文件是否有操作记录
                            fileDownLog = getFtpFileDownLog(db, fileType, fileName, str(fileWay))
                            if len(fileDownLog) != 0 and ignoreLog != 'U':
                                loggers.debug(fileName + '已有操作记录')
                                # 判断文件是否需要移his
                                if hisPath is not None and fileWay == 0:
                                    locaFileName = pathJoin(localPath, fileName)
                                    locaHisFileName = pathJoin(hisPath, fileName)
                                    if os.path.exists(locaHisFileName):
                                        os.remove(locaHisFileName)
                                    shutil.move(locaFileName, hisPath)
                                    loggers.debug(fileName + '：移动到his目录下')
                                continue

                            # 文件上传/下载并记录到表里
                            # fileWay：1为下载   0为上传
                            fileSize = None
                            if fileWay == 1:
                                FtpConnectionUtil.exceFileDown(ftp, localPath, remotePath, fileName)
                                fileSize = FtpConnectionUtil.exceGetFilesize(ftp, remotePath, fileName)
                            else:
                                FtpConnectionUtil.exceFileUp(ftp, localPath, remotePath, fileName)
                                locaFileName = pathJoin(localPath, fileName)
                                # 获取上传文件的大小
                                fileSize = os .path .getsize(locaFileName)
                            fileDownLogDict = [{
                                'file_type': fileType,
                                'file_name': fileName,
                                'proc_status': 0,
                                'create_date': datetime.datetime.now(),
                                'file_way': fileWay,
                                'notes': '直接下载' if fileWay == 1 else '直接上传',
                                'ext1': fileSize
                            }]

                            # 判断文件下载记录是否需要落表
                            if len(fileDownLog) == 0:
                                db.insertMany('cboss', 'cboss.ftp_file_interface', fileDownLogDict)

                            # 记录到保障表中  bossmdy.ftp_file_interface_guarantee
                            db.insertMany('cboss', 'bossmdy.ftp_file_interface_guarantee', fileDownLogDict)

                            # 判断文件是否需要删除
                            if fileWay == 1 and deleteState is not None and int(deleteState) == 1:
                                FtpConnectionUtil.exceFileDelete(ftp, remotePath, fileName)

                            # 判断文件是否需要移his
                            if hisPath is not None and fileWay == 0:
                                locaFileName = pathJoin(localPath, fileName)
                                locaHisFileName = pathJoin(hisPath, fileName)
                                if os.path.exists(locaHisFileName):
                                    os.remove(locaHisFileName)
                                shutil.move(locaFileName, hisPath)
                                loggers.debug(fileName + '：移动到his目录下')
                        except Exception as error:
                            loggers.error(error, exc_info=True)
                            db.rollback()
                            try:
                                fileDownLogDict = [{
                                    'file_type': fileType,
                                    'file_name': fileName,
                                    'proc_status': 0,
                                    'create_date': datetime.datetime.now(),
                                    'file_way': fileWay,
                                    'notes': ('直接下载' if fileWay == 1 else '直接上传') + '失败',
                                    'ext1': 0
                                }]
                                db .insertMany('cboss', 'bossmdy.ftp_file_interface_error', fileDownLogDict)
                            except Exception as error:
                                loggers .error(error)
                                db.rollback()
                            else:
                                db.commit()
                        else:
                            db.commit()
                except Exception as error:
                    raise error
                finally:
                    if ftp is not None:
                        FtpConnectionUtil.close(ftp)
            else:
                threadState = False
                threadLock.release()
        db.close()


def exce():
    global threadState, threadQueue, threadLock
    threadState = True
    threadQueue = queue.Queue()

    # 获取工具配置
    tool = ToolTemplate(dataSourctName=programName, logginFileName=loggingFileName)
    db = tool.getDbSourct()
    loggers = tool.getLoggers()

    # 获取需要检索的文件
    fileDataList = getFileDownloadData(db)

    # 构建数据队列
    for fileData in fileDataList:
        threadQueue.put(fileData)

    thredingMax = 10
    thredingList = []

    # 创建线程实例
    loggers.info('创建线程')
    for index in range(thredingMax):
        myThreding = FileExceThreading(index, tool)
        thredingList.append(myThreding)

    # 启动线程
    loggers.info('启动线程')
    for thread in thredingList:
        thread.start()

    #
    for thread in thredingList:
        thread.join()


if __name__ == '__main__':
    # FileDownloadMonitoring
    # configFilepath = 'E:/程序/pyhton/automation/src/configStatic/FileDownloadMonitoring'
    configFilepath = '/app/cbapp/program/config/FileDownloadMonitoring'
    configData = getConfigFile(configFilepath)

    # 获取文件执行的主机ip
    hostDesc = dict(configData.items('HOST'))
    ip = hostDesc['ip']

    threadState = True
    threadQueue = queue.Queue()
    threadLock = threading.Lock()

    crontabDesc = dict(configData.items('CRONTAB'))
    crontab = crontabDesc['crontab']

    loggingDesc = dict(configData.items('LOGGING'))
    loggingFileName = loggingDesc['fileName']

    programDesc = dict(configData.items('PROGRAM'))
    programName = programDesc['programName']

    scheduler = BlockingScheduler()

    scheduler.add_job(exce, CronTrigger.from_crontab(crontab))

    scheduler.start()
    # exce()