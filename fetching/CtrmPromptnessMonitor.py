# Time     2022/01/14 14::37
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 一级能开业务数据提取
import queue
import threading
import datetime
import pandas

from ProgramConfigurationOnly import Config
from ProgramConfigurationOnly import OraclePolls

programCode = "CtrmPromptnessMonitor"
threaingMax = 10

promptnessMonitorQueue = queue .Queue()
promptnessMonitorLock = threading .Lock()

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


# 线程类
class TargetTreading(threading .Thread):
    def __init__(self, index):
        threading .Thread .__init__(self)
        self .setName("Threading" + str(index))

    def run(self):
        promptnessMonitorExce()


def getOrigLog(origLog, origRecord, iditemrange, db):
    try:
        selectSql = """
                    select * from (
                         select a.done_code,a.transido,a.processtime,a.activitycode
                         from {origLog} a
                         where a.ext_1 like '{iditemrange}%'
                         and exists (select 1
                                       from {origRecord} c
                                      where c.done_code=a.done_code
                                        and c.resp_code='0000')
                          order by a.processtime desc)
                    where rownum=1
                    """ .format(origLog=origLog, iditemrange=iditemrange, origRecord=origRecord)
        selectData = db .selectSqlExecute('cboss', selectSql)
        return selectData
    except Exception as error:
        raise error


def promptnessMonitorExce():
    global targetState, nowTime
    db = OraclePolls(mapList, logging)
    while targetState:
        try:
            promptnessMonitorLock .acquire()
            if promptnessMonitorQueue .empty():
                targetState = False
                break
            # targetLock .acquire()
            promptnessMonitor = promptnessMonitorQueue .get()
            # targetLock .release()

            # 获取订单编号
            iditemrange = promptnessMonitor['IDITEMRANGE']
            ctrmDoneCode = promptnessMonitor['DONE_CODE']

            # 保存的数据
            upFlag = None
            doneCode = None
            transido = None
            processtime = None
            activitycode = None

            # 将时间类型转为datetime类型
            monitorProcesstime = promptnessMonitor['PROCESSTIME']
            monitorProcesstime = pandas .to_datetime(monitorProcesstime, '%Y-%m-%d %H:M:S')

            # 获取当前processtime的年份、月份,计算出下个月1号的初始时间
            year = monitorProcesstime .year
            month = monitorProcesstime .month
            nextMoth = datetime .datetime(year=year, month=month+1, day=1)

            # 表名
            origLog = 'cboss.orig_busi_log_' + monitorProcesstime .strftime('%Y%m')
            origRecord = 'cboss.orig_busi_record_'
            origRecordHis = 'cboss.orig_busi_record_' + monitorProcesstime .strftime('%Y%m')

            # 判断processtime是否是每月最后一天的最后一个小时
            if nextMoth - monitorProcesstime <= datetime .timedelta(minutes=60):
                origRecord = origRecord + nextMoth .strftime('%Y%m')
            else:
                origRecord = origRecord + monitorProcesstime .strftime('%Y%m')

            # 获取record表数据
            OrigLogData = getOrigLog(origLog, origRecord, iditemrange, db)

            if len(OrigLogData) == 0:
                OrigLogDataHis = getOrigLog(origLog, origRecordHis, iditemrange, db)
                if len(OrigLogDataHis) == 0:
                    upFlag = 3
                else:
                    OrigLogDataHis = OrigLogDataHis[0]
                    doneCode = OrigLogDataHis['DONE_CODE']
                    transido = OrigLogDataHis['TRANSIDO']
                    processtime = OrigLogDataHis['PROCESSTIME']
                    activitycode = OrigLogDataHis['ACTIVITYCODE']

                    upFlag = 1
            else:
                OrigLogData = OrigLogData[0]
                doneCode = OrigLogData['DONE_CODE']
                transido = OrigLogData['TRANSIDO']
                processtime = OrigLogData['PROCESSTIME']
                activitycode = OrigLogData['ACTIVITYCODE']

                upFlag = 1

        except Exception as error:
            upFlag = 4
            updatePromptnessMonitor(db, ctrmDoneCode, upFlag)
            logging.error(error)

        else:
            updatePromptnessMonitor(db, ctrmDoneCode, upFlag, doneCode, transido, processtime, activitycode)
        finally:
            promptnessMonitorLock .release()

    db.dataClose()


def updatePromptnessMonitor(db, ctrmDoneCode, upFlag, doneCode=None, transido=None,
                            processtime=None, activitycode=None):
    try:
        if ctrmDoneCode is None:
            raise Exception('程序出错，ctrmDoneCode为空，无法进行记录')
        updateSql = None

        if upFlag == 1:

            updateSql = """
                        update {user}.t_promptness_monitor a
                        set a.up_done_code = {doneCode},
                            a.up_activitycode = '{activitycode}',
                            a.up_transido = '{transido}',
                            a.up_processtime = to_date('{processtime}','yyyy-mm-dd hh24:mi:ss'),
                            a.up_flag = {upFlag}
                        where a.done_code = {ctrmDoneCode}
                        """ .format(upFlag=upFlag, doneCode=doneCode, transido=transido, activitycode=activitycode,
                                    ctrmDoneCode=ctrmDoneCode, processtime=processtime.strftime('%Y-%m-%d %H:%M:%S'),
                                    user=user)
        else:
            updateSql = """
                      update {user}.t_promptness_monitor a
                      set a.up_flag = {upFlag}
                      where a.done_code = {ctrmDoneCode}
                      """.format(upFlag=upFlag, ctrmDoneCode=ctrmDoneCode, user=user)
        db .sqlExecute('cboss', updateSql)
    except Exception as error:
        db .dataRollback()
        logging .error(error)
    else:
        db .dataCommit()


def getPromptnessMonitor():
    try:
        selectSql = """
                    select * from {user}.t_promptness_monitor a
                    where a.up_flag <> 1
                          and a.activitycode = 'T3000510'
                          and a.processtime > sysdate - 6/24
                    """ .format(user=user)
        selectDate = ora .selectSqlExecute('cboss', selectSql)
        return selectDate
    except Exception as error:
        raise error


def exce():
    try:
        promptnessMonitorData = getPromptnessMonitor()
        romptnessMonitorNums = len(promptnessMonitorData)

        if romptnessMonitorNums == 0:
            logging .info("当前t_promptness_monitor表中无数据，无需处理")
            return

        # 存放到队列当中
        for data in promptnessMonitorData:
            promptnessMonitorQueue .put(data)

        threadNums = romptnessMonitorNums if romptnessMonitorNums <= threaingMax else threaingMax

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
        config = Config('DefaultDataSourceCongfig')
        staticConfig = config.getStaticConfig()
        logging = config.getLogging()
        ora = config.getDataSource()
        conDict = staticConfig.getKeyDict()

        # 加载新的数据源配置映射
        mapList = getDataSourceMapping()
        nowTime = datetime .datetime .now()
        targetState = True

        exce()
    except Exception as error:
        logging .error(error)
    finally:
        if ora is not None:
            ora .dataClose()