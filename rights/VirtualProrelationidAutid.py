# Time     2021/09/14 9:58:30
# Auhtor   ShangXb
# ide      PyCharm
# Verion   1.0
# function VIRTUAL_PRORELATIONID(保障任务生成的虚拟实例id)同步权益中心
import sys
import datetime
import pymysql
import uuid
import pandas as pd
from multidict import CIMultiDict
from ProgramConfigurationOnly import Config
from ProgramConfigurationOnly import OraclePolls

programCode = "VirtualProrelationidAutid"


class PyMysql:
    def __init__(self, user, passwd, database, host, port):
        self .mysqlConnect = None
        try:
            dbConnect = pymysql .connect(user=user,
                                         passwd=passwd,
                                         database=database,
                                         host=host,
                                         port=port)
        except Exception as error:
            logging .error("数据库连接创建失败！")
            logging .error(error)
            raise error
        else:
            self .mysqlConnect = dbConnect
            self .cursor = self .mysqlConnect .cursor(cursor=pymysql.cursors.DictCursor)
            logging .info("数据库创建成功！！")

    def getConnect(self):
        if self .mysqlConnect is not None:
            return self .mysqlConnect
        else:
            raise Exception("当前数据库连接为空，不允许操作！！！")

    def updateSql(self, sql):
        try:
            self .cursor .execute(sql)
        except Exception as error:
            logging .error(error)
            raise error

    def insertSql(self, sql, *args):
        try:
            self .cursor .execute(sql, *args)
        except Exception as error:
            logging .error(error)
            raise error

    def selectSql(self, sql):
        try:
            self .cursor .execute(sql)
            dataVauleList = self .cursor.fetchall()
            selectData = [CIMultiDict(dataVaule) for dataVaule in dataVauleList]
        except Exception as error:
            logging.error(error)
            raise error
        else:
            return selectData

    def commitData(self):
        try:
            self .mysqlConnect .commit()
        except Exception as error:
            logging.error(error)
            raise error

    def rollbackData(self):
        try:
            self .mysqlConnect .rollback()
        except Exception as error:
            logging .error(error)
            raise error

    def closeConnect(self):
        try:
            self .mysqlConnect .close()
        except Exception as error:
            logging .error(error)
            raise error


def getDataSourceMapping():
    try:
        global conDict, db
        getSourceSql = """
                       select t.* from bossmdy.program_sourct_config t
                       where t.config_state = 'U'
                             and t.python_code = 'VirtualProrelationidAutid'
                       """

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
                    select t.*,rowid from bossmdy.Data_source_mapping t
                    where t.map_name in ({sourceNameList})
                          and t.state = 'U'
                    """ .format(sourceNameList = sourceNameList)
        mapDataList = ora .selectSqlExecute('cboss', getMapSql)

        mapList = []
        for mapData in mapDataList:
            mapDict = {
                'userName': mapData['MAP_NAME'],
                'linkName': mapData['LINK_NAME'],
                'state': '0' if mapData['SOURCE_TYPE'] .upper() == 'ORACLE' else '1'
            }
            mapList .append(mapDict)

        db = OraclePolls(mapList, logging)
    except Exception as error:
        raise Exception('加载新的数据源映射失败')


def getOrderInfoSynTotal():
    getSynTotalSql = """
                     select t.orderid,
                            t.suborderid,
                            t.virtual_prorelationid,
                            'U' as autid_state,
                            sysdate as autid_time
                     from cboss.order_info_syn_total t
                     where not exists(
                           select 1 from bossmdy.Virtual_Prorelationid_Autidlog m
                           where t.orderid = m.orderid
                           )
                            and t.virtual_prorelationid is not null
                     """
    synTotalData = db .selectSqlExecute('cboss', getSynTotalSql)

    if len(synTotalData) != 0:
        try:
            db .batchInsertAll('cboss', 'bossmdy.Virtual_Prorelationid_Autidlog', synTotalData)
        except Exception as error:
            logging .error("数据插入Virtual_Prorelationid_Autid_log失败：{error}".format(error=str(error)))
            db .dataRollback()
        else:
            db .dataCommit()

    getAutidLog = """
                  select * from bossmdy.Virtual_Prorelationid_Autidlog t
                  where t.autid_state = 'U'
                  """
    autidLogData = ora .selectSqlExecute('cboss', getAutidLog)

    return autidLogData


# 将数据同步至权益心
def equityRepairSync(suborderId, orderId, virtualProrelationId):
    try:
        desc = ''
        sqlValue = "select * from tf_rc_right_info_case where out_sub_open_order_id = '{}'".format(suborderId)
        dataValue = plMysql.selectSql(sqlValue)
        if len(dataValue) == 0:
            desc = "权益中心tf_rc_right_info_case中无【{}】的数据".format(orderId)
            logging .info(desc)
            return -1,desc
        else:
            desc = "在tf_rc_right_info_case找到【{}】的记录".format(suborderId)

        provinceRelationId = dataValue[0]['PROVINCE_RELATION_ID']
        id = str(uuid.uuid1()) .replace("-", "")

        logging .info(desc)

        sqlValue = "select * from tf_rc_province_relation_audit where OUT_OPEN_ORDER_ID = '{}'".format(orderId)
        dataValue = plMysql.selectSql(sqlValue)
        if len(dataValue) > 0:
            desc = "权益中心tf_rc_province_relation_audit有【{}】的数据".format(orderId)
            logging.info("权益中心tf_rc_province_relation_audit有【{}】的数据".format(orderId))
            logging .info(dataValue)
            logging .info("不做任何处理")
            return 1, desc

        logging.info("生成的uuid：" + str(id))
        logging .info("主订单编号：" + orderId)
        logging .info("子订单编号：" + suborderId)
        logging .info("虚拟策划实例编号：" + virtualProrelationId)
        logging .info("真实策划实例编号：" + provinceRelationId)


        sqlValue = "insert into tf_rc_province_relation_audit(id,out_open_order_id,out_sub_open_order_id,province_relation_old_id,province_relation_new_id,done_date) " \
                   "VALUES(%(id)s, %(orderId)s, %(suborderId)s, %(virtualProrelationId)s, %(provinceRelationId)s, %(doneDate)s)"
        dictData = {
            'id': id,
            'orderId': orderId,
            'suborderId': suborderId,
            'virtualProrelationId': virtualProrelationId,
            'provinceRelationId': provinceRelationId,
            'doneDate': datetime .datetime .now() .strftime("%Y-%m-%d")
        }

        logging .info(sqlValue)
        plMysql.insertSql(sqlValue, dictData)
    except Exception as error:
        logging .error(error)
        plMysql .rollbackData()
    else:
        plMysql .commitData()
        desc = '同步成功'
        return 1, desc



def exce():
    # 获取需要处理的数据
    autidLogDataList = getOrderInfoSynTotal()

    if len(autidLogDataList) == 0:
        logging .info('当前无数据需要处理')
        return

    for autidLogData in autidLogDataList:
        suborderId = autidLogData['SUBORDERID']
        orderId = autidLogData['ORDERID']
        virtualProrelationId = autidLogData['VIRTUAL_PRORELATIONID']
        returnState, desc = equityRepairSync(suborderId, orderId, virtualProrelationId)

        if returnState == 1:
            try:
                AutidLogUpdate = """
                                 update bossmdy.Virtual_Prorelationid_Autidlog t
                                 set t.autid_state = 'E',
                                     t.autid_desc = '{desc}'
                                 where t.orderid = '{orderid}'
                                 """ .format(orderid=orderId,desc=desc)
                db .sqlExecute('cboss', AutidLogUpdate)
            except Exception as error:
                logging .error(error)
                db .dataRollback
            else:
                db .dataCommit()


if __name__ == '__main__':
    try:
        # 读取配置EquityRepairAutidNew
        config = Config(programCode)
        staticConfig = config.getStaticConfig()
        logging = config.getLogging()
        ora = config.getDataSource()
        db = None
        conDict = staticConfig.getKeyDict()

        # 连接权益中心mysql
        plMysql = PyMysql(user="new_sale_rightcenter", passwd="new_sale_rightcenter1q#", database="new_sale_rightcenter_db",
                          host="10.70.22.11", port=8899)

        # 加载新的数据源配置映射
        getDataSourceMapping()

        exce()
    except Exception as error:
        logging .error(error)
    finally:
        if ora is not None:
            ora .dataClose()
        if db is not None:
            db .dataClose()
        if plMysql is not None:
            plMysql .closeConnect()




