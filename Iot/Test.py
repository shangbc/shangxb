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


def getOrderData():
    try:
        targetSql = """              
                    select distinct
                    a.mobile_no,
                    substr(a.input_param_1,
                            INSTR(a.input_param_1, 'orderId', 1, 1) + 10, 
                            INSTR(a.input_param_1, 'orderIdType', 1, 1) -INSTR(a.input_param_1, 'orderId', 1, 1) - 13) order_id,
                            substr(a.input_param_1,
                            INSTR(a.input_param_1, 'goodsId', 1, 1) + 10, 
                            INSTR(a.input_param_1, 'goodsName', 1, 1) -INSTR(a.input_param_1, 'goodsId', 1, 1) - 13) goods_id,
                            substr(a.input_param_1,
                            INSTR(a.input_param_1, 'goodsName', 1, 1) + 12, 
                            INSTR(a.input_param_1, 'price', 1, 1) -INSTR(a.input_param_1, 'goodsName', 1, 1) - 15) goods_name,
                             substr(a.input_param_1,
                            INSTR(a.input_param_1, 'subOrderId', 1, 1) + 13, 
                            INSTR(a.input_param_1, 'totalFee', 1, 1) -INSTR(a.input_param_1, 'subOrderId', 1, 1) - 16) order_sub_id
                             from   party.cop_business_log_202201 a where a.busi_code='CIP00155' 
                    and a.create_time>=sysdate - 1
                    and (input_param_1 like '%returnType":"6"%' or input_param_1 like '%returnType":"4"%' )
                    and a.output_param_1  like '%{"respCode":"20102","respDesc%'
                    """
        targetData = ora.selectSqlExecute('party', targetSql)

        return targetData
    except Exception as error:
        logging.error(error)


def insertData(orderData):
    try:
        ora .sqlExecute('cboss', 'delete from bossmdy.EQUITY_UNSUB_TMP')
        ora .batchInsertAll('cboss', 'bossmdy.EQUITY_UNSUB_TMP', orderData)

        selectSql = """
                    select a.*, 0 as status from bossmdy.EQUITY_UNSUB_TMP a
                    where not exists (
                          select 1 from bossmdy.EQUITY_UNSUB_EXCE b
                          where a.order_id = b.order_id
                    )
                    """

        selectData = ora .selectSqlExecute('cboss', selectSql)
        if len(selectData) == 0:
            return
        ora.batchInsertAll('cboss', 'bossmdy.EQUITY_UNSUB_EXCE', selectData)
    except Exception as error:
        logging .error(error)
        ora .dataRollback()
    else:
        ora .dataCommit()


def getData():
    try:
        selectSql = """
                    select * from bossmdy.EQUITY_UNSUB_EXCE t
                    where t.status = 0
                    """
        selectDate = ora .selectSqlExecute('cboss', selectSql)
        return selectDate
    except Exception as error:
        raise error


def exce():
    try:
        orderData = getOrderData()

        if len(orderData) == 0:
            logging .info('当前没有数据需要处理')
            return

        insertData(orderData)

        exceData = getData()

        compileStr = re .compile('SC.*')

        for data in exceData:
            try:
                phone = data['mobile_no']
                orderid = data['order_id']
                suborderid = data['order_sub_id']
                goodsid = data['goods_id']
                goodsname = data['goods_name']
                orderType = True if compileStr .search(orderid) else False

                dictStr = {
                    'phone': phone,
                    'orderid': orderid,
                    'suborderid': suborderid,
                    'goodsid': goodsid,
                    'goodsname': goodsname,
                    'orderidtype': '2' if orderType else '1',
                    'payment': 0,
                    'refundamount': 0,
                    'price': 0,
                    'returnquantity': 1,
                    'returntype': '4' if orderType else '6',
                    'totalfee': 0,
                    'status': 'U',
                    'upper_nums': 0
                }

                ora .batchInsertAll('cboss', 'bossmdy.unsubscribe_upper_task', [dictStr])

                updateSql = """
                            update bossmdy.EQUITY_UNSUB_EXCE t
                            set t.status = 1
                            where t.order_id = '{orderId}'
                            """ .format(orderId=orderid)

                ora .sqlExecute('cboss', updateSql)
            except Exception as error:
                ora .dataRollback()
                logging .error(error)
            else:
                ora .dataCommit()




    except Exception as error:
        logging .error(error)
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