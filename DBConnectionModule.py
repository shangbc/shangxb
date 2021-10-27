import traceback
from Decorator import sourceSwitching, batchOperation
import cx_Oracle as Oracle
import pandas as pd
from DBUtils.PooledDB import PooledDB
from ExceptionUtil import KeyNotExistException, DbSourctTypeException, DataIsNullException, \
    DbSourctConnConfigException, PythonConfigCbossDataException
from LoggingConfigUtil import LoggingConfigUtil
from MySqlPoolUtil import MySqlPoolUtil
from OraclePoolUtil import OraclePoolUtil
from StaticConfig import getStaticConfig
from StaticData import getVariable, init


# 获取数据源信息
class DbSourctConnStaticUtil:
    # 数据源的默认构造方法
    # 不为空的时候可以直接传入数据源名字
    def __init__(self, dataSourctName):
        try:
            self .__loggers = getVariable('loggers')
            dataSourceUtil = {}
            for key, vaule in dataSourctName .items():
                dataSource = key
                dataSourceType = vaule['type'] .upper()
                conn = vaule['source']
                source = None
                sourceType = "ORACLE"
                if dataSourceType == 'ORACLE':
                    dataSourceData = conn .connection()
                    source = OraclePoolUtil(dataSourceData, dataSource)
                    sourceType = "ORACLE"
                elif dataSourceType == 'MYSQL':
                    dataSourceData = conn.connection()
                    source = MySqlPoolUtil(dataSourceData, dataSource)
                    sourceType = "MYSQL"
                else:
                    errorStr = "DbSourctConnConfigException:数据源【{}】的数据类型错误，不为Oracle或MySql!!".format(dataSource)
                    raise DbSourctConnConfigException(errorStr)

                dataSourceUtil .update({
                    dataSource: {
                        'source': source,
                        'type': sourceType
                    }
                })

                self .__loggers  .debug("数据源【{}】添加成功！！".format(dataSource))
            self .__loggers  .debug(dataSourceUtil)
            self .__loggers  .debug("结束创建数据源链接")
            self.dataSourceUtil = dataSourceUtil
        except DataIsNullException as dataIsnullexception:
            raise dataIsnullexception
        except Exception as error:
            errorContex = "数据源加载失败：" + str(error)
            raise Exception(errorContex)

    #
    def getDataSourceUtilValues(self, keyName):
        dataSourceUtil = self.dataSourceUtil
        if keyName not in dataSourceUtil:
            raise KeyNotExistException(keyName)

        source = dataSourceUtil[keyName]['source']
        sourceType = dataSourceUtil[keyName]['type']

        return source, sourceType

    @sourceSwitching("select")
    def select(self, dbName, sql, *args):
        pass

    @sourceSwitching("executeMany")
    def executeMany(self, dbName, sql, *args):
        pass

    @sourceSwitching("insert")
    def insert(self, dbName, sql, *args):
        pass

    @sourceSwitching("insertMany")
    def insertMany(self, dbName, sql, *args):
        pass

    @sourceSwitching("update")
    def update(self, dbName, sql, *args):
        pass

    @sourceSwitching("delete")
    def delete(self, dbName, sql, *args):
        pass

    @sourceSwitching("create")
    def create(self, dbName, sql, *args):
        pass

    @sourceSwitching("drop")
    def drop(self, dbName, sql, *args):
        pass

    @sourceSwitching("alter")
    def alter(self, dbName, sql, *args):
        pass

    @sourceSwitching("truncate")
    def truncate(self, dbName, sql, *args):
        pass

    @batchOperation('commit')
    def commit(self):
        self .__loggers  .info("数据源提交成功！")

    @batchOperation('rollback')
    def rollback(self):
        self .__loggers  .info("数据源回滚成功！")

    @batchOperation('close')
    def close(self):
        self .__loggers  .info("数据源关闭成功！")


