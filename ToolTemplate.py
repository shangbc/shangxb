# Time     2021/09/23 10::49
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 默认
from DBUtils.PooledDB import PooledDB

from DBConnectionModule import DbSourctConnStaticUtil
from ExceptionUtil import DbSourctTypeException, DataIsNullException, DbSourctConnConfigException, \
    PythonConfigCbossDataException
from LoggingConfigUtil import LoggingConfigUtil
from MethodTools import keyBoolen
from MySqlPoolUtil import MySqlPoolUtil
from OraclePoolUtil import OraclePoolUtil
from StaticConfig import getStaticConfig
from StaticData import init, getVariable

import cx_Oracle as Oracle
import pymysql
import pandas as pd

# 初始化数据源信息，获取配置中所有的数据源信息
def getDbSourctConnStaticConfig():
    try:
        dataSourctNameList = None
        # 将预先配置的连接信息存放到内存中
        # 获取配置存放的数据库连接信息
        staticConfig = getVariable('staticConfig')
        dbConnConfig = staticConfig .items('DBConnectConfig')
        # 转换为字典类型，方便下面方便提取
        dbConnConfig = dict(dbConnConfig)
        host = dbConnConfig['db_host']
        port = dbConnConfig['db_port']
        user = dbConnConfig['db_user']
        password = dbConnConfig['db_password']
        servceName = dbConnConfig['db_name']

        # 拼接dns
        dns = host + ":" + port + "/" + servceName

        # 获取数据库连接
        pool = PooledDB(creator=Oracle, user=user, password=password, dsn=dns)
        connect = pool.connection()
        cursor = connect.cursor()
        sourcSql = """
                   select * from cboss.Db_Sourct_Conn_Config t
                   """

        # 获取全部配置数据
        cursor .execute(sourcSql)
        df = pd. read_sql(sourcSql, connect)
        sourcDataList = df. to_dict(orient="records")

        # 当表中的数据为空时，抛出异常
        if len(sourcDataList) == 0:
            raise DataIsNullException

        # 转换成dict
        sourcDataDict = dict()
        for sourcData in sourcDataList:
            sourcDataDict[sourcData['DB_NAME']] = sourcData

        return sourcDataDict
    except DataIsNullException as dataIsnullexception:
        raise dataIsnullexception("Db_Sourct_Conn_Config表中的数据为空")
    except Exception as error:
        errorContex = "数据源加载失败：" + str(error)
        raise Exception(errorContex)


class ToolTemplate:
    def __init__(self, dataSourctName=None, logginFileName=None):
        init()

        # 全局静态配置变量
        getStaticConfig()

        # 创建日志输出对象
        LoggingConfigUtil(logginFileName)

        try:
            self.__loggers = getVariable('loggers')

            if dataSourctName is not None and not isinstance(dataSourctName, str):
                raise DbSourctTypeException("dataSourctName")

            # 获取数据源配置
            sourctConnConfig = getDbSourctConnStaticConfig()
            self.sourctConnConfig = sourctConnConfig

            # 判断是否启用默认的配置
            dataSourctStr = None
            if dataSourctName is None:
                staticConfig = getVariable('staticConfig')
                dataSourctDefault = dict(staticConfig.items("StaticConfigData"))
                dataSourctStr = dataSourctDefault['default_key']
            else:
                dataSourctStr = dataSourctName

            # 获取配置
            self.getConfigKeyData(dataSourctStr)
            dataSourceList = self.getConfigKeyBoolen('DATASOURCE')
            dataSourceList = dataSourceList.split(';')
            dataSourceUtil = {}
            self.__loggers.debug("开始创建数据源链接：")
            for dataSource in dataSourceList:
                if dataSource not in sourctConnConfig:
                    self.__loggers.error("数据源【{}】在表Db_Sourct_Conn_Config中没有配置！！".format(dataSource))
                    continue
                dataSourceData = self.sourctConnConfig[dataSource]
                dataSourceType = dataSourceData['DB_TYPE'].upper()
                source = None
                sourceType = "ORACLE"

                host = keyBoolen('DB_HOST', dataSourceData)
                port = keyBoolen('DB_PORT', dataSourceData)
                user = keyBoolen('DB_USER', dataSourceData)
                password = keyBoolen('DB_PASSWORD', dataSourceData)
                servceName = keyBoolen('DB_SERVER_NAME', dataSourceData)
                maxConn = keyBoolen('DB_MAX_CONN', dataSourceData)

                if dataSourceType == 'ORACLE':
                    # 拼接dns
                    dns = host + ":" + str(port) + "/" + servceName

                    # 创建连接信息
                    oraclePool = PooledDB(creator=Oracle, user=user, password=password, dsn=dns, maxconnections=maxConn)
                    source = oraclePool
                    sourceType = "ORACLE"
                elif dataSourceType == 'MYSQL':
                    oraclePool = PooledDB(creator=pymysql, maxconnections=None, mincached=2, maxcached=maxConn,
                                          maxshared=3,
                                          blocking=True, maxusage=None, setsession=[], ping=0, host=host,
                                          port=int(port),
                                          user=user, password=password, database=servceName, charset='UTF8')
                    source = oraclePool
                    sourceType = "MYSQL"
                else:
                    errorStr = "DbSourctConnConfigException:数据源【{}】的数据类型错误，不为Oracle或MySql!!".format(dataSource)
                    raise DbSourctConnConfigException(errorStr)

                dataSourceUtil.update({
                    dataSource: {
                        'source': source,
                        'type': sourceType
                    }
                })
                self.__loggers.debug("数据源【{}】添加成功！！".format(dataSource))
            self .__dataSourceUtil = dataSourceUtil
            self.__loggers.debug(dataSourceUtil)
            self.__loggers.debug("结束创建数据源链接")
            self.dataSourceUtil = dataSourceUtil
        except DataIsNullException as dataIsnullexception:
            raise dataIsnullexception
        except Exception as error:
            errorContex = "数据源加载失败：" + str(error)
            raise Exception(errorContex)

    def getDbSourct(self):
        db = DbSourctConnStaticUtil(self .__dataSourceUtil)
        return db

    def getLoggers(self):
        return getVariable('loggers')

    def getStaticConfig(self):
        return getVariable('staticConfig')

    def getConfigKeyData(self, pythonCode):
        configKeySql = """
                       select * from cboss.program_sourct_config t
                       where t.python_code = :pythonCode
                             and t.config_state = 'U'
                       """
        configKeyDict = {
            'pythonCode': pythonCode
        }

        self .__loggers .error(configKeyDict)
        sourcName = self.sourctConnConfig['cboss']
        host = keyBoolen('DB_HOST', sourcName)
        port = keyBoolen('DB_PORT', sourcName)
        user = keyBoolen('DB_USER', sourcName)
        password = keyBoolen('DB_PASSWORD', sourcName)
        servceName = keyBoolen('DB_SERVER_NAME', sourcName)
        maxConn = keyBoolen('DB_MAX_CONN', sourcName)
        dns = host + ":" + str(port) + "/" + servceName
        oraclePool = PooledDB(creator=Oracle, user=user, password=password, dsn=dns, maxconnections=maxConn)

        configKeySourc = OraclePoolUtil(oraclePool .connection(), 'cboss')
        configKeyData = configKeySourc .select(configKeySql, configKeyDict)
        if len(configKeyData) == 0:
            raise PythonConfigCbossDataException(pythonCode)

        # 处理查询出来的数据，将其转换成一一对应的字典类型
        configKeyDict = {

        }
        for configKey in configKeyData:
            key = configKey['CONFIG_CODE']
            values = configKey['CONFIG_VAULE']
            configKeyDict[key] = values
        self .__loggers  .debug("获取到的python_config_cboss配置为：" + configKeyDict .__str__())
        self .configKeyDict = configKeyDict

        # 获取配置数据

    def getConfigKeyBoolen(self, keyName):
        configKeyDict = self.configKeyDict
        if configKeyDict is None or configKeyDict.__len__() == 0:
            raise PythonConfigCbossDataException(keyName)

        if keyName not in configKeyDict:
            raise PythonConfigCbossDataException(keyName)

        return configKeyDict[keyName]

    def getConfigDict(self):
        return self .configKeyDict