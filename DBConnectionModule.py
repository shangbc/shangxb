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
                   select * from Db_Sourct_Conn_Config t
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


# 获取数据源信息
class DbSourctConnStaticUtil:
    # 数据源的默认构造方法
    # 不为空的时候可以直接传入数据源名字
    def __init__(self, dataSourctName=None):
        try:
            self .__loggers = getVariable('loggers')

            if dataSourctName is not None and not isinstance(dataSourctName, str):
                raise DbSourctTypeException("dataSourctName")

            # 获取数据源配置
            sourctConnConfig = getDbSourctConnStaticConfig()
            self.sourctConnConfig = sourctConnConfig

            # 判断是否启用默认的配置
            dataSourctStr = None
            if dataSourctName is None:
                staticConfig = getVariable('staticConfig')
                dataSourctDefault = dict(staticConfig .items("StaticConfigData"))
                dataSourctStr = dataSourctDefault['default_key']
            else:
                dataSourctStr = dataSourctName

            # 获取配置
            self .getConfigKeyData(dataSourctStr)
            dataSourceList = self .getConfigKeyBoolen('DATASOURCE')
            dataSourceList = dataSourceList.split(';')
            dataSourceUtil = {}
            self .__loggers  .debug("开始创建数据源链接：")
            for dataSource in dataSourceList:
                if dataSource not in sourctConnConfig:
                    self .__loggers  .error("数据源【{}】在表Db_Sourct_Conn_Config中没有配置！！".format(dataSource))
                    continue
                dataSourceData = self.sourctConnConfig[dataSource]
                dataSourceType = dataSourceData['DB_TYPE'] .upper()
                source = None
                sourceType = "ORACLE"
                if dataSourceType == 'ORACLE':
                    source = OraclePoolUtil(dataSourceData)
                    sourceType = "ORACLE"
                elif dataSourceType == 'MYSQL':
                    source = MySqlPoolUtil(dataSourceData)
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

    # 获取配置数据
    def getConfigKeyBoolen(self, keyName):
        configKeyDict = self.configKeyDict
        if configKeyDict is None or configKeyDict.__len__() == 0:
            raise PythonConfigCbossDataException(keyName)

        if keyName not in configKeyDict:
            raise PythonConfigCbossDataException(keyName)

        return configKeyDict[keyName]

    def getConfigKeyData(self, pythonCode):
        configKeySql = """
                       select * from program_sourct_config t
                       where t.python_code = :pythonCode
                             and t.config_state = 'U'
                       """
        configKeyDict = {
            'pythonCode': pythonCode
        }

        self .__loggers .error(configKeyDict)
        sourcName = self.sourctConnConfig['cboss']
        configKeySourc = OraclePoolUtil(sourcName)
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

    @sourceSwitching("select")
    def select(self, dbName, sql, *args):
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


