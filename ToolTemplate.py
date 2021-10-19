# Time     2021/09/23 10::49
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 默认
from DBConnectionModule import DbSourctConnStaticUtil
from LoggingConfigUtil import LoggingConfigUtil
from StaticConfig import getStaticConfig
from StaticData import init, getVariable


class ToolTemplate:
    def __init__(self, dataSourctName=None, logginFileName=None):
        init()

        # 全局静态配置变量
        getStaticConfig()

        # 创建日志输出对象
        LoggingConfigUtil(logginFileName)

        db = DbSourctConnStaticUtil(dataSourctName)
        self.__db = db

    def getDbSourct(self):
        return self.__db

    def getLoggers(self):
        return getVariable('loggers')

    def getStaticConfig(self):
        return getVariable('staticConfig')
