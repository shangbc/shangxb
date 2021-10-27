# Time     2021/09/19 17::08
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 默认
# mysql操作工具类
# 继承PoolUtil父类
from MethodTools import keyBoolen
from PoolUtil import PoolUtil


class MySqlPoolUtil(PoolUtil):
    def __init__(self, connDescDict, dataSource):
        try:
            connection = connDescDict
            cursor = connection.cursor()

            dbName = dataSource

            PoolUtil .__init__(self, connection, cursor, dbName)

        except Exception as exceptionError:
            raise exceptionError
