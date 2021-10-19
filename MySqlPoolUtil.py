# Time     2021/09/19 17::08
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 默认
# mysql操作工具类
# 继承PoolUtil父类
import pymysql
from DBUtils.PooledDB import PooledDB
from ExceptionUtil import DbSourctTypeException
from MethodTools import keyBoolen
from PoolUtil import PoolUtil


class MySqlPoolUtil(PoolUtil):
    def __init__(self, connDescDict):
        try:

            if connDescDict is None or not isinstance(connDescDict, dict):
                raise DbSourctTypeException("connDescDict")

            host = keyBoolen('DB_HOST', connDescDict)
            port = keyBoolen('DB_PORT', connDescDict)
            user = keyBoolen('DB_USER', connDescDict)
            password = keyBoolen('DB_PASSWORD', connDescDict)
            servceName = keyBoolen('DB_SERVER_NAME', connDescDict)
            maxConn = keyBoolen('DB_MAX_CONN', connDescDict)

            # 创建连接信息
            oraclePool = PooledDB(creator=pymysql, maxconnections=None, mincached=2, maxcached=maxConn, maxshared=3,
                                  blocking=True, maxusage=None, setsession=[], ping=0, host=host, port=int(port),
                                  user=user, password=password, database=servceName, charset='UTF8')
            connection = oraclePool.connection()
            cursor = connection.cursor()

            dbName = keyBoolen('DB_NAME', connDescDict)

            PoolUtil .__init__(self, connection, cursor, dbName)

        except Exception as exceptionError:
            raise exceptionError
