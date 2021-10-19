# Time     2021/09/19 17::38
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 默认
# oracle操作工具类
# 继承PoolUtil父类
import cx_Oracle as Oracle
import pandas as pd
from DBUtils.PooledDB import PooledDB

from Decorator import sqlExecutePrint, sqlAnalysis
from ExceptionUtil import DbSourctTypeException
from MethodTools import keyBoolen, manyDataExec
from PoolUtil import PoolUtil


class OraclePoolUtil(PoolUtil):
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

            # 拼接dns
            dns = host + ":" + str(port) + "/" + servceName

            # 创建连接信息
            oraclePool = PooledDB(creator=Oracle, user=user, password=password, dsn=dns, maxconnections=maxConn)
            connection = oraclePool.connection()
            cursor = connection.cursor()

            dbName = keyBoolen('DB_NAME', connDescDict)

            PoolUtil.__init__(self, connection, cursor, dbName)
        except Exception as exceptionError:
            raise exceptionError

    # 批量执行
    @sqlExecutePrint('executemany')
    # @sqlAnalysis('all')
    def insertMany(self, table, *args):
        dataList = args[0]
        df, nameList, columnList = manyDataExec(dataList)
        # 统一用字段名作为预占符
        # state  0  采用Oracle的预占方式  1采用mysql的预占方式
        vauleList = ":" + ",:".join(columnList)
        # 拼接sql
        dbpInsertSql = "insert into " + table + "(" + nameList + ") values(" + vauleList + ")"
        keyList = df

        self .loggers .info(dbpInsertSql)

        self .executeMany(dbpInsertSql, keyList)
