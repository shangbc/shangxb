# Time     2021/09/19 17::38
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 默认
# oracle操作工具类
# 继承PoolUtil父类
from Decorator import sqlExecutePrint, sqlAnalysis
from MethodTools import keyBoolen, manyDataExec
from PoolUtil import PoolUtil


class OraclePoolUtil(PoolUtil):
    def __init__(self, connDescDict, dataSource):
        try:
            connection = connDescDict
            cursor = connection.cursor()

            dbName = dataSource

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
