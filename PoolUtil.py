# Time     2021/09/19 16::54
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 默认
import pandas as pd
import cx_Oracle as Oracle
from multidict import CIMultiDict
from Decorator import sqlExecutePrint, strFormat, sqlAnalysis
from StaticData import getVariable


class PoolUtil(object):
    def __init__(self, connection, cursor, dbName, ):
        self.__connection = connection
        self.__cursor = cursor
        self.__dbName = dbName
        self.loggers = getVariable('loggers')

    # 查询函数
    @sqlExecutePrint('select')
    @strFormat
    @sqlAnalysis('select')
    def select(self, sql, *args):
        try:
            conn = self.__connection
            cursor = self.__cursor
            cursor.execute(sql, *args)
            df = None
            if args.__len__() != 0:
                df = pd.read_sql(sql, conn, params=args[0])
            else:
                df = pd.read_sql(sql, conn)
            dataDictList = df.to_dict(orient="records")
            dataList = [CIMultiDict(dataDict) for dataDict in dataDictList]
            selectCount = len(dataList)
            self .loggers .info('查询sql获取的记录数为：' + str(selectCount))
            return dataList
        except Oracle.DatabaseError as databaseerror:
            raise databaseerror
        except Exception as error:
            raise error

    # 插入函数
    @sqlExecutePrint('insert')
    @sqlAnalysis('insert')
    def insert(self, sql, *args):
        try:
            cursor = self.__cursor
            cursor.execute(sql, *args)
            insertCount = cursor.rowcount
            self .loggers .info('插入sql影响的记录数为：' + str(insertCount))
        except Oracle.DatabaseError as databaseerror:
            raise databaseerror
        except Exception as error:
            raise error

    # 更新函数
    @sqlExecutePrint('update')
    @sqlAnalysis('update')
    def update(self, sql, *args):
        try:
            cursor = self.__cursor
            cursor.execute(sql, *args)
            insertCount = cursor.rowcount
            self .loggers .info('更新sql影响的记录数为：' + str(insertCount))
        except Oracle.DatabaseError as databaseerror:
            raise databaseerror
        except Exception as error:
            raise error

    # 删除函数
    @sqlExecutePrint('delete')
    @sqlAnalysis('delete')
    def delete(self, sql, *args):
        try:
            cursor = self.__cursor
            cursor .execute(sql, *args)
            insertCount = cursor .rowcount
            self .loggers  .info('删除sql影响的记录数为：' + str(insertCount))
        except Oracle .DatabaseError as databaseerror:
            raise databaseerror
        except Exception as error:
            raise error

    # 清空函数
    @sqlExecutePrint('truncate')
    @strFormat
    @sqlAnalysis('truncate')
    def truncate(self, sql, *args):
        try:
            cursor = self.__cursor
            cursor .execute(sql, *args)
            insertCount = cursor .rowcount
            self .loggers .info('清空sql影响的记录数为：' + str(insertCount))
        except Oracle .DatabaseError as databaseerror:
            raise databaseerror
        except Exception as error:
            raise error

    # 字段更新函数
    @sqlExecutePrint('alter')
    @sqlAnalysis('alter')
    def alter(self, sql, *args):
        try:
            cursor = self.__cursor
            cursor .execute(sql, *args)
            insertCount = cursor .rowcount
            self .loggers .info('字段更新sql影响的记录数为：' + str(insertCount))
        except Oracle .DatabaseError as databaseerror:
            raise databaseerror
        except Exception as error:
            raise error

    # 创建函数函数
    @sqlExecutePrint('create')
    @sqlAnalysis('create')
    def create(self, sql, *args):
        try:
            cursor = self.__cursor
            cursor .execute(sql, *args)
            insertCount = cursor .rowcount
            self .loggers .info('创建sql影响的记录数为：' + str(insertCount))
        except Oracle .DatabaseError as databaseerror:
            raise databaseerror
        except Exception as error:
            raise error

    # 表删除函数
    @sqlExecutePrint('drop')
    @sqlAnalysis('drop')
    def drop(self, sql, *args):
        try:
            cursor = self.__cursor
            cursor .execute(sql, *args)
            insertCount = cursor .rowcount
            self .loggers .info('删表sql影响的记录数为：' + str(insertCount))
        except Oracle .DatabaseError as databaseerror:
            raise databaseerror
        except Exception as error:
            raise error

    # 执行函数
    @sqlExecutePrint('execute')
    @sqlAnalysis('all')
    def execute(self, sql, *args):
        try:
            cursor = self.__cursor
            cursor .execute(sql, *args)
            insertCount = cursor.rowcount
            self .loggers  .info('execute sql影响的记录数为：' + str(insertCount))
        except Oracle .DatabaseError as databaseerror:
            raise databaseerror
        except Exception as error:
            raise error

    # 批量执行
    @sqlExecutePrint('executemany')
    @sqlAnalysis('all')
    def executeMany(self, sql, *args):
        try:
            cursor = self.__cursor
            cursor .executemany(sql, *args)
            insertCount = cursor.rowcount
            self .loggers  .info('executemany sql影响的记录数为：' + str(insertCount))
        except Oracle .DatabaseError as databaseerror:
            raise databaseerror
        except Exception as error:
            raise error

    # 数据提交
    def commit(self):
        try:
            conn = self.__connection
            conn .commit()
        except Exception as error:
            self .loggers .error("数据源【{}】提交失败！！".format(self.__dbName))
            raise error
        else:
            self .loggers .debug("数据源【{}】提交成功！！".format(self.__dbName))

    # 数据回滚
    def rollback(self):
        try:
            conn = self.__connection
            conn .rollback()
        except Exception as error:
            self .loggers .error("数据源【{}】回滚失败！！".format(self.__dbName))
            raise error
        else:
            self .loggers .debug("数据源【{}】回滚成功！！".format(self.__dbName))

    # 数据关闭
    def close(self):
        try:
            conn = self.__connection
            conn.close()
        except Exception as error:
            self .loggers .error("数据源【{}】关闭失败！！".format(self.__dbName))
            raise error
        else:
            self .loggers .debug("数据源【{}】关闭成功！！".format(self.__dbName))
