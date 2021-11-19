# Time     2021/09/19 16::49
# Auhtor   ShangXb
# ide      PyCharm
# Verion   1.0
# function 装饰器模块
# 装饰器部分
# ------------------------------------------------------------------------------------------------------
# ----------------------------------装饰器部分---------------------------
import re
import sqlparse


# 配置生成的实例和默认实例切换
from ExceptionUtil import SqlTypeIllegalException
from StaticData import getVariable


def loggerSourceCut(fun):
    def wrap(self, msg, *args, **kwargs):
        logger = None
        if self.loggerNews is None:
            logger = self.logger
        else:
            logger = self.loggerNews
        self.logs = logger
        result = fun(self, msg, *args, **kwargs)
        return result

    return wrap


# DEBUG模式下的SQL打印
def sqlExecutePrint(executeType):

    executeTypeStrDict = {
        'select': "查询语句",
        'insert': "插入语句",
        'update': "更新语句",
        'delete': "表数据删除语句",
        'create': "表创建语句",
        'drop': "表删除语句",
        'truncate': "表清空语句",
        'alter': "表字段更新语句",
        'executemany': "批量执行语句"
    }

    executeTypeStr = executeTypeStrDict[executeType] if executeType in executeTypeStrDict else'sql'

    def sqlExecute(fun):
        def wrap(self, sql, *args):
            try:
                logStr = "2021-08-20 12:00:05,150 - root -  - "
                logStr = "\n" + " " * (len(logStr) + 8)
                sqlStr = '\n' + sql .strip()
                sqlStr = re .sub(r'[\t]+', '\t', sqlStr)
                sqlStr = re .sub(r'[ ]+', ' ', sqlStr)
                sqlStr = sqlStr .replace('\n ', '\n') .replace('\n', '', 1)
                sqlStr = re .sub(r'[\n]', logStr, sqlStr)
                loggers = getVariable('loggers')
                loggers .info(executeTypeStr + "开始执行")
                loggers .debug(sqlStr)
                if args.__len__() != 0:
                    loggers .debug("sql入参为：" + args.__str__())
                result = fun(self, sql, *args)
                loggers .info(executeTypeStr + "执行结束")
                return result
            except Exception as exceptionError:
                loggers .error(executeTypeStr + "执行异常")
                loggers .error(sqlStr)
                raise exceptionError
        return wrap
    return sqlExecute


# sql类型校验
# SqlTypeIllegalException
def sqlAnalysis(sqlType):
    def analysis(fun):
        def wrap(self, sql, *args):
            statements = sqlparse.parse(sql)
            if sqlType.upper() != "TRUNCATE" and sqlType.upper() != "ALL":
                for statement in statements:
                    if statement.get_type() != sqlType.upper():
                        raise SqlTypeIllegalException(sqlType)
            if sqlType.upper() == "TRUNCATE":
                strType = sql .strip(' ') .split(' ', 1)[0]
                if strType.upper() != sqlType.upper():
                    raise SqlTypeIllegalException(sqlType)
            else:
                for statement in statements:
                    sqlTypeList = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'TRUNCATE', 'ALTER']
                    sqlTypeBoolen = False
                    if statement.get_type() in sqlTypeList:
                        sqlTypeBoolen = True
                        break
                    if not sqlTypeBoolen:
                        raise SqlTypeIllegalException("DDL或DML类型")

            result = fun(self, sql, *args)
            return result
        return wrap
    return analysis


# sql格式化
def strFormat(fun):
    def wrap(self, sql, *args):
        string = sql .expandtabs(tabsize=8)
        string = re .sub(r'[\s]+', ' ', string)
        string = string .strip()
        string = string + "\n"
        result = fun(self, string, *args)
        return result
    return wrap


# 数据源切换
def sourceSwitching(method):
    def witching(fun):
        def wrap(self, dbName, sql, *args):
            sourct, sourctType = self .getDataSourceUtilValues(dbName)
            resultStr = "sourct .{}(sql, *args)" .format(method)
            result = eval(resultStr)
            return result
        return wrap
    return witching


# 动态执行方法
def batchOperation(method):
    def operation(fun):
        def wrap(self):
            dataSourceUtil = self.dataSourceUtil
            for key, values in dataSourceUtil.items():
                source = values['source']
                execStr = "source .{}()" .format(method)
                eval(execStr)
            result = fun(self)
            return result
        return wrap
    return operation
