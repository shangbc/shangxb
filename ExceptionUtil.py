# Time     2021/09/19 16::24
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 默认


class LoggerException(Exception):

    def __init__(self, errorStr=None):
        if errorStr is None:
            self.__errorStr = "未知错误！！"
        else:
            self.__errorStr = errorStr

    def __str__(self):
        exceptionStr = "LoggerException：".format(self.__errorStr)
        return exceptionStr


class DataIsNullException(Exception):

    def __init__(self, errorStr=None):
        if errorStr is None:
            self.__errorStr = "未知错误！！"
        else:
            self.__errorStr = errorStr

    def __str__(self):
        exceptionStr = "DataIsNullException：{}".format(self.__errorStr)
        return exceptionStr


class DataSourcConfigException(Exception):

    def __init__(self, errorStr=None):
        if errorStr is None:
            self.__errorStr = "未知错误！！"
        else:
            self.__errorStr = errorStr

    def __str__(self):
        exceptionStr = "DataSourcConfigException：".format(self.__errorStr)
        return exceptionStr


# 自定义字典异常类
class KeyNotExistException(Exception):

    def __init__(self, errorStr):
        self.__errorStr = errorStr

    def __str__(self):
        exceptionStr = "KeyNotExistException：{}在配置中不存在！！！".format(self.__errorStr)
        return exceptionStr


# 自定义字典异常类
class DbSourctTypeException(Exception):

    def __init__(self, errorStr=None):
        if errorStr is None:
            self.__errorStr = "DbSourctTypeException:配置异常！"
        else:
            self.__errorStr = errorStr

    def __str__(self):
        exceptionStr = self.__errorStr
        return exceptionStr


# 自定义sql异常类
class SqlTypeIllegalException(Exception):

    def __init__(self, errorStr):
        self.__errorStr = errorStr

    def __str__(self):
        exceptionStr = "SqlTypeIllegalException：sql执行类型错误，不为{}！！！".format(self.__errorStr)
        return exceptionStr


class PythonConfigCbossDataException(Exception):
    def __init__(self, errorStr):
        self.errorStr = errorStr

    def __str__(self):
        exceptionStr = "PythonConfigCbossDataException：{}在配置表program_sourct_config中没有数据！！".format(self.errorStr)
        return exceptionStr


class DbSourctConnConfigException(Exception):
    def __init__(self, errorStr=None):
        if errorStr is None:
            self.__errorStr = "DbSourctConnConfigException:配置异常！"
        else:
            self.__errorStr = errorStr

    def __str__(self):
        exceptionStr = self.__errorStr
        return exceptionStr
