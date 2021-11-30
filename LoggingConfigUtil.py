# @Time    2021/07/19 11:33:00
# Auhtor   shangxb
# Verion   1.0
# function 日志输出控制，根据配置表中的配置来设置不同的日志格式
import os
import sys
import time
import logging
import traceback

from Decorator import loggerSourceCut
from MethodTools import keyBoolen
from StaticData import getVariable, setVariable
from logging .handlers import TimedRotatingFileHandler


class LoggingConfigUtil:
    # 初始化时设置默认的格式
    def __init__(self, loggingFileName=None):
        try:
            self .loggingFileName = loggingFileName
            self .setLogger()
        except Exception as exceptionError:
            raise exceptionError

    # 切换日志实例
    def setLogger(self, level=None):
        try:
            loggingFileName = self .loggingFileName
            staticConfig = getVariable('staticConfig')
            # 获取当前信息
            os.chdir(sys.path[0])
            localTime = time.strftime("%Y-%m-%d", time.localtime())

            # 获取配置文件中的信息
            loggingConfig = staticConfig.items('LoggingConfig')
            loggingConfig = dict(loggingConfig)
            loggingPath = keyBoolen('logging_path', loggingConfig)
            loggingEncoding = keyBoolen('logging_encoding', loggingConfig)
            loggingLevel = keyBoolen('logging_level', loggingConfig)
            loggingFormatter = keyBoolen('logging_formatter', loggingConfig)

            # 设置当前日志级别
            levelDict = {
                'debug': logging.DEBUG,
                'info': logging.INFO,
                'warning': logging.WARNING,
                'error': logging.ERROR,
                'critical': logging.CRITICAL,
                'fatal': logging.FATAL
            }
            loggingLevel = levelDict[level if level is not None else loggingLevel]

            # 屏幕输出和日志输出设置
            logger = logging.getLogger()
            logger.setLevel(loggingLevel)
            # logfile = loggingPath + ('' if loggingFileName is None else (loggingFileName + '-')) + str(
            #     localTime) + '.log'
            logfile = loggingPath + ('' if loggingFileName is None else (loggingFileName))
            formatter = logging .Formatter(loggingFormatter)

            # 日志输出配置
            # fh = logging.FileHandler(logfile, mode='a', encoding=loggingEncoding)
            # fh.setLevel(loggingLevel)
            # fh.setFormatter(formatter)
            timeHandler = TimedRotatingFileHandler(filename=logfile, when='D')
            timeHandler .setLevel(loggingLevel)
            timeHandler .setFormatter(formatter)

            timeHandler .suffix = "%Y-%m-%d.log"

            # 屏幕输出设置
            ch = logging.StreamHandler()
            ch.setLevel(loggingLevel)
            ch.setFormatter(formatter)

            # 清除以前的设置
            for h in logger.handlers[:]:
                logger.removeHandler(h)

            # 添加新的日志设置
            # logger.addHandler(fh)
            logger .addHandler(timeHandler)
            logger .addHandler(ch)

            self.logger = logger
            self.logs = None
            self.loggerNews = None

            setVariable('loggers', self)
        except Exception as exceptionError:
            raise exceptionError

    @loggerSourceCut
    def debug(self, msg, *args, **kwargs):
        logs = self.logs
        logs .debug(msg, *args, **kwargs)

    # info
    @loggerSourceCut
    def info(self, msg, *args, **kwargs):
        logs = self.logs
        logs .info(msg, *args, **kwargs)

    # WARNING
    @loggerSourceCut
    def warning(self, msg, *args, **kwargs):
        logs = self.logs
        logs .warning(msg, *args, **kwargs)
        logs .warning(traceback.format_exc())

    # error
    @loggerSourceCut
    def error(self, msg, *args, **kwargs):
        logs = self.logs
        logs.error(msg, *args, **kwargs)
        logs.error(traceback.format_exc())

    # CRITICAL
    @loggerSourceCut
    def critical(self, msg, *args, **kwargs):
        logs = self.logs
        logs .critical(msg, *args, **kwargs)

    # FATAL
    @loggerSourceCut
    def fatal(self, msg, *args, **kwargs):
        logs = self.logs
        logs .fatal(msg, *args, **kwargs)
