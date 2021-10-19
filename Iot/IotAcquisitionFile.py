# Time     2021/10/19 09:15:19
# Auhtor   ShangXb
# ide      PyCharm
# Verion   1.0
# function 物联网批量销户文件生成
from ToolTemplate import ToolTemplate
from MethodTools import getConfigFile


# 获取指定的产品数据
def getBxrProdData():
    pass


if __name__ == '__main__':

    configFilepath = 'E:/程序/pyhton/automation/src/configStatic/IotAcquisitionFileConfig'
    # configFilepath = '/app/cbapp/program/config/FtpProcessMonitoring'
    configData = getConfigFile(configFilepath)

    programName = dict(configData .items('PROGRAM'))['programName']
    loggingFileName = dict(configData .items('LOGGING'))['fileName']

    # tool = ToolTemplate(dataSourctName=programName, logginFileName=loggingFileName)
    # db = tool.getDbSourct()
    # loggers = tool.getLoggers()