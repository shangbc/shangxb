# Time     2021/09/19 16::07
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 默认
import os
import configparser as configparser
from StaticData import setVariable


def getStaticConfig():
    try:
        # 配置文件的路径
        configurationFile = "E:/程序/pyhton/automation/src/configStatic/DBConnectionModuleConfig"

        # 判断文件是否存在
        fileBool = os.path.isfile(configurationFile)
        if not fileBool:
            raise FileNotFoundError("没有找到相关的配置文件：" + configurationFile)

        # 读取配置文件
        config = configparser.RawConfigParser()
        # python 3.0以上
        config.read(configurationFile, encoding="utf-8")
        # python 2.0
        # config.read(configurationFile)
        setVariable('staticConfig', config)
    except Exception as error:
        raise error
