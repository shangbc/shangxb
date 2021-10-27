# Time     2021/09/19 16::03
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 默认
import pandas as pd
import os
import configparser as configparser
from ExceptionUtil import KeyNotExistException


def keyBoolen(keyName, dictData):
    try:
        if keyName not in dictData:
            raise KeyNotExistException(keyName)
    except KeyNotExistException as keyNotExistExceptionError:
        raise keyNotExistExceptionError
    else:
        return dictData[keyName]


def manyDataExec(valuesList):
    df = pd .DataFrame(valuesList)
    dfList = df.columns.values.tolist()
    columnList = []
    for name in dfList:
        name = name.upper()
        if name in columnList:
            continue
        else:
            columnList.append(name)

    # 将所有空值替换为None
    df = df.astype(object).where(pd.notnull(df), None)
    # 重新转成diict
    df = df.to_dict('records')
    nameList = ",".join(columnList)
    return df, nameList, columnList


def getConfigFile(path):
    try:
        # 配置文件的路径
        configurationFile = path

        # 判断文件是否存在
        fileBool = os.path.isfile(configurationFile)
        if not fileBool:
            raise FileNotFoundError("没有找到相关的配置文件：" + configurationFile)

        # 读取配置文件
        config = configparser .RawConfigParser()
        config .optionxform = lambda option: option
        # python 3.0以上
        config .read(configurationFile, encoding="utf-8")
        # python 2.0
        # config.read(configurationFile)
        return config
    except Exception as error:
        raise error


def pathJoin(a, *paths):
    path = os .path .join(a, *paths)
    path = path .replace('\\', '/')
    return path