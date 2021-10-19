# Time     2021/09/19 16::25
# Auhtor   ShangXb
# ide      PyCharm
# Verion   1.0
# function 全局变量保存和获取
staticDict = None


def init():
    global staticDict
    staticDict = {

    }


def setVariable(name, value):
    global staticDict
    newDict = {
        name: value
    }
    staticDict .update(newDict)


def getVariable(name):
    global staticDict
    return staticDict[name]