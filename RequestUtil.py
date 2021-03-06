# Time     2021/11/09 17::12
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 默认
import requests
import json


# 获取连接
def getRequestContent(requestUrl, requestData, headers):
    try:
        print(requests .status_codes .codes)
        # 设置重连次数
        requests.adapters.DEFAULT_RETRIES = 5
        #设置关闭多余的连接
        s = requests.session()
        s.keep_alive = False
        # 设置连接信息
        requestsContent = requests .post(url=requestUrl, data=json .dumps(requestData), headers=headers, stream=False)
        # 提取接口返回的数据
        requestsTextStr = requestsContent .text
        # 返回的是一个字符串的数据,转换为字典类型
        requestsTextJson = json .loads(requestsTextStr)
        return requestsTextJson
    except Exception as error:
        raise error
    finally:
        requestsContent .close()
