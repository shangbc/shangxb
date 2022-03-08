import io
import logging
from ftpLibTest import FTP
# from ftplib import FTP


class FtpUtil():
    def __init__(self, host, port, username, password):
        try:
            ftp = FTP()
            # 打开调试级别2，显示详细信息
            # ftp.set_debuglevel(2)
            ftp.connect(host, port, timeout=60)
            ftp.encoding = 'utf-8'
            ftp.set_pasv(True)
            ftp.login(username, password)
            self .ftp = ftp
            logging .info('连接{host}成功'.format(host=host))
        except Exception as error:
            raise error

            # 文件下载

    def fileDown(self):
        try:
            ftp = self.ftp
            ftp.cwd('/app/ftpuser/shangxb')
            locaFile = '/app/ftpuser/shangxb/571_20220226_fail.xls'
            # mylocaFile = 'C:\\Users\\Lenovo\\Desktop\\新建文件夹\\BOSS5710_busAuthorization_20211230080010_FRATE9980.C001.gz'
            # file = open('C:\\Users\\Lenovo\\Desktop\\OAO_new.docx', 'wb')
            # myBytes = bytes()
            fileCodeDone = "RETR " + locaFile
            a, b = ftp.retrbinary(fileCodeDone)
            # a, b = ftp.retrbinary(fileCodeDone, file .write)
            print(a)
            # print(b)
            return b

            # print(b .decode())

        except Exception as error:
            raise error
        else:
            logging.info(locaFile + "下载成功！")
        finally:
            pass
            # myFile.flush()
            # myFile.close()




ftpUtil = FtpUtil('20.26.26.26', 21, 'ftpuser', 'Sec#2021#')
mydata = ftpUtil .fileDown()

import pandas

sheetName = '失败详细信息'
excel = pandas .read_excel(bytes(mydata), sheetName)

headDict = {
    '发起方标识': 'CHARACTERISTIC',
    '发起方名称': 'INITIATOR_NAME',
    '手机号码': 'MOBILE',
    '归属省': 'PROVINCE',
    '发起方的交易流水号': 'SERIAL',
    '类型': 'INITIATOR_TYPE',
    '省BOSS返回给积分平台的应答码': 'RESPONSE',
    '省BOSS给积分平台的应答描述': 'RESPONSE_DESC',
    '考核原因': 'ERR_CAUSE',
    '积分平台发起的原始报文': 'INTEGRATE_MESSAGE',
    '省BOSS返回给积分平台报文': 'BOSS_MESSAGE',
    '交易失败后的记录时间': 'TIMES'
}

import datetime
excel = excel .rename(columns=headDict)

excel['CREATE_TIME'] = datetime .datetime .now()
excel['FILE_NAME'] = '571_20220226_fail.xls'

excel = excel.astype(object).where(pandas.notnull(excel), None)

excel['times'] = pandas.to_datetime(excel['TIMES'])
excelDictList = excel.to_dict(orient="records")

for e in excelDictList:
    print(e)

del mydata
del excel

#
# file = open('C:\\Users\\Lenovo\\Desktop\\OAO_new.docx', 'wb')
# print(mydata)
# file .write(mydata)
# file .flush()
# file .close()


