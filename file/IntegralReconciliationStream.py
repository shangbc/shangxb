# Time     2022/03/08 11::24
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 默认
import logging
import re
import os
import datetime
import pandas
from ftplib import FTP, _SSLSocket
from ProgramConfigurationOnly import Config, OraclePolls

programCode = 'DefaultData'
user = "bossmdy"


# 继承FTP类，并重写retrbinary、storbinary方法
class StreamFtp(FTP):
    def retrbinary(self, cmd, callback=None, blocksize=8192, rest=None):
        self.voidcmd('TYPE I')
        returnBytearray = bytearray()
        with self.transfercmd(cmd, rest) as conn:
            while 1:
                data = conn.recv(blocksize)
                if not data:
                    break
                # callback(data)
            # shutdown ssl layer
                returnBytearray .extend(data)
            if _SSLSocket is not None and isinstance(conn, _SSLSocket):
                conn.unwrap()
        return self.voidresp(), returnBytearray

    def storbinary(self, cmd, fp, blocksize=8192, callback=None, rest=None):
        self.voidcmd('TYPE I')
        with self.transfercmd(cmd, rest) as conn:
            buf = fp
            conn .sendall(buf)
            if callback:
                callback(buf)
        # shutdown ssl layer
            if _SSLSocket is not None and isinstance(conn, _SSLSocket):
                conn.unwrap()
        return self.voidresp()


def getDataSourceMapping():
    try:
        global conDict
        getSourceSql = """
                       select t.* from {user}.program_sourct_config t
                       where t.config_state = 'U'
                             and t.python_code = '{programCode}'
                       """.format(user=user, programCode=programCode)

        sourceDateList = ora.selectSqlExecute('cboss', getSourceSql)

        if len(sourceDateList) == 0:
            raise Exception('program_sourct_config配置错误')

        sourceDict = {}
        for sourceDate in sourceDateList:
            sourceDict.update({
                sourceDate['CONFIG_CODE']: sourceDate['CONFIG_VAULE']
            })

        if 'DATASOURCE' not in sourceDict:
            raise Exception('program_sourct_config配置中没有datasource节点')

        conDict = sourceDict

        sourceNameList = "'" + "','".join(sourceDict['DATASOURCE'].split(';')) + "'"

        getMapSql = """
                    select t.*,rowid from {user}.Data_source_mapping t
                    where t.map_name in ({sourceNameList})
                          and t.state = 'U'
                    """.format(user=user, sourceNameList=sourceNameList)
        mapDataList = ora.selectSqlExecute('cboss', getMapSql)

        mapList = []
        for mapData in mapDataList:
            mapDict = {
                'userName': mapData['MAP_NAME'],
                'linkName': mapData['LINK_NAME'],
                'state': '0' if mapData['SOURCE_TYPE'].upper() == 'ORACLE' else '1'
            }
            mapList.append(mapDict)

        return mapList
    except Exception as error:
        logging.error(error)
        raise Exception('加载新的数据源映射失败')

# 获取连接
def getConn(host, port, username, password):
    try:
        ftp = StreamFtp()
        # 打开调试级别2，显示详细信息
        # ftp.set_debuglevel(2)
        ftp .connect(host, port, timeout=60)
        ftp .encoding = 'utf-8'
        ftp .set_pasv(True)
        ftp .login(username, password)
        logging .info('连接{host}成功'.format(host=host))
    except Exception as error:
        raise error
    else:
        return ftp


# 获取指定目录下的文件
def getFileName():
    matchingRe = '^571_[0-9]{8}_fail.xls'
    path = '/app/ftpuser/shangxb'
    matchingReCompile = re .compile(matchingRe)
    ftpConn .cwd(path)
    files = []
    fileNames = []
    ftpConn .dir('.', files.append)
    logging.info("当前路径：" + path)
    for file in files:
        ftpConn .voidcmd("NOOP")
        if file .startswith('d'):
            # 是目录就不做任何操作
            pass
        else:
            try:
                fileName = file.split(" ")[-1]

                logging .info("当前检索的文件：" + fileName)
                # print("当前检索的文件：" + path + "/" + fileName)
                # if deleteTime > beginDate:
                #     fileNum += 1
                if not matchingReCompile.search(fileName):
                    continue
                fileDict = {
                    'path': path,
                    'name': fileName
                }
                fileNames .append(fileDict)
            except Exception as error:
                logging .error(error)
    return fileNames


# 将指定文件转移到his
def fileTransfer(fileName):
    try:
        currentPath = '/app/ftpuser/shangxb'
        historicPath = '/app/ftpuser/shangxb/his'

        logging .info(fileName + '：' + currentPath + '  >>>>  ' + historicPath)

        ftpConn .cwd(currentPath)
        try:
            ftpConn .size(fileName)
        except:
            raise Exception('文件不存在')

        currentFile = currentPath + '/' + fileName
        historicPath = historicPath + '/' + fileName

        ftpConn .rename(currentFile, historicPath)
    except Exception as error:
        logging .error('文件转移失败：')
        logging .error(error)
    else:
        logging. info('文件转移成功：')


def getExecFileDate(excelFileName, excelFileBytearray):
    try:
        fileName = excelFileName
        sheetName = '失败详细信息'

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

        excel = pandas .read_excel(bytes(excelFileBytearray), sheetName)
        excel = excel .rename(columns=headDict)

        excel['CREATE_TIME'] = datetime .datetime .now()
        excel['FILE_NAME'] = fileName

        excel = excel.astype(object).where(pandas.notnull(excel), None)

        excel['times'] = pandas .to_datetime(excel['TIMES'])
        excelDictList = excel.to_dict(orient="records")

        return excelDictList
    except Exception as error:
        logging .error(error)


def fileStorage(fileList):
    try:
        for file in fileList:
            try:
                path = file['path']
                name = file['name']

                file = path + '/' + name

                # 获取远端文件流
                fileCodeDone = "RETR " + file
                result, bytearrayData = ftpConn .retrbinary(fileCodeDone)

                excelData = getExecFileDate(name, bytearrayData)





            except Exception as error:
                logging .error(error)
            finally:
                del bytearrayData
    except Exception as error:
        logging .error(error)


def exce():
    fileList = getFileName()
    fileStorage(fileList)


if __name__ == '__main__':
    try:
        # 读取配置EquityRepairAutidNew
        config = Config(programCode)
        staticConfig = config .getStaticConfig()
        logging = config .getLogging()
        ora = config .getDataSource()
        conDict = staticConfig.getKeyDict()

        # 加载新的数据源配置映射
        mapList = getDataSourceMapping()
        ora = OraclePolls(mapList, logging)

        # 获取ftp连接
        ftpConn = getConn('20.26.26.26', 21, 'ftpuser', 'Sec#2021#')

        exce()

    except Exception as error:
        logging.error(error)
    finally:
        if ora is not None:
            ora.dataClose()