# Time     2022/02/22 09::23
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 默认
from ftplib import FTP
import logging
import re
from ProgramConfigurationOnly import Config
import datetime

# ftp配置
ftpIp = '10.78.220.32'
ftpUser = 'ftpuser'
ftpPass = 'ftpuser'


# 连接ftp
def getFtpConn():
    try:
        ftpConn = FTP()
        # 打开调试级别2，显示详细信息
        # ftp.set_debuglevel(2)
        ftpConn .connect(ftpIp, 21, timeout=60)
        ftpConn .encoding = 'utf-8'
        ftpConn .set_pasv(True)
        ftpConn .login(ftpUser, ftpPass)
        logging .info('连接{host}成功'.format(host=ftpIp))
        return ftpConn
    except Exception as error:
        raise error


# 返回指定目录的文件
def getFileName():
    try:
        filePath = '/app/ftpuser/outgoing/OSP/pic/571/tmp'
        fileNameComp = 'BOSS*'

        matchingReCompile = re.compile(fileNameComp)

        # 跳转目录
        ftp.cwd(filePath)

        files = []
        fileNames = []
        ftp.dir('.', files.append)
        for file in files:
            ftp.voidcmd("NOOP")
            if file.startswith('d'):
                # 是目录就不做任何操作
                pass
            else:
                try:
                    fileName = file.split(" ")[-1]

                    if matchingReCompile is not None and not matchingReCompile.search(fileName):
                        continue
                    fileDict = {
                        'fileName': fileName,
                    }
                    fileNames .append(fileDict)
                except Exception as error:
                    logging .error(error)
    except Exception as error:
       logging .error(error)
    finally:
        logging .info("目录：" + filePath + "检索完成！")
        return fileNames


# 修改记录表状态，使下载任务重新解析上移
def setStaus():
    try:
        # 判断是否有文件
        if fileNames is None or len(fileNames) == 0:
            logging .info('当前无需处理')
            return

        deleteSql = "delete from bossmdy.ftp_file_name"
        ora .sqlExecute('cboss', deleteSql)
        ora .batchInsertAll('cboss', 'bossmdy.ftp_file_name', fileNames)
        ora .dataCommit()

        for fileName in fileNames:
            try:
                name = fileName['fileName']
                # 判断文件处理记录是否存在
                selectSql = "select *  from cboss.ftp_file_interface t where t.file_name = '{fileName}'" .format(fileName=name)
                selectData = ora .selectSqlExecute('cboss', selectSql)

                nowData = datetime .datetime .now()
                if len(selectData) == 0:
                    insDict = [{
                        'FILE_TYPE': '7711',
                        'file_name': name,
                        'proc_status': 0,
                        'file_status': 0,
                        'process_date': nowData,
                        'create_date': nowData,
                        'file_way': '1',
                        'total_amount': 0,
                        'notes': '文件解析入库成功'
                    }]
                    ora .batchInsertAll('cboss', 'cboss.ftp_file_interface', insDict)
                else:
                    updateSql = "update cboss.ftp_file_interface t set t.proc_status = 0 where t.file_name = '{name}'" \
                        .format(name=name)
                    ora .sqlExecute('cboss', updateSql)
            except Exception as error:
                ora .dataRollback()
                logging .error(error)
            else:
                ora .dataCommit()
    except Exception as error:
        ora .dataRollback()
        logging .error(error)


if __name__ == '__main__':
    try:
        #获取数据源链接
        config = Config('DefaultDataSourceCongfig')
        staticConfig = config.getStaticConfig()
        logging = config.getLogging()
        ora = config.getDataSource()
        conDict = staticConfig.getKeyDict()

        # 获取ftp
        ftp = getFtpConn()
        fileNames = getFileName()

        setStaus()
    finally:
        ora .dataClose()
