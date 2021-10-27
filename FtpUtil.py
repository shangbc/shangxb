# Time     2021/09/28 14::56
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 默认
from ftplib import FTP
from FtpConnectionUtil import FtpConnectionUtil
from StaticData import getVariable
from MethodTools import pathJoin
import datetime
import re


class FtpUtil(FtpConnectionUtil):
    def __init__(self, host, port, username, password):
        try:
            self .loggers = getVariable('loggers')
            ftp = FTP()
            # 打开调试级别2，显示详细信息
            # ftp.set_debuglevel(2)
            ftp.connect(host, port, timeout=60)
            ftp.encoding = 'utf-8'
            ftp.set_pasv(False)
            ftp.login(username, password)
            self .ftp = ftp
            self .loggers .info('连接{host}成功'.format(host=host))
        except Exception as error:
            raise error

    # 文件检索
    def getSearchDir(self, path, matchingRe, inteval):
        try:
            ftp = self .ftp
            fileNum = 0
            matchingReCompile = re.compile(matchingRe) if matchingRe is not None else None
            # matchingReCompile = re.compile('BOSS5710_busReConfirm_.*')
            ftp.cwd(path)
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
                        L = list(ftp.sendcmd('MDTM ' + fileName))
                        dir_t = L[4] + L[5] + L[6] + L[7] + '-' + L[8] + L[9] + '-' + L[10] + L[11] + ' ' + L[12] + L[
                            13] + ':' + L[14] + L[
                                    15] + ':' + L[16] + L[17]
                        beginDate = datetime.datetime.strptime(dir_t, "%Y-%m-%d %H:%M:%S")
                        self .loggers .debug("当前检索的文件：" + path + "/" + fileName)
                        # print("当前检索的文件：" + path + "/" + fileName)
                        # if deleteTime > beginDate:
                        #     fileNum += 1
                        if matchingReCompile is not None and not matchingReCompile.search(fileName):
                            continue
                        if inteval is not None and datetime.datetime.now() - datetime.timedelta(
                                minutes=inteval * 2) < beginDate:
                            continue
                        fileDict = {
                            'fileName': fileName,
                            'fileTime': beginDate
                        }
                        fileNames .append(fileDict)
                        self .loggers .debug(fileName)
                    except Exception as error:
                        self .loggers .error(error)
        except Exception as error:
            self .loggers .error(error)
        finally:
            self .loggers .info("目录：" + path + "检索完成！")
            return fileNames

    # 文件下载
    def fileDown(self, locaDir, downDir, file):
        try:
            ftp = self.ftp
            ftp .cwd(downDir)
            locaFile = pathJoin(locaDir, file)
            myFile = open(locaFile, 'wb')
            fileCodeDone = "RETR " + file
            ftp.retrbinary(fileCodeDone, myFile.write)

        except Exception as error:
            raise error
        else:
            self .loggers .info(file + "下载成功！")
        finally:
            myFile.flush()
            myFile.close()

    # 文件删除
    def fileDelete(self, deleteDir, file):
        try:
            ftp = self.ftp
            ftp.cwd(deleteDir)
            ftp.delete(file)
        except Exception as error:
            raise error
        else:
            self.loggers.info(file + "删除成功！")

    # 文件上传
    def fileUp(self, locaDir, upPath, fileName):
        try:
            ftp = self.ftp
            ftp.cwd(upPath)
            locaFile = pathJoin(locaDir, fileName)
            myFile = open(locaFile, 'rb')
            fileCodeUp = "STOR " + fileName
            ftp.storbinary(fileCodeUp, myFile)
        except Exception as error:
            raise error
        else:
            self .loggers .info(fileName + "上传成功！")
        finally:
            myFile.flush()
            myFile.close()

    def getFilesize(self, path, fileName):
        try:
            ftp = self .ftp
            ftp .cwd(path)
            size = ftp .size(fileName)
            return size
        except Exception as error:
            raise error

    def close(self):
        try:
            ftp = self .ftp
            ftp .close()
        except Exception as error:
            raise error