# Time     2021/09/28 14::56
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 默认
from FtpConnectionUtil import FtpConnectionUtil
from StaticData import getVariable
from MethodTools import pathJoin
import datetime
import re
import stat
import os
import paramiko


class SFtpUtil(FtpConnectionUtil):
    def __init__(self, host, port, username, password):
        try:
            self .loggers = getVariable('loggers')
            self.loggers.info('开始连接sftp：{host}'.format(host=host))
            client = paramiko.SSHClient()  # 获取SSHClient实例
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(host, username=username, password=password, port=port)  # 连接SSH服务端
            # client.connect("192.168.65.128", username="silent", password="shangbc")  # 连接SSH服务端
            transport = client.get_transport()  # 获取Transport实例

            # 创建sftp对象，SFTPClient是定义怎么传输文件、怎么交互文件
            sftp = paramiko.SFTPClient.from_transport(transport)

            self .sftp = sftp
            self .client = client
            self .loggers .info('连接{host}成功'.format(host=host))
        except Exception as error:
            raise error

    # 文件检索
    def getSearchDir(self, path, matchingRe, inteval):
        try:
            sftp = self .sftp
            fileNum = 0
            matchingReCompile = re.compile(matchingRe) if matchingRe is not None else None
            # matchingReCompile = re.compile('BOSS5710_busReConfirm_.*')
            fileNum = 0
            fileNames = []
            dirList = sftp.listdir_attr(path)
            for dirFile in dirList:
                if stat.S_ISDIR(dirFile.st_mode):
                    pass
                else:
                    fileTime = datetime.datetime.fromtimestamp(dirFile.st_mtime)
                    fileName = dirFile.filename
                    if matchingReCompile is not None and not matchingReCompile.search(fileName):
                        continue
                    self.loggers.error(fileTime)
                    self .loggers .error(fileName)
                    if inteval is not None and datetime.datetime.now() - datetime.timedelta(
                            minutes=inteval * 2) < fileTime:
                        continue
                    fileDict = {
                        'fileName': fileName,
                        'fileTime': fileTime
                    }
                    fileNames.append(fileDict)
        except Exception as error:
            self .loggers .error(error)
        finally:
            self .loggers .info("目录：" + path + "检索完成！")
            return fileNames

    # 文件下载
    def fileDown(self, locaDir, downDir, file):
        try:
            sftp = self .sftp
            locaFile = pathJoin(locaDir, file)
            downFile = pathJoin(downDir, file)
            sftp .get(downFile, locaFile)

        except Exception as error:
            self .loggers .error(file + "下载失败！")
            raise error
        else:
            self .loggers .info(file + "下载成功！")

    # 文件删除
    def fileDelete(self, deleteDir, file):
        try:
            sftp = self.sftp
            fileName = pathJoin(deleteDir, file)
            sftp .remove(fileName)
        except Exception as error:
            self .loggers .error(file + "删除失败！")
            raise error
        else:
            self.loggers.info(file + "删除成功！")

    # 文件上传
    # 默认情况下， pysftp.Connection.put会验证通过检查目标文件的大小来上传.如果服务器端进程设法过快地删除文件，则读取文件大小将失败.
    # 您可以通过将confirm参数设置为False来禁用上传后检查:
    def fileUp(self, locaDir, upPath, fileName):
        try:
            sftp = self.sftp
            locaFile = pathJoin(locaDir, fileName)
            upFile = pathJoin(upPath, fileName)
            sftp .put(locaFile, upFile, confirm=False)
        except Exception as error:
            self .loggers .error(fileName + "上传失败！")
            raise error
        else:
            self.loggers.info(fileName + "上传成功！")

    def getFilesize(self, path, fileName):
        try:
            sftp = self .sftp
            file = pathJoin(path, fileName)
            size = sftp .stat(file) .st_size
            return size
        except Exception as error:
            raise error

    def close(self):
        try:
            sftp = self .sftp
            client = self .client
            sftp .close()
            client .close()
        except Exception as error:
            raise error