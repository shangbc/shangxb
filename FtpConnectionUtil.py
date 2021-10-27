# Time     2021/09/28 10::49
# Auhtor   ShangXb
# ide      PyCharm
# Verion   1.0
# function ftp连接方式


class FtpConnectionUtil(object):
    @staticmethod
    def getSearchDir(self):
        pass

    def exceGetSearchDir(object, path, matchingRe=None, inteval=None):
        try:
            return object.getSearchDir(path, matchingRe, inteval)
        except Exception as error:
            raise error

    def exceFileDown(object, locaDir, downDir, file):
        return object.fileDown(locaDir, downDir, file)

    def exceGetFilesize(object, path, fileName):
        return object.getFilesize(path, fileName)

    def exceFileDelete(object, deleteDir, file):
        return object.fileDelete(deleteDir, file)

    def exceFileUp(object, locaDir, upPath, fileName):
        return object.fileUp(locaDir, upPath, fileName)

    def close(object):
        return object .close()
