# -*- encoding: utf-8 -*-
# -------------------------------------------------------------------------------
# Name:
# Purpose:
#
# Author:      bhuang$
#
# Created:     
# Copyright:   (c) bhuang$ 08$
# Licence:     <your licence>
# -------------------------------------------------------------------------------
import gzip
try:
    import simlejson as json
except:
    import json
import re,os
import sys


import time

sys.path.append(".")
from superbase.utility.safeUtil import TryTime
from superbase.globalData import PROJECT_ROOT, gConfig
from superbase.utility.ioUtil import isLinux, mkdir, gzipOneFile

from superbase.baseClass import BaseClass
from superbase.constant import CFG_ACCOUNT_PROVIDER
from superbase.utility.accountUtil import AccountManager
from superbase.utility.logUtil import logInfo, logException, logDebug
from superbase.utility.processUtil import callMethod


def scandir(startdir, filter, result):
    for obj in os.listdir(startdir):
        file1 = os.path.join(startdir, obj)
        if os.path.isdir(file1):
            scandir(file1, filter, result)
        else:
            if filter(file1):
                result.append(file1)


def getGZFromDir(dir1):
    """
    从目录中取出所有gz文件，如果不是gz，gzip first
    :param dir1:
    :param gzfirst:
    :return:
    """

    def filter(f1):
        return True if os.path.splitext(f1)[1] == ".gz" else False

    result = []
    scandir(dir1, filter, result)
    return result


def gzAllFiles(dir1):
    def filter(f1):
        if os.path.splitext(f1)[1] != ".gz":
            gzipOneFile(f1)
        return True

    result = []
    scandir(dir1, filter, result)
    return result


ALIYUN = "aliyun.account"
ALIYUN_LOCALROOT = "aliyun.localRoot"


def preProcessDir(dir1):
    if len(dir1) > 1:
        if dir1[0] == "/":
            dir1 = dir1[1:]
        if dir1[-1] != "/":
            dir1 += "/"

    return dir1.replace("\\","/")


class AliYun(BaseClass):
    """
    121.42.9.207
    10.163.248.228
    """

    def __init__(self, params=""):
        subCfg = {
            CFG_ACCOUNT_PROVIDER: "spideraccount.samanager",
            #ALIYUN_LOCALROOT: PROJECT_ROOT,  # 正常这就是本地根
        }
        import oss2
        BaseClass.__init__(self, params, subCfg)
        aliyunCfg = AccountManager().getAccount(ALIYUN)
        accessKeyId = aliyunCfg["accessKeyId"]
        accessKeySecret = aliyunCfg["accessKeySecret"]
        endpoint = aliyunCfg["endPoint"]
        self.bucket = aliyunCfg["bucket"]
        self.oss = oss2.Bucket(oss2.Auth(accessKeyId, accessKeySecret),endpoint,aliyunCfg["bucket"])
        self.prefix = gConfig.get(ALIYUN_LOCALROOT,self._getDefaultDownRoot()).replace("\\", "/")

    def glob(self, rootDir):
        pass

    def preProcessPath(self, file1):
        if file1[0] == "/":  # 不能以/ 开头
            file1 = file1[1:]
        return file1.replace("\\", "/")

    def upFile(self, file1):
        dest = self.preProcessPath(file1[len(self.prefix):])
        MAX_TRY_UP = 10
        tryTime = 0  #
        while tryTime < MAX_TRY_UP:
            try:
                with open(file1, 'rb') as fileobj:
                    res = self.oss.put_object(dest, fileobj)
                if res and res.status == 200:
                    logInfo("ret=%s,file=%s" % (res.status, dest))
                    return
            except Exception, e:
                logException()
            tryTime += 1
            time.sleep(1)

    def downFile(self, file1, dest=None):
        """

        :param file1:
        :param dest:目标路径
        :return:
        """
        if not dest:
            dest = self.prefix
        dest = os.path.join(dest, file1)
        # dest = os.path.split(dest)[0]
        path = os.path.split(dest)[0]
        mkdir(path)
        file1 = self.preProcessPath(file1)

        @TryTime(3)
        def safeDown():
            return self.oss.get_object_to_file(file1, dest)

        res = safeDown()
        logInfo("ret=%s,src=%s,dest=%s" % (res.status, file1, dest))
        return dest

    def listDir(self, dir1, timeRange=None):
        """
        :param dir1:downData/www_tianyancha_com/detail/company_1
        :param timeRange: fmt:2018020100-2018020200
        :return:
        """
        objects = []
        tryTime = 0  #

        def getNameTS(name):
            m = re.search(r"(\d{4})/(\d{2})/(\d{2})/(\d{2})", name)
            return "%s%s%s%s" % (m.group(1), m.group(2), m.group(3), m.group(4))

        while tryTime < 3:
            try:
                dir1 = preProcessDir(dir1)
                # 列出bucket中”fun/”目录下所有文件
                beginTime, endTime = timeRange.split("-") if timeRange else (None, None)
                import oss2
                for idx, object_info in enumerate(oss2.ObjectIterator(self.oss,prefix=dir1)):
                    if beginTime and endTime:
                        ts = getNameTS(object_info.key)
                        if ts < beginTime or ts >= endTime:
                            # logDebug("ignore:%s"%object_info.key)
                            continue
                    objects.append(object_info.key)
                    logDebug("%s:%s" % (idx, object_info.key))
                return objects
            except Exception, e:
                logException()
            tryTime += 1
            time.sleep(1)

        return objects


    def downDir(self, dir1, dest=None, timeRange=None):
        if not dest:
            dest = self._getDefaultDownRoot()

        objects = self.listDir(dir1, timeRange)
        for obj in objects:
            try:
                self.downFile(obj, dest)
            except Exception:
                logException()

    def upDir(self, dir1, gzfirst=True):
        if gzfirst:
            gzAllFiles(dir1)
        files = getGZFromDir(dir1)
        for file in files:
            try:
                if True:  # os.path.getsize(file)>30:#小于30Byte的文件不处理
                    self.upFile(file)
                else:
                    logInfo("the file is too small,give up")
            except Exception:
                logException()
        return len(files)

    def delLocalDir(self, dir1):
        import shutil
        shutil.rmtree(dir1)

    def _getDefaultDownRoot(self):
        return "/opt/tempAliyun" if isLinux() else "c:/tempAliyun"

    def readFileFromAliyun(self, aliDir, timeRange=None, dest=None):
        """
        一个生成器
        用法：
        lines = AliYun().readJsonFromAliyun('downData/www_tianyancha_com/detail/company_1', '2018020100-2018020300')
        for line in lines:
            logDebug(line['name'])
        :param aliDir: downData/www_tianyancha_com/detail/company_1
        :param timeRange: fmt:2018020100-2018020200
        :param localRoot:如果指定就用该目录，推荐不指定
        :return:
        """

        if not dest:
            dest = self._getDefaultDownRoot()

        files = self.listDir(aliDir, timeRange)
        for fileName in files:
            try:
                fileName2 = os.path.join(dest, fileName)
                if not os.path.exists(fileName2):
                    self.downFile(fileName, dest)
                yield fileName2

            except Exception, e:
                logException()

    def readJsonFromAliyun(self, aliDir, timeRange=None, dest=None):
        """
        一个生成器
        用法：
        lines = AliYun().readJsonFromAliyun('downData/www_tianyancha_com/detail/company_1', '2018020100-2018020300')
        for line in lines:
            logDebug(line['name'])
        :param aliDir: downData/www_tianyancha_com/detail/company_1
        :param timeRange: fmt:2018020100-2018020200
        :param localRoot:如果指定就用该目录，推荐不指定
        :return:
        """

        if not dest:
            dest = self._getDefaultDownRoot()

        files = self.listDir(aliDir, timeRange)
        for fileName in files:
            try:
                fileName2 = os.path.join(dest, fileName)
                if not os.path.exists(fileName2):
                    self.downFile(fileName, dest)
                with gzip.open(fileName2, 'rb') as f:
                    for line in f:
                        yield json.loads(line)

            except Exception, e:
                logException()

    def help(self):

        print """
            usage: python aliyun.py cmd params
            commands:
            upFile filename
            downFile src [dest] #if dest is none,dest path is same as src
            upDir dirname
            downDir src,[dest] #if dest is none,dest path is same as src
            listDir dirname
            checkFile filename
            """


if __name__ == '__main__':
    callMethod(AliYun, sys.argv[1:])
