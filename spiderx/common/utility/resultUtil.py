# -*- encoding: utf-8 -*-
"""
   Author: bafeng huang<hbfhero@163.com>
   Copyright bafeng huang

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""
import json
import os
import time
from sqlite3 import OperationalError

from spiderx.common.constant import CFG_DOWN_INDEX, CFG_DOWN_SYNCINFO, CFG_DOWN_SYNCBEGIN, \
    CFG_DOWN_SYNCCURR, CFG_DOWN_SYNCEND, CFG_DOWN_INCMODE, CFG_DOWN_INCPOINT, CFG_RESULT_FUNC, CFG_DOWN_BRKMODE, \
    CFG_DOWN_SYNCINTERVAL, CFG_DOWN_ROOT
from spiderx.common.utility.dbUtil import LocalDb
from superbase.constant import CFG_JOB_NAME, CFG_JOB_BATCH, CFG_JOB_ENABLE
from superbase.globalData import PROJECT_ROOT, gConfig
from superbase.utility.ioUtil import printDict, str2line, mkdir, md5, getPrintDict, jsonFromFile, json2File
from superbase.utility.logUtil import logInfo, logException, logError
from superbase.utility.timeUtil import getTimestamp, getHour


class BaseResultHandler(object):
    """
    默认只用于debug使用
    """

    def __init__(self, callBackResult=None, handleFunc=None):
        """
        :param preResult: callback result
        """
        self.callBackResult = callBackResult if callBackResult else {}
        self.handleFunc = handleFunc

    def handle(self, result):
        """ 更新回调函数返回结果"""
        # 更新回调函数
        result.update(self.callBackResult)
        if self.handleFunc:
            self.handleFunc(result)
        else:
            printDict(result)


class HourlySaveLocalHandler(BaseResultHandler):
    """
    每条结果按如下路径存储：
    PROJECT_ROOT/downData/{index}/year/month/day/hour/down.txt
    是按每小时保存这一小时的结果
    """
    def __init__(self, callBackResult=None, syncPoint=None, filterResultFunc=None):
        """ 继承BaseResultHandler"""
        BaseResultHandler.__init__(self, callBackResult)
        self.sync = SyncPoint() if not syncPoint else syncPoint
        self.filterResult = filterResultFunc
        self.defaultFileName = "%s.txt" % (time.strftime("%Y%m%d%H",time.localtime()))
    # override optional
    def preProcess(self, result):
        """ 更新回调函数返回结果"""
        result.update(self.callBackResult)
        if self.filterResult:
            result = self.filterResult(result)
        return result

    def setCallBackResult(self,cr):
        self.callBackResult = cr

    @classmethod
    def getDownIndex(self):
        """
        :return: index,without the "/" at the begin and end
        路径index,可以多级,逐级细分
        """
        idx = gConfig.get(CFG_DOWN_INDEX)
        if idx.endswith("/"):
            idx = idx[:-1]
        if idx.startswith("/"):
            idx = idx[1:]
        return idx

    # override optional
    def getFileName(self,params=None):
        return self.defaultFileName

    # override optional
    def getSavePath(self,params=None):
        hour = getHour(getTimestamp())
        index = self.getDownIndex()
        # 设置文件的保存路径
        rootPath = gConfig.get(CFG_DOWN_ROOT)
        path = os.path.join(rootPath, "downData/{index}/{hour}/{batch}".format(index=index, batch=gConfig.get(CFG_JOB_BATCH),hour=hour))
        mkdir(path)
        return path

    # override optional
    def saveFile(self,fileName,result):
        # 将内容保存
        r2 = str2line(json.dumps(result))
        with open(fileName, "a") as f:
            f.write("%s\n" % r2)

    def handle(self, result):
        try:
            result = self.preProcess(result)
            if result:

                r1 = self.sync.saveSyncPoint(result)
                path = self.getSavePath()
                fileName = self.getFileName()
                fileName = os.path.join(path, fileName)
                self.saveFile(fileName,r1)

        except Exception:
            logException()

class FileSaveHandler(HourlySaveLocalHandler):
    def __init__(self,fileName):
        self.fileName = fileName
        HourlySaveLocalHandler.__init__(self)

    def getFileName(self,params=None):
        return self.fileName

    def saveFile(self,fileName,result):
        res = result["file"]
        with open(fileName, 'wb') as f:
            for chunk in res.iter_content(100000):
                f.write(chunk)

class SyncPoint(object):
    """
    同步点的作用：
    当任务中断重启或者周期任务运行，可以syncFromRemote，获得上次最后一条数据
    爬虫需要维护同步信息，也就是数据中应该包含同步信息
    简单的同步信息可以从时间，id获得，复杂的需要自行提供一个CFG_DOWN_SYNCINFO:value,
    """

    def __init__(self, index=None):
        """
        :param index:
        """
        self.index = gConfig.get(CFG_DOWN_INDEX) if not index else index
        if self.index:
            syncDir = os.path.join(PROJECT_ROOT,"syncDir")
            if self.index:
                path1 = os.path.join(syncDir,"%s.json"%self.index)
                mkdir(os.path.split(path1)[0])
                self.localFile = path1
            self.lastSyncInfo = None
            self.saveNum = 0
            self.syncRemote = gConfig.get(CFG_JOB_ENABLE) or gConfig.get("debug.sync")
            self.checkSync()
        else:
            logError("no syncIndex!!")
            self.syncRemote = False

    def getSyncPoint(self):
        """
        默认从本地文件读取同步点
        :param index:
        :return: 同步点的result,or None
        """
        if self.index:
            data = jsonFromFile(self.localFile)
            if data and isinstance(data["syncInfo"],basestring):
                data["syncInfo"] = json.loads(data["syncInfo"])
            return data


    def saveSyncPoint(self, result,sync2Remote=False):
        """
        保存同步点
        :param result:
        :param sync2Remote:默认每次都同步到remote
        :return:返回去掉syncInfo的数据
        """
        if self.index:
            try:
                index = self.index
                #如果result中 同步点信息
                if CFG_DOWN_SYNCINFO in result:
                    #把result中 已经同步的信息 赋值与syncInfo
                    syncInfo = result.get(CFG_DOWN_SYNCINFO,{})
                    #删除result 中的同步点信息
                    del result[CFG_DOWN_SYNCINFO]
                    data = {
                        "id": md5(index),
                        "idx": index,
                        "syncInfo": syncInfo,
                        "upTime": getTimestamp()#时间戳
                    }
                    json2File(self.localFile,data)
                    self.saveNum += 1
                    if sync2Remote or (self.saveNum%gConfig.get(CFG_DOWN_SYNCINTERVAL,5)==1): #默认每5次同步到remote:
                        self.syncToRemote(data)

            except Exception, e:
                logException()

            return result

    def saveLastSyncInfo(self,result):
        """
        特别处理最后一条同步信息，确保同步到remote
        :param result:
        :return:
        """
        return self.saveSyncPoint(result,True)

    def checkSync(self):
        """
        检查远程同步点和本地同步点
        :return:
        """
        if self.syncRemote:
            index = self.index
            from jobManager.job import Job
            dataRemote = Job().getSyncPoint(index)
            dataLocal = self.getSyncPoint()
            remoteTime = dataRemote[0]["upTime"] if dataRemote else "20000102030405"
            localTime = dataLocal["upTime"] if dataLocal else "20000102030405"
            if localTime > remoteTime:
                self.syncToRemote()
            elif localTime < remoteTime:
                self.syncFromRemote()

    def syncToRemote(self,data=None):
        """
        同步到远程
        :return:
        """
        if self.syncRemote:
            index = self.index
            if not data:
                data = self.getSyncPoint()
            if isinstance(data["syncInfo"],dict):
                data["syncInfo"] = json.dumps(data["syncInfo"])
            from jobManager.job import Job
            Job().saveSyncPoint(data, index)

    def syncFromRemote(self):
        """
        从远程同步
        :param index:
        :return:
        """
        if self.syncRemote:
            index = self.index
            from jobManager.job import Job
            data = Job().getSyncPoint(index)
            if data:
                data = data[0]
                data["syncInfo"] = json.loads(data["syncInfo"])
                json2File(self.localFile,data)

    def test(self, case):
        """
        test 同步
        case 1:
            saveLocal without syncInfo
                check local db,insert done
            getLocal
                check result
            saveLocal with syncInfo
                check local db,update done
            getLocal
                check result

        case 2:
            checkSync
                check remote db,insert done
            saveLocal again with something new
            checkSync
                check remote db,update done
            saveRemote,and change something
            checkSync
                check local db,update done
        :param case:
        :return:
        """
        case = int(case)
        ori = {
            "data": "test"
        }
        if case == 1:
            data = self.saveSyncPoint(ori)
            # assert(data==ori)
            print("saveLocal without syncInfo:insert=%s\n" % (self.getSyncPoint()))
            ori[CFG_DOWN_SYNCINFO] = {"idtest": 1}
            data2 = self.saveSyncPoint(ori)
            print("saveLocal with syncInfo:insert=%s\n" % (self.getSyncPoint()))
        elif case == 2:
            self.checkSync()
            from jobManager.job import Job

            print("check remote db,insert done %s\n" % (Job().getSyncPoint(self.index)))
            ori["newData"] = 'test2'
            time.sleep(2)
            data = self.saveSyncPoint(ori)
            self.checkSync()
            print("check remote db,update done %s\n" % (Job().getSyncPoint(self.index)))
            time.sleep(2)
            ori[CFG_DOWN_SYNCINFO] = json.dumps({"remote": 1})
            data = {
                "result": json.dumps(ori),
                "syncInfo": json.dumps({"remote": 1}),
                "upTime": getTimestamp()
            }
            Job().saveSyncPoint(data, self.index)
            self.checkSync()
            print("sync from remote,local=%s\n" % (self.getSyncPoint()))
        print ("test done\n")


    def _getSyncInfoByCfg(self,syncInfo):
        # anyway，如果有人工干预参数，以干预参数为准
        def setByCfg(name):
            """ 设置产数"""
            if gConfig.get(name, None):
                syncInfo[name] = gConfig.get(name)

        setByCfg(CFG_DOWN_SYNCBEGIN)       # 同步开始点信息
        setByCfg(CFG_DOWN_SYNCCURR)     # 同步当前点信息
        setByCfg(CFG_DOWN_SYNCEND)       # 同步结束点信息
        setByCfg(CFG_DOWN_INCPOINT)      # 增量起始点信息
        return syncInfo

    def getNewSyncInfoByDesc(self, oldSyncInfo, initBegin=None, initEnd=-365 * 10 * 24 * 3600 * 1000):
        """
        适用于同步信息是递减的情况,如以发帖时间为同步点
        :param oldSyncInfo: 前一次的同步信息
        :param initEnd: 第一次创建时用的结束时间，毫秒级别，默认是10年前
        :return:
        """
        syncInfo = oldSyncInfo

        if not initBegin:#默认就用当前时间
            initBegin = time.time() * 1000

        if not syncInfo:  # 第一次抓取
            syncInfo = {
                CFG_DOWN_INCPOINT: initBegin,  # 下一次的增量起始点
                CFG_DOWN_SYNCBEGIN: initBegin,   # 开始
                CFG_DOWN_SYNCCURR: initBegin,    # 执行
                CFG_DOWN_SYNCEND: initBegin + initEnd    # 结束
            }
        else:
            # 存量抓完了，或者没抓完但是放弃剩余存量，就用正常增量模式
            if syncInfo[CFG_DOWN_SYNCCURR] <= syncInfo[CFG_DOWN_SYNCEND] or gConfig.get(CFG_DOWN_INCMODE, 0):
                syncInfo[CFG_DOWN_SYNCEND] = syncInfo[CFG_DOWN_INCPOINT]
                syncInfo[CFG_DOWN_INCPOINT] = syncInfo[CFG_DOWN_SYNCBEGIN] = syncInfo[CFG_DOWN_SYNCCURR] = initBegin
            else:  # 上次没爬完，断点续爬
                logInfo("use break point mode,go on crawling from the last break point")
                # syncInfo[CFG_DOWN_INCPOINT] = syncInfo[CFG_DOWN_SYNCBEGIN],
                syncInfo[CFG_DOWN_SYNCBEGIN] = syncInfo[CFG_DOWN_SYNCCURR]


        return self._getSyncInfoByCfg(syncInfo)

    def getNewSyncInfoByInc(self, oldSyncInfo, initBegin=0, initEnd=10 ** 10):
        """
        适用于同步信息是递减的情况,如以ID为同步点
        :param oldSyncInfo: 前一次的同步信息
        :param initBegin: 第一次创建时用的起始ID，默认是0
        :param initEnd: 第一次创建时用的结束ID，默认是10**10
        :return:
        """
        syncInfo = oldSyncInfo
        if not syncInfo:  # 第一次抓取
            syncInfo = {
                CFG_DOWN_SYNCBEGIN: initBegin,   # 开始
                CFG_DOWN_SYNCCURR: initBegin,    # 执行
                CFG_DOWN_SYNCEND:  initEnd    # 结束
            }
        else:#递增模式下无论断点还是增量都用一种逻辑,就是断点续爬
            syncInfo[CFG_DOWN_SYNCBEGIN] = syncInfo[CFG_DOWN_SYNCCURR]


        return self._getSyncInfoByCfg(syncInfo)
