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
import multiprocessing
import os
import subprocess
import sys
import threading
import time

import psutil

from superbase.constant import CFG_DB_MONITOR, CFG_DB_BUSINESS, CFG_JOB_NAME, LOG_ALWAYS
from superbase.globalData import gConfig
from superbase.utility import timeUtil
from superbase.utility.ioUtil import isLinux
from superbase.utility.logUtil import logException, logDebug, logError, logInfo
from superbase.utility.safeUtil import exceptionWrap
from superbase.utility.timeUtil import getTimestamp, getTimestampBySec


def assert2(condition, info="assert error"):
    """
    声明
    :param condition: 环境
    :param info: 错误消息
    :return:
    """

    if not condition:
        logError(condition)
        # raise AssertionError(info)
        assert (condition)


def runProcess(cmd, outInfo=None, maxOutInfoNum=1000, debug=False, redirect=False,exitInfo=None):
    """
    运行多进程
    :param cmd:
    :param outInfo: 输出的console信息list
    :param log: 可定制的logger
    :param maxOutInfoNum: 最多输出的console 信息行数
    :param debug: debug模式只是输出命令行
    :param redirectFile: 是否用重定向文件模式
    :param ,exitInfo: 遇到该消息退出
    :return:
    """
    # cmd += "\n" #what the hell use it?
    from superbase.utility.logUtil import logInfo
    try:
        if redirect:
            idx = cmd.rfind(">")
            if idx > 0:  # 判断是否需要重定向,重定向必须是绝对路径
                outfile = cmd[idx + 1:].strip()
                outfile = os.path.abspath(outfile)
                logInfo("redirect-file=%s" % outfile)
                dir1 = os.path.dirname(outfile)
                from superbase.utility.ioUtil import mkdir
                mkdir(dir1)
                redirectFile = open(outfile, "w")
                cmd = cmd[:idx]
        else:
            redirectFile = None
        logDebug("\n%s the cmd is %s\n" % (timeUtil.getCurTime(), cmd))
        if debug:
            return
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
        lineNum = 0
        while True:
            line = p.stdout.readline()
            if not line:
                break
            if exitInfo and line.find(exitInfo)>=0:
                break
            # log.debug(line)
            if (outInfo != None):
                outInfo.append(line);
                lineNum += 1
                if maxOutInfoNum > 0 and lineNum > maxOutInfoNum:
                    del outInfo[:-1]
                    lineNum = 0
                    if redirectFile:
                        redirectFile.flush()
                if redirectFile:
                    redirectFile.write(line)
        if redirectFile:
            redirectFile.close()

        logDebug("process-done:%s" % cmd)
    except Exception:
        from superbase.utility.logUtil import logException
        logException()

    return outInfo


class TProcessEngine(threading.Thread):  #############这里有问题???
    """
    多线程引擎
    notifyDone is a class with a callback() method
    class Notify:
        def callback(self,outInfo=None):
            pass
    """

    def __init__(self, cmd, outInfo=None, maxOutInfoNum=1000, debug=False, redirect=False, hang=False, notifyDone=None):
        super(TProcessEngine, self).__init__()
        self.cmd = cmd
        self.outInfo = outInfo
        self.notifyDone = notifyDone
        self.debug = debug
        self.maxOutInfoNum = maxOutInfoNum
        self.redirect = redirect

        if not hang:
            self.start()

    # 运行
    def run(self):
        runProcess(self.cmd, self.outInfo, maxOutInfoNum=self.maxOutInfoNum, debug=self.debug, redirect=self.redirect)
        if self.notifyDone != None:
            self.notifyDone.callback(self.outInfo)

class TAsyncJob(threading.Thread):
    """

    """

    def __init__(self,cmd,shell=True,delay=0):
        threading.Thread.__init__(self)
        self.delay = delay
        self.cmd = cmd
        self.shell = shell
        self.start()

    # 运行
    def run(self):
        if self.delay:
            time.sleep(self.delay)
        asyncRun(self.cmd,self.shell)

def asyncRun(cmd,shell=True):
    try:
        logDebug("asyncRun:%s"%cmd)
        subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=shell)
    except Exception, e:
        logException()


def timeoutFunc(cmd, timeout, interval=1):
    """
    超时处理
    :param cmd: class cmd:
                    def execute():
                      返回结果就结束任务,otherwise,继续
    :param timeout: 超时的时间
    :param interval: 延时间隔
    :return:
    """
    begin = time.time()
    while begin + timeout > time.time():
        time.sleep(interval)
        result = cmd.execute()
        if result:
            return result


def mThread(threadNum, func, argsList):
    """
    多线程
    :param threadNum: 线程数
    :param func: 要使用多线程的函数
    :param argsList: [（进程1的参数tuple,queue），（进程2的参数tuple）,...]
    :return:
    """
    record = []
    q = multiprocessing.Queue()

    class MyThread(threading.Thread):
        def __init__(self, args):
            threading.Thread.__init__(self)
            self.args = args

        def run(self):
            return func(*self.args)

    for i in range(threadNum):
        param = list(argsList[i])
        param.append(q)
        thread = MyThread(param)
        thread.start()
        record.append(thread)

    for thread in record:
        thread.join()
    return q


def mProcess(processNum, func, argsList):
    """
    多进程
    :param processNum: 进程数
    :param func: 要使用多进程的函数
    :param argsList: [（进程1的参数tuple），（进程2的参数tuple）,...]
    :return:
    """
    record = []
    q = multiprocessing.Queue()

    for i in range(processNum):
        param = list(argsList[i])
        param.append(q)
        process = multiprocessing.Process(target=func, args=tuple(param))
        process.start()
        record.append(process)

    for process in record:
        process.join()
    return q


def applyFunc(obj, strFunc, arrArgs):
    """
    调用方法
    :param obj: 要使用的对象
    :param strFunc: 方法名
    :param arrArgs: 参数
    :return:
    """
    try:
        return callFunc(obj, strFunc, arrArgs)
    except:
        logException()

def callFunc(obj, strFunc, arrArgs):
    objFunc = getattr(obj, strFunc)
    return callFunction(objFunc, arrArgs)

def callMethod(cls, argv):
    """
    脚本的入口函数
    :param cls:class
    :param argv: cfg method params, cfg can be null
    :return:
    """
    try:
        cfg = argv[0]
        if cfg.find("=") > 0:
            obj = cls(cfg)
            return callFunc(obj, argv[1], argv[2:])
        else:
            obj = cls()
            return callFunc(obj, argv[0], argv[1:])
    except Exception:
        logException(LOG_ALWAYS)
    finally:  # close global resource if has
        from superbase.globalData import gTop
        gTop.release()
        #logInfo("--finish--")

def callFunction(func, argv):
    """
    调用函数优化
    :param func: 要调用的方法
    :param argv: 参数
    :return:
    """
    try:
        return apply(func, argv)
    except Exception:
        logException()


def reloadModule(name):
    """
    重新加载模块
    :param name: 模块名
    :return:
    """
    try:
        reload(sys.modules[name])
    except Exception:
        logException()


class MapFunc(object):
    def __init__(self,func,params=None):
        """

        :param func:
        :param params: a dict
        """
        self.params = params if params else {}
        self.func = func

    @exceptionWrap
    def map(self,x):
        return self.func(x, self.params)

def isProcessAliveFromCmd(cmd):
    """
    判断进程是否存活
    :param cmd:
    :return: True/False
    """
    import psutil
    isAlive = False
    for pid in psutil.pids():
        p = psutil.Process(pid)
        info = " ".join(p.cmdline())
        idx = info.find(cmd)
        if idx== 0:
            print("idx=%s,pid=%s\n%s"%(idx,pid,info))
            isAlive = True
            break
    return isAlive

def isProcessAlive(pid,createTime=None):
    try:
        p = psutil.Process(pid)
        if not createTime or p.create_time()==createTime:#如果有传入createTime，则严格匹配
            return p.is_running()
    except psutil.NoSuchProcess,e:
        logInfo("the process is killed already-%s"%pid)
    except Exception, e:
        logException()
    return False

def getProcessInfo(pid=None):
    try:
       if not pid:
        pid = os.getpid()
        pro = psutil.Process(pid)
        return {
            "pid":pid,
            "createTime":pro.create_time(),
            "name":pro.name()
        }
    except psutil.NoSuchProcess,e:
        logInfo("the process is killed already-%s"%pid)
    except Exception, e:
        logException()

def killChildren(childName):

    try:
        pro = psutil.Process(os.getpid())
        for proc in pro.children(recursive=True):
            pName = proc.name()
            if pName.find(childName)>=0:
                proc.kill()
                return logInfo("kill--%s"%pName)
    except psutil.NoSuchProcess,e:
        logInfo("the child has been killed")
    except Exception, e:
        logException()

def killProcess(pid,createTime=None,killParent=True):
    logInfo("before kill pid=%s"%pid)
    pid = int(pid)
    killInfo = []
    def killone(proc):
        #cmd = " ".join(proc.cmdline())
        try:
            info = "\nkill Job %s--%s-\n%s"%(proc.pid,getTimestampBySec(proc.create_time())," ".join(proc.cmdline()))
            if isLinux():
                runProcess("sudo kill -9 %s"%proc.pid)
            else:
                proc.kill()
            logInfo(info)
            killInfo.append(info)
        except psutil.NoSuchProcess,e:
            killInfo.append("has killed:%s"%info)
        except Exception, e:
            logException()

    try:
         pro = psutil.Process(pid)
         if not createTime or pro.create_time()==float(createTime):
             for proc in pro.children(recursive=True):
                killone(proc)
             parent = pro.parent()
             killone(pro)
             if killParent:
                 killone(parent)
         else:
             logError("error:createTime=%s,proTime=%s"%(createTime,pro.create_time()))
    except psutil.NoSuchProcess,e:
        info = "\nthe process is killed already-%s"%pid
        logInfo(info)
        killInfo.append(info)
    except Exception:
        logException()
    return killInfo

def showProcess(qinfo='python'):
    """
    获取进程的进程号和cmdline
    :param qinfo:python!!job.id=!!
    :return:
    cd /opt/work/spiderman/superbase/;sudo git pull;cd ..;python jobManager/manage/node.py pkill getTYCDetail;exit
    """
    import psutil
    # 遍历所有运行的进程的进程号,然后通过进程号获取每个进程的cmdline
    result = []
    for pid in psutil.pids():
        try:
            p = psutil.Process(pid)
            cmd = p.cmdline()  # 获取进程的cmdline,形式: /bin/bash
            if len(cmd) > 0:
                info = " ".join(cmd)
                allFound = 0
                #if " pkill " not in info:#不处理pkill
                subInfos = qinfo.split("!!")
                for subInfo in subInfos:
                    if info.find(subInfo) < 0:
                        allFound = 0
                        break
                    allFound+=1
                if allFound and pid!=os.getpid():#排除showProcess进程
                    print ("pid=%s-%s %s" % (pid,p.create_time(), info))
                    result.append((pid,p.create_time(),info))
        except Exception,e:
            logInfo(e)
    return result

def pkill(exe,ts=0):
    """

    :param exe:
    :param ts: 创建时间超过3s
    :return:
    """
    pids = showProcess(exe)
    info = []
    curTime = time.time()
    curPID = os.getpid()
    for pid,ctime,info2 in pids:
        try:
            if curTime-ctime>ts and pid!=curPID:
                info.extend(killProcess(pid))
            else:
                logInfo("the process'create time not match the critical-\n%s"%info2)
        except Exception, e:
            logInfo(e)
    return info

class ProcssLock(object):
    def __init__(self,name):
        self.name = name
        self.lock = multiprocessing.Lock()

    def __enter__(self):
        logInfo("before--lock %s"%self.name)
        self.lock.acquire()
        logInfo("lock %s"%self.name)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.lock.release()
        logInfo("unlock %s"%self.name)