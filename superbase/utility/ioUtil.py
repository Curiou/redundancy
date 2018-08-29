# -*- coding: utf-8 -*-
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
import codecs
import json
import os
import time


def jsonFromFile(path1,encoding="utf-8"):
    try:
        with open(path1,"r") as f:
            from collections import OrderedDict
            return json.load(f,encoding=encoding,object_pairs_hook=OrderedDict)
    except Exception:
        from superbase.utility.logUtil import logError
        logError("no the file -%s"%path1)
        return None


def json2File(path1,data):
    try:
        with open(path1,"w") as f:
            json.dump(data,f)
    except Exception, e:
        pass


def getOSName():
    import platform
    return platform.platform().lower()

def isLinux():
    return getOSName().find("linux")>=0

def getOSSplit(os1=None):
    osName = getOSName() if not os1 else os1
    return "&" if "windows" in osName else ";"

def mkdir(path):
    """
    递归创建文件夹
    :param path: 文件路径
    :return:
    """
    if not os.path.exists(path):
        os.makedirs(path)


def getPrintDict(data):
    """
    dumps 是将dict转化成str格式，loads是将str转化成dict格式。
    :param data: 提取的数据
    :return:  str
    """
    if isinstance(data, dict):
        try:
            import json
            s = json.dumps(data, indent=4, ensure_ascii=False)
            return s
        except Exception, e:
            return "invalid-json-format"


def printDict(dict, log=None):
    """
    输出dict形式
    :param dict:  字典的数据
    :param log: 是否写入日志中
    :return:
    """
    info = json.dumps(dict, indent=4, ensure_ascii=False)
    if info:
        if not log:
            from superbase.utility.logUtil import logDebug
            logDebug(info)
        else:
            log(info)


def saveFile(fname, content):
    """
    保存文件
    :param fname: 文件的名字
    :param content: 要保存的内容
    :return:
    """
    mkdir(os.path.split(fname)[0])
    # content数据编码不统一时，我们要把得到的东西先decode为unicode再encode为str，而codecs.open就起这个作用
    with codecs.open(fname, "w", "utf-8") as f:
        f.write(content)


def listDir(dirname):
    """
    返回的是指定的文件夹包含的文件或文件夹的名字的列表
    :param dirname: 文件夹的名字
    :return:
    """
    from superbase.utility.logUtil import logInfo
    try:
        # 返回的是指定的文件夹包含的文件或文件夹的名字的列表
        dir1 = os.listdir(dirname)
        if dir1:
            logInfo("--list-%s" % dirname)
            for file in dir1:
                logInfo(file)
        return dir1
    except Exception:
        pass


def safeOpen(filePath, mode='r'):



    """
    返回一个打开的文件对象，一般只能读
    :param filePath: 文件的路径
    :param mode: io的形式
    :return: 打开的文件对象
    """
    import time
    while not os.path.exists(filePath):
        time.sleep(1)
    return open(filePath, mode)


def inputLoop(handleFunc, hint=""):
    """
    输入循环
    :param handleFunc: 一个回调函数的对象
    :param hint: 提示或标示
    :return:
    """
    exitSymbol = ":q"
    print("\n\n--hint:%s\ntype%s to exit" % (hint, exitSymbol))
    while True:
        ins = raw_input("$>>").strip()
        # 判断是否有错误
        if ins != exitSymbol:
            handleFunc(ins)
        else:
            return


def str2line(str):
    # 替换字符串里的换行符
    return str.replace("\n", "").replace("\r", "")


def file2line(fn):
    """
    把保存的文件重新清洗
    :param fn: 文件名
    :return: 替换过换行符的字符串
    """
    with open(fn, "r") as f:
        return str2line(f.read())


def removeNullItem(dict1):
    """
    将dict1 一点点分解
    :param dict1: 一个字典
    :return:
    """
    empty = (None, {}, (), [])
    for key, value in dict1.items():
        # 判断 value是否是dict型
        if isinstance(value, dict):
            removeNullItem(value)  # 调用自己
        # 判断 value是否是dict型或tuple型
        elif isinstance(value, list) or isinstance(value, tuple):
            for item1 in value:
                if isinstance(item1, dict):
                    removeNullItem(item1)
        else:
            # 删除空值的键值
            if value in empty:
                del dict1[key]


def gzipOneFile(path, dest=None):
    """
    gzip一个文件
    :param path: 路径
    :param dest: 文件
    :return:dest
    """
    if not dest:
        dest = "%s.gz" % path
    import gzip
    # 解压文件，
    g = gzip.GzipFile(filename="", mode="wb", compresslevel=9, fileobj=open(dest, 'wb'))
    g.write(open(path).read())
    g.close()
    return dest


def getExtIP(extractor):
    """
    还是直接查询ip138最简单
    :return:ip
    """
    from superbase.utility.logUtil import logInfo
    from spiderx.common.utility.parseUtil import CssElement
    url = "http://httpbin.org/ip"
    result = {}
    content = extractor.getResultByUrl(url, {"ip": CssElement("body > pre")}, result)
    data = result["ip"] if result["ip"] else content  # 用request会直接返回json
    ip = json.loads(data)["origin"]
    logInfo("the ip is:%s" % ip)
    return ip

def getLanIP():

    import socket
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    finally:
        s.close()

    return ip
def Sysinfo():
    """
    系统信息
    psutil是一个跨平台库
    用来查看数据的状态
    :return:
    """
    import psutil
    # 设置时间
    Boot_Start = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(psutil.boot_time()))
    time.sleep(0.5)
    Cpu_usage = psutil.cpu_percent()
    RAM = int(psutil.virtual_memory().total / (1027 * 1024))
    RAM_percent = psutil.virtual_memory().percent
    Swap = int(psutil.swap_memory().total / (1027 * 1024))
    Swap_percent = psutil.swap_memory().percent
    Net_sent = psutil.net_io_counters().bytes_sent
    Net_recv = psutil.net_io_counters().bytes_recv
    Net_spkg = psutil.net_io_counters().packets_sent
    Net_rpkg = psutil.net_io_counters().packets_recv
    BFH = r'%'
    print " \033[1;32m开机时间：%s\033[1;m" % Boot_Start
    print " \033[1;32m当前CPU使用率：%s%s\033[1;m" % (Cpu_usage, BFH)
    print " \033[1;32m物理内存：%dM\t使用率：%s%s\033[1;m" % (RAM, RAM_percent, BFH)
    print "\033[1;32mSwap内存：%dM\t使用率：%s%s\033[1;m" % (Swap, Swap_percent, BFH)
    print " \033[1;32m发送：%d Byte\t发送包数：%d个\033[1;m" % (Net_sent, Net_spkg)
    print " \033[1;32m接收：%d Byte\t接收包数：%d个\033[1;m" % (Net_recv, Net_rpkg)

    for i in psutil.disk_partitions():
        print " \033[1;32m盘符: %s 挂载点: %s 使用率: %s%s\033[1;m" % (i[0], i[1], psutil.disk_usage(i[1])[3], BFH)


def Net_io(s):
    """
    测试爬虫的运行的效率
    :param s:
    :return:
    """
    import psutil
    x = 0
    sum = 0
    while True:
        if x >= s:
            break
        r1 = psutil.net_io_counters().bytes_recv
        time.sleep(1)
        r2 = psutil.net_io_counters().bytes_recv
        y = r2 - r1
        print "%.2f Kb/s" % (y / 1024.0)
        sum += y
        x += 1
    result = sum / x
    print "\033[1;32m%s秒内平均速度：%.2f Kb/s \033[1;m" % (x, result / 1024.0)


def md5(str2):
    import hashlib
    if isinstance(str2,unicode):
        str2 = str2.encode(encoding="utf-8")
    return hashlib.md5(str2).hexdigest()


def delOverTimeFiles(root,before=3600*72):
    """
    遍历某个目录及其子目录，删除所有更新时间<stopTime
    :param root: 根目录，如c:\\spiderman\log
    :param stopTime: 数字时间戳格式,time.time()
    :return:
    """
    from superbase.utility.logUtil import logException,logInfo
    def deleteNullDir(dirr):
        """
        删除路径下的空目录
        :param dirr:
        :return:
        """
        if os.path.isdir(dirr):
            for p in os.listdir(dirr):
                d = os.path.join(dirr, p)
                if (os.path.isdir(d) == True):
                    deleteNullDir(d)
        if not os.listdir(dirr):
            info = 'del empty dir: %s'% dirr
            try:
                os.rmdir(dirr)
                logInfo("done:%s"%info)
            except Exception, e:
                logException("fail-%s"%info)


    ds = list(os.walk(root))  # 获得所有文件夹的信息列表
    stopTime = time.time()-before
    for d in ds:  # 遍历该列表
        try:
            from superbase.utility.logUtil import logDebug
            logDebug("processing-%s"%d[0])
            os.chdir(d[0])  # 进入本级路径，防止找不到文件而报错
            if d[2] != []:  # 如果该路径下有文件
                for x in d[2]:  # 遍历这些文件
                    try:
                        lastmodifytime = os.stat(x).st_mtime  # 获取文件创建时间
                        if lastmodifytime < stopTime:
                            try:
                                info = "del-%s/%s"%(d[0],x)
                                os.remove(x)  # 删掉
                                logInfo("done:%s"%info)
                            except Exception, e:
                                logException("fail:%s"%info)
                    except Exception, e:
                        logException()
        except Exception, e:
            logException()



    #TODO,清除空目录
    deleteNullDir(root)
