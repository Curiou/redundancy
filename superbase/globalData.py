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
import os
import platform
import sys


def getProjectRoot():
    p = os.path.dirname(os.path.abspath(__file__))
    i = p.rfind("superbase")
    return p[:i]


PROJECT_ROOT = getProjectRoot()
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# print sys.path

OS_PLATFORM = platform.system()  # Linux,Windows,Mac

from superbase.constant import INVALID_JOB_NAME, INVALID_BATCHID, \
    CFG_JOB_NAME, CFG_JOB_BATCH, CFG_LOG_CONSOLE_LEVEL, \
    CFG_LOG_FILE_LEVEL, CFG_LOG_DB_LEVEL, CFG_LOG_FILE_NAME, CFG_LOG_MAX_EXCEPTION, CFG_LOG_DISABLE_DB, \
    CFG_BATCH_CANCLE, CFG_JOB_ID, CFG_DEBUG_SAVEFILE, \
    CFG_DEBUG_SAVEFILENAME, CFG_JOB_HEARTBEAT, CFG_DB_MONITOR, CFG_DB_BUSINESSNAME, CFG_DB_BUSINESS, \
    CFG_LOG_DISABLE_FILE, GD_LOGGER, GD_CFG_IN, GD_JOB_ERROR, GD_SUB, GD_ACCOUNTINFO, CFG_ACCOUNT_PROVIDER, \
    CFG_NODE_IPTYPE, NODE_TPE_PROXYIP, NODE_TPE_AUTOIP


class gConfig(object):
    # 全局配置
    cfg = {
        'env': 'DEV',  # ONLINE,TEST,DEV

        ##log related
        CFG_LOG_CONSOLE_LEVEL: 'DEBUG',  # CRITICAL,ERROR,WARN,INFO,DEBUG
        CFG_LOG_FILE_LEVEL: 'INFO',  # CRITICAL,ERROR,WARN,INFO,DEBUG
        CFG_LOG_DB_LEVEL: 'ERROR',  # CRITICAL,ERROR,WARN,INFO,DEBUG,要进入db的log必须是error or critical
        CFG_LOG_FILE_NAME: 'spiderx.log',
        CFG_LOG_MAX_EXCEPTION: 20,  # 每个job catch的exception log最大数量
        CFG_LOG_DISABLE_DB: 0,  # 控制db log,默认开
        CFG_LOG_DISABLE_FILE: 0,  # 控制file log,默认开

        # job related
        CFG_JOB_NAME: INVALID_JOB_NAME,  # 无效的 工作名称
        CFG_JOB_BATCH: INVALID_BATCHID,  # 没有分批处理的id
        CFG_JOB_ID: 0,
        CFG_JOB_HEARTBEAT: 600,  # 600 seconds 没响应,可以认为任务出错
        CFG_BATCH_CANCLE: 1,  # 强制提交,已经提交但未运行的将被close

        # db related
        # CFG_DB_DISABLE:0, #deprecated
        CFG_DB_MONITOR: "datamonitor",  # 监控
        CFG_DB_BUSINESS: None,  # 业务
        #CFG_ACCOUNT_PROVIDER:"spideraccount.samanager",

        CFG_NODE_IPTYPE: NODE_TPE_AUTOIP,#节点类型默认是自动IP

        # debug related:
        CFG_DEBUG_SAVEFILE: 0,  # 存储下载文件
        CFG_DEBUG_SAVEFILENAME: 0,  # 存储下载文件名称
    }

    @classmethod
    def set(cls, key, val):  # 设置
        cls.cfg[key] = val

    @classmethod
    def get(cls, key, defVal=None):  # 获取
        return cls.cfg[key] if key in cls.cfg else defVal

    @classmethod
    def update(cls, newDict):  # 更新
        cls.cfg.update(newDict)

    @classmethod
    def items(cls):  # 项目
        return cls.cfg.items()


# 设置工作的信息
def setJobInfo(jobName, batchId):
    if gConfig.get(CFG_JOB_NAME) == INVALID_JOB_NAME:
        gConfig.set(CFG_JOB_NAME, jobName)
    if gConfig.get(CFG_JOB_BATCH) == INVALID_BATCHID:
        gConfig(CFG_JOB_BATCH, batchId)


# 全局数据单点控制
class gTop(object):
    cfg = {
        GD_LOGGER: None,
        GD_CFG_IN: {},  # input config
        GD_JOB_ERROR: {},  # 错误信息,used for resume the job after error
        CFG_DB_MONITOR: None,  # 监控db dbmonitor
        CFG_DB_BUSINESS: None,  # 业务db
        GD_SUB: {},  # 如{"spider":gSpider,"trainer":gTrainer}
        GD_ACCOUNTINFO: None,  # 账号信息类,deprecated
    }

    @classmethod
    def set(cls, key, value):  # 设置
        cls.cfg[key] = value

    @classmethod
    def get(cls, key, defValue=None):  # 获取
        if key in (CFG_DB_MONITOR, CFG_DB_BUSINESS):
            if not gTop.cfg[key]:
                from superbase.utility.mysqlUtil import createDb
                createDb(key)
        return cls.cfg.get(key, defValue)

    @classmethod
    def release(cls):
        """
        全局资源释放
        :return:
        """
        for dbName in (CFG_DB_MONITOR, CFG_DB_BUSINESS):
            db = gTop.cfg[dbName]  # 这里不要用get
            if db:
                db.close()
        for name, subGdata in gTop.get(GD_SUB).items():
            subGdata.release()
