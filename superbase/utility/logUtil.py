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
import logging
import logging.config
import os, sys
import traceback
from superbase.constant import EXCEPTION_PREFIX, CFG_LOG_CONSOLE_LEVEL, CFG_LOG_FILE_LEVEL, CFG_JOB_BATCH, \
    CFG_JOB_NAME, CFG_LOG_FILE_NAME, CFG_DB_MONITOR, GD_LOGGER, INVALID_BATCHID, INVALID_JOB_NAME, CFG_JOB_ID, \
    LOG_ALWAYS

# 日志记录器
SMILE_LOGGER = "smileGo"
# 基本配置
BASIC_SETTINGS = {
    'version': 1,  # 版本
    'handlers': {  # 处理程序
        'console': {  # 控制台
            'class': 'logging.StreamHandler',  # 日志流处理程序
            'level': 'INFO',
            'formatter': 'simple',
            'stream': 'ext://sys.stdout',
        },
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': 'INFO',
            'formatter': 'detailed',
            'filename': 'log/spiderx.log',
            'mode': 'a',
            'maxBytes': 10485760,
            'backupCount': 2,
        },

    },
    'formatters': {  # 格式化程序
        'simple': {
            'format': '%(asctime)s  %(message)s',
        },
        'detailed': {  # 详细的
            'format': '%(asctime)s  ' \
                      '%(levelname)-2s %(message)s',
        },
        'email': {  # 电子邮件
            'format': 'Timestamp: %(asctime)s\nModule: %(module)s\n' \
                      'Line: %(lineno)d\nMessage: %(message)s',
        },
    },
    'loggers': {  # 日志记录器
        SMILE_LOGGER: {
            'level': 'DEBUG',
            'handlers': ['console', 'file']
        },
    }
}

# 输入参数控制
IN_PARAMS_KEY = {
    CFG_LOG_CONSOLE_LEVEL: ("handlers", "console", "level"),
    CFG_LOG_FILE_LEVEL: ("handlers", "file", "level"),
}

LOG_EXIST = 2
LOG_NOTEXIST = 3
class logAdaper(logging.LoggerAdapter):

    def __init__(self, logger, extra={}):
        """
        继承logging.LoggerAdapter初始化参数
        :param logger: log对象
        :param extra:
        """
        logging.LoggerAdapter.__init__(self, logger, extra)
        self.exceptions=[]
    def info(self, msg, *args, **kwargs):
        """
        将日志打印到日志文件中
        :param msg: 日志所有数据
        :param args:
        :param kwargs:
        :return:
        """
        self.logger.info(msg, *args, **kwargs)

    def debug(self, msg, *args, **kwargs):
        # 将调试日志打印到日志文件中
        self.logger.debug(msg, *args, **kwargs)

    def warn(self, msg, *args, **kwargs):
        # 将警告日志打印到日志文件中
        self.logger.warn(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        # 将错误日志打印到日志文件中
        # print("may do more control")
        self.logger.error(msg, *args, **kwargs)


    def logDB(self, jobName, batch, msg,id):
        """
        保存断点和增量的数据库的日志
        :param jobName: 项目的名字
        :param batch:
        :param msg: db日志信息
        :return:
        """

        TABLE = "exceptions"
        from superbase.globalData import gTop
        from superbase.utility.timeUtil import getTimestamp
        # 获取时间节点
        curTime = getTimestamp()
        db = gTop.get(CFG_DB_MONITOR)
        if id and db and db.conn:
            if not db.getOne("select eid from exceptions where eid='%s'" % id):
                params = {
                    'eid': id,
                    'jobName': jobName,
                    'batch': batch,
                    'info': msg,
                    'inTime': curTime
                }
                db.insert(TABLE, params)
                return LOG_NOTEXIST
            else:
                return LOG_EXIST
        return 0

    def critical(self, msg, *args, **kwargs):
        try:
            from superbase.globalData import gConfig
            name = gConfig.get(CFG_JOB_NAME)
            batch = gConfig.get(CFG_JOB_BATCH)
            from superbase.utility.timeUtil import getTimestamp
            curTime = getTimestamp()
            def _createId():
                lines = msg.split("\n")
                lines.reverse()
                for line in lines:
                    if line.find("!myex!File ") >= 0 and line.find(" line ") > 0:
                        return hash("%s_%s_%s_%s" % (curTime[:8], line,name,batch))  # or use hash_lib.md5
                return None
            id = _createId()
            if id and id not in self.exceptions:
                #用db来保证多进程下log唯一性
                ret = self.logDB(name, batch, msg,id)
                if ret!=LOG_EXIST or msg.find(LOG_ALWAYS)>=0:
                    self.logger.critical("{name} {batch}  {msg}".format(name=name, batch=batch, msg=msg), *args, **kwargs)
                self.exceptions.append(id)
            elif gConfig.get("env")=="DEV":
                print("dev-debug:%s"%msg)
        except ImportError, e1:
            print("%s/n/%s"%(e1,sys.path))
        except Exception, e:
            #logException()防止异常递归,只print
            print("error in logCritical%s"%e)



    def exception(self, msg, *args, **kwargs):
        from superbase.globalData import gConfig
        excInfo = "\n".join(map(lambda x: "{prefix}{excepInfo}".format(
            prefix=EXCEPTION_PREFIX, excepInfo=x.strip()), traceback.format_exc().split("\n")))
        self.critical("%s--\n%s" % (msg, excInfo), *args, **kwargs)


def createLogger(cfg, forceNew=False):
    if cfg.get(CFG_JOB_BATCH) != INVALID_BATCHID and cfg.get(CFG_JOB_NAME) != INVALID_JOB_NAME:
        cfg.set(CFG_LOG_FILE_NAME, "%s/%s.txt" % (cfg.get(CFG_JOB_BATCH), cfg.get(CFG_JOB_NAME)))
        MyLogger.getLogger(cfg, forceNew)


class MyLogger(object):
    @staticmethod
    def getLogger(cfg, forceNew=False):

        from superbase.globalData import gTop
        if not gTop.get(GD_LOGGER) or forceNew:  # singleton or force a new logger

            from superbase.globalData import gConfig
            from superbase.globalData import PROJECT_ROOT
            from superbase.utility.ioUtil import getPrintDict,mkdir


            logDir = os.path.join(PROJECT_ROOT, "log")
            mkdir(logDir)

            for key, value in cfg.items():
                if key in IN_PARAMS_KEY:
                    L1, L2, L3 = IN_PARAMS_KEY[key]
                    BASIC_SETTINGS[L1][L2][L3] = value
                elif key == CFG_LOG_FILE_NAME:
                    logFileName = os.path.join(logDir, value)
                    dir = os.path.split(logFileName)[0]
                    mkdir(dir)
            BASIC_SETTINGS["handlers"]["file"]["filename"] = logFileName

            logging.config.dictConfig(BASIC_SETTINGS)
            logger = logging.getLogger(SMILE_LOGGER)
            logger2 = logAdaper(logger)
            gTop.set(GD_LOGGER,logger2)  # logger#
            hint = "current code root %s\n--config is--\n%s" % (PROJECT_ROOT, getPrintDict(gConfig.cfg))
            if gConfig.get(CFG_JOB_ID,0)>0:
                logger2.info(hint)
            else:
                logger2.debug(hint)

        return gTop.get(GD_LOGGER)


def _log(level, str, obj=None):
    # 输出 日志
    try:
        from superbase.globalData import gConfig
        from superbase.utility.processUtil import applyFunc
        log = MyLogger.getLogger(gConfig)
        applyFunc(log, level, [str])
    except Exception:
        print ("%s\n%s"%(str,traceback.format_exc()))


def logException(str="", obj=None):
    # 日志打印 异常
    _log("exception", "!!e%s" % str, obj)


def logCritical(str="", obj=None):
    _log("critical", str, obj)


def logError(str="", obj=None):
    _log("error", str, obj)


def logWarn(str="", obj=None):
    # 日志打印 警告
    _log("warn", str, obj)


def logInfo(str="", obj=None):
    # 日志打印 信息
    _log("info", str, obj)


def logDebug(str="", obj=None):
    # 日志打印 debug
    _log("debug", str, obj)


def getLogFileName(batch, jobName):
    """
    获取日志文件的名字
    :param batch:
    :param jobName:
    :return: jobLog and nodeLog
    """
    from superbase.globalData import PROJECT_ROOT
    from superbase.globalData import gConfig
    # PROJECT_ROOT 项目根
    logDir = PROJECT_ROOT + "log/"
    jobLog = None
    nodeLog = None
    if batch:
        # 工作日志 拼接 路径
        jobLog = os.path.join(logDir, "%s/%s.txt" % (batch, jobName))
        # 节点日志
    nodeLog = os.path.join(logDir, gConfig.get(CFG_LOG_FILE_NAME))
    return jobLog, nodeLog
