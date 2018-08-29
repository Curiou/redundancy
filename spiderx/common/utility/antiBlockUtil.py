# -*- coding:utf-8 -*-
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

from spiderx.common.constant import CFG_BLOCK_MAXCHECK, BLOCKED_ELEMENT, BLOCKED_INFO, CFG_HTTP_INTERVAL, \
    CFG_HTTP_ENCODING, \
    CFG_DOWN_ACCOUNTID, BLOCKED_EXIT, CFG_AB_MAXRETRY
from superbase.constant import CFG_JOB_NAME, CFG_JOB_BATCH, \
    CFG_LOG_FILE_NAME, CFG_JOB_ENABLE, CFG_JOB_EMAIL, CFG_JOB_BEGINTIME, CFG_JOB_CHANGENODE, GD_JOB_ERROR
from superbase.globalData import gConfig, gTop, PROJECT_ROOT
from superbase.utility.ioUtil import getPrintDict
from superbase.utility.logUtil import logError, logInfo, logCritical
from superbase.utility.mailUtil import Mail
from superbase.utility.processUtil import applyFunc
from superbase.utility.timeUtil import getTimestamp, getTimestampBySec


class AntiBlock(object):
    """
   #针对某些url,check是否被block,也同时用于页面结构是否改变
antiBlocks=(
                {
                    'key':['cmp?keyword='],#url 标示key
                    'blockInfo':None,#如果有该info，直接使用
                    "blockElement":(
                                    ({"name":CssElement("body > ul > li:nth-child(1)")},u"公司名字"),#不确定有blockInfo时，用element判断,

                                   ),
                    "strategy":AntiBlockStategy(strategy="_postpone 3600,_changeAccount,_incInterval 2"),#默认策略实现类
                },
                {
                    'key':['jobui.com/company/'],#
                    'blockInfo':None,#如果有该info，直接使用
                    "blockElement":(({"name":("#navTab > a:first",None,None)},u"公司介绍"),),#不确定有blockInfo时，用element判断,
                    "strategy":MyAntiBlockStategy(strategy="_postpone 3600,_changeAccount),#自定义策略实现,子类,通常是
                },
               )


            爬虫是个循环,碰到block错误,基本策略是
            1,报告错误
            2,根据strategy预处理好下一次运行的配置,如切换账号,增加爬虫间隔
            3,set jobStatus = Error,set new beginTime, and update cfg
            4,将错误处理交给上层:
                4.1 交给worker,set errorHandler:worker
                    worker checkError and run it when beginTime reached
                4.2 交给foreman,set errorHandler:foreman
                    worker checkError and reAssign  it
    """

    def __init__(self, antiBlockConf, extractor):
        self.antiBlocks = antiBlockConf
        self.extractor = extractor
        self.http = extractor.http
        self.blocked = 0
        self.blockCheck = 0
        self.needExit = 0
        self.curUrl = None
        self.retryNum = 0

    def getAntiBlockConf(self, url):
        result = []
        for item in self.antiBlocks:
            key = item['key']
            for key1 in key:
                if url.find(key1) >= 0 or key1 == "*":
                    result.append(item)

        return result

    def checkBlock(self, url, content):
        """
        一个页面可以配置多个conf，对应不同的strategy
        :param url:
        :param content:
        :return:
        """
        antiBlocks = self.getAntiBlockConf(url)
        for antiBlock in antiBlocks:
            stragegy = self.doCheckBlock(url, content, antiBlock)
            if self.blocked:
                return self.blocked, stragegy
            else:
                self.retryNum = 0
        return 0,None

    def retry(self):
        self.blocked = 0
        self.retryNum += 1
        logInfo("blocked!!--retry-%s"%self.retryNum)
        if self.retryNum>gConfig.get(CFG_AB_MAXRETRY,2):
            logCritical("retry antiblcok Fail")
            self.retryNum = 0
            self.setExit()

    def isBlocked(self):
        return self.blocked

    def setExit(self):
        """
        当前爬虫无法处理，如需要切换账号，IP等，需要退出
        :return:
        """
        self.needExit = BLOCKED_EXIT

    def isNeedExit(self):
        return BLOCKED_EXIT if self.needExit  else 0

    def handleBlock(self, url, content, handleStrategy, downInfo):
        """

        :param url:
        :param content:
        :param handleStrategy:
        :param downInfo: downNum,downTime,downInterval etc.
        :return:
        """
        self.curUrl = url
        if self.blocked:
            if handleStrategy:
                handleStrategy.handle(self)
            else:
                self.alarmPageError(url, content, downInfo)
                self.setExit()

    def doCheckBlock(self, url, content, antiBlock):
        blockInfo = antiBlock.get("blockInfo", None)
        if blockInfo:

            for b1 in blockInfo:
                info = b1["info"] #兼容
                self.blocked = BLOCKED_INFO if content.find(info) > 0 else 0
                if self.blocked:
                    logError("!!!!block by %s,url=%s" % (gConfig.get(CFG_JOB_NAME), url))
                    return b1["strategy"]

        # check the elements

        blocked = False
        element = antiBlock.get("blockElement",None)
        if element:

            strategy = element.get("strategy",None)
            elements = element["elements"]
            for template, value in elements:
                result = {}
                self.extractor.getResultByContent(content, template, result)
                checkName = result.get("name", None)
                if not checkName or (value and checkName.find(value) == -1):
                    blocked = True
                else:
                    blocked = False
                    break  # 非block马上跳出
            if blocked:
                self.blockCheck += 1
                logError("%s:the element not exist,block?%s" % (self.blockCheck,url))
            else:
                self.blockCheck = 0  # reset

            globalCheckNum = gConfig.get(CFG_BLOCK_MAXCHECK,30)
            localCheckNum = element.get("maxCheckNum",globalCheckNum) #如果有local，用local

            if self.blockCheck > localCheckNum:
                logError("block by element,pls check the content,maybe the structure has changed!")
                self.blocked = BLOCKED_ELEMENT
                self.blockCheck = 0
                return strategy

    def alarmPageError(self, url, content, downInfo):
        """
        解析元素有错,有可能是blocked 也有可能是页面结构变化,邮件警告,人工检查
        :param url:
        :param content:
        :param downInfo:downNum,downTime,downInterval etc.
        :return:
        """
        fname, filePath = AntiBlock.saveWrongPage(content)
        info = {
            'jobName': gConfig.get(CFG_JOB_NAME),
            'batch': gConfig.get(CFG_JOB_BATCH),
            'url': url,
            'filePath': filePath,
            'type': self.blocked,
            'detail': json.dumps(downInfo),
            'inTime': getTimestamp(),
        }
        title = "block-%s" % self.blocked
        content = getPrintDict(info)
        attach = [(fname, filePath)]
        emails2 = [gConfig.get(CFG_JOB_EMAIL)] if gConfig.get(CFG_JOB_EMAIL, None) else []
        if gConfig.get(CFG_JOB_ENABLE, 0):
            gTop.get('db').insert("block", info)
            from jobManager.job import Job
            Job().sendEmail(
                title=title,
                content=content,
                attach=attach,
                emails2=emails2
            )
        else:
            Mail.sendEmail(
                title=title,
                content=content,
                t_address=emails2,
                attaches=attach
            )
        logError("blocked?check the content\n%s" % getPrintDict(info))

    @staticmethod
    def saveWrongPage(content2, htmlFile=None):
        import random
        if not htmlFile:
            htmlFile = gConfig.get(CFG_LOG_FILE_NAME).replace(".txt", "%s.html" % (random.randint(100, 999)))
            htmlFile = os.path.join(PROJECT_ROOT + "log/", htmlFile)
        fname = os.path.split(htmlFile)[1]
        import codecs
        with codecs.open(htmlFile, 'wb', gConfig.get(CFG_HTTP_ENCODING, "utf-8")) as f:
            f.write(content2)
        logInfo("saveWrongPage:%s" % htmlFile)
        return fname, htmlFile


class AntiBlockStrategy(object):
    """
    反block策略集合,可以通过gConfig配置策略,策略可以作为一个策略组合,用分号隔开
    如ab.strategy="postpone 3600;changeAccount 1"
    当被block后,
        该batch的任务会被挂起,
        参数重新设置 by gData.get(GD_JOB_ERROR)
        等待重新调度 by worker or foreman
    """

    def __init__(self, strategy,exit=False):
        """
        :param strategy: 策略可以作为一个策略组合,用分号隔开如"postpone 3600;changeAccount 1"
        :param exit: 是否退出当前爬虫，如果需要重新登录，切换账号等，需要设为True
        :return:
        """
        self.handlers = [filter(lambda x: len(x), handler.strip().split(" ")) for handler in strategy.split(";")]
        self.exit = True
    def handle(self,parent):
        """

        :param http: httpAgent instance
        :return:
        """
        self.parent = parent
        self.http = parent.http
        for handler in self.handlers:
            if handler:
                applyFunc(obj=self, strFunc=handler[0], arrArgs=handler[1:])

    def postpone(self, seconds):
        """
        最简单,推后处理,默认postpone也会换IP
        :param seconds:
        :return:
        """
        logInfo("AntiBlock:postpone %s seconds" % seconds)
        #gTop.get(GD_JOB_ERROR)[CFG_JOB_BEGINTIME] = getTimestampBySec(time.time() + int(seconds))
        self.http.setSessionTime(time.time() + int(seconds))

    def changeInterval(self, interval):
        """
        :param interval: format 0.1 or 0.1-10
        :return:
        """
        logInfo("AntiBlock:changeInterval %s" % interval)
        self.http.setDownInterval(interval)

    def changeAccount(self):
        """
        切换账号,这个函数的实现需要子类
        :param accountId: 为空标示随机取下一个
        :return:
        """
        raise Exception("AntiBlock:changeAccount,neet override it!")

    def changeIP(self):
        """
        :return:
        """
        logInfo("AntiBlock:change IP")
        self.http.newProxy()

    def changeNode(self):
        """
        :return:
        """
        logInfo("AntiBlock:change node")
        gTop.get(GD_JOB_ERROR)[CFG_JOB_CHANGENODE] = 1.
        self.parent.setExit()