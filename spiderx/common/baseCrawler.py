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
import time
from StringIO import StringIO

from lxml import etree
from pyquery import PyQuery as pq

from spiderx.common.constant import CFG_HTTP_INTERVAL, CFG_HTTP_TIMEOUT, CFG_HTTP_OUTFORMAT, CFG_HTTP_ENCODING, \
    CFG_HTTP_UNESCAPE, CFG_HTTP_ENGINE, CFG_HTTP_UA, CFG_HTTP_BROWSER, CFG_HTTP_MAXREQUEST, CFG_DOWN_MAXNUM, \
    CFG_DOWN_MAXPAGENUM, CFG_BLOCK_MAXCHECK, CFG_AB_STRATEGY, ERR_BLOCK, ERR_MAXNUM, \
    ERR_TIMEOUT, TAG_LIST_ITEMS, CFG_DEBUG_PROGRESS, CFG_RESULT_FUNC, GD_HTTP_ENGINE, CFG_DOWN_ROOT, CFG_DOWN_WEBSITE, \
    CFG_HTTP_BROWSERMODE, BROWSER_TPE_PHANTOMJS
from spiderx.common.utility.antiBlockUtil import AntiBlock
from spiderx.common.utility.httpUtil import RequestsAgent, SeleniumAgent
from spiderx.common.utility.parseUtil import Extractor
from spiderx.common.utility.resultUtil import SyncPoint
from spiderx.common.utility.spiderJobUtil import SpiderJobUtil
from superbase.baseClass import BaseClass
from superbase.constant import IS_ERROR, ET_RUN_UNKNOWN, CFG_JOB_ENABLE, CFG_DB_DISABLE, \
    CFG_JOB_BEGINTIME, CFG_JOB_RUNTIME, GD_SPIDER, GD_SUB, GD_ACCOUNTINFO, CFG_ACCOUNT_PROVIDER, CFG_JOB_HEARTBEAT
from superbase.globalData import gConfig, gTop, PROJECT_ROOT
from superbase.utility.ioUtil import getPrintDict, isLinux
from superbase.utility.jobUtil import JobUtil
from superbase.utility.logUtil import logInfo, logDebug, logException, logError
from superbase.utility.timeUtil import getTimestamp, ts2seconds, getCurTime


class BaseCrawler(BaseClass,SpiderJobUtil):
    """
    """

    def __init__(self, params, subConfig=None):
        self.basicConfig = {
            # http down related
            CFG_HTTP_INTERVAL: 0.01,  # 请求间隔
            CFG_HTTP_TIMEOUT: 10,  #
            CFG_HTTP_OUTFORMAT: 'html',  # json
            CFG_HTTP_ENCODING: 'utf-8',  # gbk
            CFG_HTTP_UNESCAPE: 0,  # remove special character quoting
            CFG_HTTP_ENGINE: 'requests',  # selenium
            CFG_HTTP_UA: 'windows',  # mac,ios,android
            CFG_HTTP_BROWSERMODE: 'headless',  #
            CFG_HTTP_BROWSER: BROWSER_TPE_PHANTOMJS,
            CFG_HTTP_MAXREQUEST: 0,  # 一个session 最多请求次数,0表示无限制,否则超过这个次数将重启session
            CFG_JOB_RUNTIME: 0,  # 爬虫运行时间，0无限制,单位是秒
            CFG_JOB_HEARTBEAT: 60,  # 任务心跳间隔,单位是秒
            CFG_DOWN_MAXNUM: 0,  # 一次爬虫最多下载数量，0无限制
            CFG_DOWN_MAXPAGENUM: 0,  # 一次爬虫最多下载页面数量，0无限制
            CFG_BLOCK_MAXCHECK: 100,  # 反block,元素检查,檢查次數默认>100次,就算blocked,也有可能是页面结构改变
            CFG_ACCOUNT_PROVIDER:"spideraccount.samanager",

        }

        if subConfig:
            # 如果subConfig有配置 则更新基本配置
            self.basicConfig.update(subConfig)
        # 把基本配置加入到全局配置中 并写入日志中
        BaseClass.__init__(self, params, self.basicConfig)
        #依赖配置一定要紧跟在BaseClass后
        if not gConfig.get(CFG_DOWN_ROOT,None):
            gConfig.set(CFG_DOWN_ROOT,"d:/" if gConfig.get("env") in ("ONLINE") and not isLinux() else PROJECT_ROOT)
        SpiderJobUtil.__init__(self)

        # 如果获取配置的引擎不是seleenium就用selenium请求，如果是就用requests请求
        self.http = RequestsAgent() if gConfig.get(CFG_HTTP_ENGINE, "requests") != "selenium" else SeleniumAgent()
        # 获取全局编码格式 并用lxml 解析
        self.parser = etree.HTMLParser(encoding=gConfig.get(CFG_HTTP_ENCODING))
        # 把请求的方式和用什么编码解析 放到 下载内容提取器
        self.extractor = Extractor(self.http, self.parser)
        self.antiBlock = None

        # 工作开始
        self.syncPoint = self.jobBegin()

    # 添加反爬机制
    def addAntiBlock(self, antiBlockConf):
        self.antiBlock = AntiBlock(antiBlockConf, self.extractor)
        self.http.setAntiBlock(self.antiBlock)

    def downLists(self, listConf, listItemConf, resultHandlerClass, urlMgr):
        """
        :param listConf: 列表配置
        :param listItemConf: 列表项配置
        :param resultHandlerClass: 结果处理类
        :param urlMgr: urlManager 提供url
        :return:
        """

        # 打印时间戳
        logInfo("%s_begin downLists" % (getTimestamp()))
        err = num = 0
        # url跳页 处理
        for url in urlMgr.pageUrls(listConf):
            try:
                # debug 打印url
                logDebug(url)
                # 中转 获取原网页的源码content 并交给 downOneList2 处理
                self.downOneList(url, listConf, listItemConf, resultHandlerClass())
                num += 1
                # 检查下载状态
                err = self.checkDownStatus(num)
                if IS_ERROR(err):  # 如果err<0 break
                    break
            except Exception:
                logException()
        return err

    def downOneList2(self, url, content, listConf, listItemConf, resultHandler):
        """
        downOneList 的具体实现
        :param url: 只是起到log作用
        :param content: 页面内容
        :param listConf: 列表配置
        :param listItemConf: 列表项配置
        :param resultHandler: 结果的handler
        :return: error:-1,ok:0
        """

        # pq(etree.parse())直接接受一个文档，按照文档结构解析
        # StringIO经常被用来作字符串的缓存，因为StringIO的一些接口和文件操作是一致的，
        # 同样的代码，可以同时当成文件操作或者StringIO操作。
        # getroot 获取原网页的根
        root = pq(etree.parse(StringIO(content), self.parser).getroot())
        # list 行数组的模式
        css = listConf[TAG_LIST_ITEMS]
        trs = root(css)

        if trs and len(trs) > 0:
            for idx, tr in enumerate(trs):  # enumerate 列举
                try:
                    result = {}
                    # 把提取的原网页内容 以行组模式 css选择器 为匹配方式 并以dict形式 保存到result中
                    self.extractor.getResult(pq(tr), listItemConf, result)
                    # debug输出
                    logDebug(getPrintDict(result))
                    # 输出 -->BaseResultHandler().handle(result)
                    resultHandler.handle(result)
                except Exception:
                    logException()
        else:
            # 没有这个lsits 并打印错误的url
            logError("%s !no lists" % url)
            return -1
        return 0

    def downOneList(self, url, listConf, listItemConf, resultHandler):
        """
        :param url: 列表页的url
        :param listConf: 列表配置
        :param listItemConf: 列表项配置
        :param resultHandler: 结果的handler
        :return: error:-1,ok:0
        """

        # 中转 获取原网页的源码content 并交给 downOneList2 处理
        content = self.http.get(url)
        return self.downOneList2(url, content, listConf, listItemConf, resultHandler)

    def downDetails(self, urls, detailConf, resultHandlerClass):
        """
        下载所有详情页
        :param urls: 详情页url list,固定3个字段：id，sourceId,url
        :param detailConf: 详情页配置
        :param resultHandlerClass: 结果处理类
        :return:
        """

        err = num = 0
        for id, sourceId, url in urls:
            try:
                result = {'id': id, 'sourceId': sourceId}
                # 下载 解析 并输出
                self.downOneDetail(url, detailConf, resultHandlerClass(result))
                num += 1
                err = self.checkDownStatus(num)  # 检查下载状态
                if IS_ERROR(err):  # 如果err<0 break
                    break
            except Exception:
                logException("url=%s" % (url))
                # self.http.newSession()
        logInfo("%s down-%s-num=%s,err=%s" % (getTimestamp(), type, num, err))
        return err

    def downOneDetail(self, url, detailConf, resultHandler):
        """
        :param url: 详情页的url
        :param detailConf:可以为空，如果返回是json模式
        :param resultHandler:
        :return:
        """
        # 把提取的原网页内容 以行组模式  css选择器 为匹配方式 并以dict形式 保存到result中
        result = self.downOne(url, detailConf)
        resultHandler.handle(result)

    def downOne(self, url, detailConf):
        """
        :param url:
        :param detailConf:可以为空如果返回是json模式
        :return:
        """
        result = {}
        # 把提取的原网页内容 以行组模式 css选择器 为匹配方式 并以dict形式 保存到result中
        self.extractor.getResultByUrl(url, detailConf, result)
        # debug 输出
        # logDebug(getPrintDict(result))
        return result

    def checkDownStatus(self, num):
        """
        #检查下载状态
        :param num:
        :return:
        """
        try:
            # 默认每隔2048个显示下载进度，更新任务心跳
            if num & (gConfig.get(CFG_DEBUG_PROGRESS, 2048) - 1) == 0:
                logInfo("%s_%s:down=%s" % (getTimestamp(), gConfig.get(CFG_DOWN_WEBSITE,"undefined website"), num))

            self.jobHearBeat()

            if self.http.isBlocked():
                return ERR_BLOCK

            # 下载最大数
            maxNum = gConfig.get(CFG_DOWN_MAXNUM, 0)
            if maxNum and num > maxNum:
                logError("!!reach the maxNum %s" % maxNum)
                return ERR_MAXNUM

            # 工作 运行时间
            if gConfig.get(CFG_JOB_RUNTIME,0) > 0:
                beginTime = int(gConfig.get(CFG_JOB_BEGINTIME))
                runTime = int(gConfig.get(CFG_JOB_RUNTIME))
                if time.time() - ts2seconds(beginTime) > runTime:
                    logInfo("begin=%s:exit for runTime=%s out" % (beginTime,runTime))
                    return ERR_TIMEOUT

        except Exception, e:
            logException()

        return 0



class gSpider(object):
    """
    #爬虫全局数据单点控制
    """
    cfg = {
        GD_HTTP_ENGINE: set(),
    }

    @staticmethod
    def add2Toptree():
        """
        #将自身作为子节点放入全局数据树
        :return:
        """
        if GD_SPIDER not in gTop.cfg[GD_SUB]:
            gTop.cfg[GD_SUB][GD_SPIDER] = gSpider

    @classmethod
    def set(cls, key, value):
        """
        设置全局数据树
        :return:
        """
        gSpider.add2Toptree()
        cls.cfg[key] = value

    @classmethod
    def get(cls, key, defValue=None):
        """
        获取全局数据树
        :return:
        """
        gSpider.add2Toptree()
        return cls.cfg.get(key, defValue)

    @classmethod
    def release(cls, key=None):
        """
        全局资源释放
        :return:
        """
        if len(gSpider.cfg[GD_HTTP_ENGINE]):
            for eng in gSpider.cfg[GD_HTTP_ENGINE]:
                eng.close()
            gSpider.cfg[GD_HTTP_ENGINE] = set()
            logInfo("release http engine")
