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
# -------------------------------------------------------------------------------
import sys
from spiderx.common.baseCrawler import BaseCrawler
from spiderx.common.utility.resultUtil import BaseResultHandler, HourlySaveLocalHandler, FileSaveHandler
from superbase.utility.logUtil import logInfo
from spiderx.common.utility.urlUtil import PageUrlOffer
from superbase.constant import CFG_JOB_BATCH, CFG_JOB_NAME, ET_RUN_BLOCK, CFG_JOB_EMAIL, CFG_JOB_ENABLE, CFG_JOB_ID
from spiderx.common.constant import TAG_LIST_PAGE_PATTERN, TAG_LIST_ITEMS, TAG_LIST_TOTAL_PAGE_NUM, TAG_LIST_NO_RESULT, \
    CFG_HTTP_ENCODING, CFG_DOWN_MAXPAGENUM, CFG_DOWN_MAXNUM, CFG_DOWN_ACCOUNTID, BLOCKED_ELEMENT, CFG_BLOCK_MAXCHECK, \
    CFG_AB_STRATEGY, CFG_DOWN_INDEX, CFG_HTTP_OUTFORMAT, CFG_DOWN_SYNCINTERVAL, CFG_DOWN_ROOT
from superbase.globalData import gConfig, PROJECT_ROOT
from superbase.utility.ioUtil import printDict
from spiderx.common.utility.parseUtil import CssElement, RegexHandler, NextLevelHandler, SpecialHandler, Extractor, \
    ListElement, EmmbedElement
from superbase.utility.processUtil import callMethod


class SampleSpider(BaseCrawler):
    """
    0,理解下载(httpUtil.py)与解析(parseUtil.py)
    1,熟悉pyquery 语法 http://pyquery.readthedocs.io/en/latest/
    2,调试detailTest,listTest,antiBlockTest1,antiBlockTest2
    3,熟悉
    """

    def __init__(self, params=None):
        # 添加这两个配置只是为了调试方便
        myCfg = {
            CFG_JOB_BATCH: "sample_test20140717",
            CFG_JOB_NAME: "sample",
        }
        BaseCrawler.__init__(self, params, myCfg)

    def detailTest(self):
        """
        熟悉灵活的配置，包括深层嵌套
        http://www.jobui.com/company/10375749/
        :return:
        """

        # 全局爬虫参数：实际项目中，建议在__init__中设置
        gConfig.set(CFG_DOWN_INDEX, "www_jobui_com/company")  # 索引需要统一设计，多级，便于存储检索


        url = "http://www.jobui.com/company/10375749/"
        company_detail_conf = {
            "logo": CssElement("div.company-logo > a > img", "src"),
            "name": CssElement("#companyH1 > a"),
            "type": CssElement("#cmp-intro > div > div > dl > dd:nth-child(2)"),
            "industry": CssElement("#cmp-intro > div > div > dl > dd.comInd > a"),
            "short_name": CssElement("#cmp-intro > div > div > dl > dd.gray3"),
            "info": CssElement("#textShowMore",text=False),
            # 这是重点关注的，listElement
            "rank": ListElement("div.swf-contA > ul.swf-gnfo > li", itemCssElement={
                "name": CssElement("dfn"),
                "stars": CssElement(".f60"),
            }),
            # 这是重点关注的，EmmbedElement
            "others": EmmbedElement(embedDict={
                "address": CssElement("div.cfix > div.s-wrapper > dl.fs16 > dd:nth-child(2)"),
                "website": CssElement("div.cfix > div.s-wrapper > dl.fs16 > dd:nth-child(4)")
            }),

        }

        self.downOneDetail(url, company_detail_conf, HourlySaveLocalHandler())

        # 任务完成 需要调用JobDone
        self.jobDone()

    def listTest(self):
        """
        这里演示的是下载51job上海地区互联网，软件开发工程师列表，以及详细公司信息
        url pattern:jobarea=020000&funtype=0100&industrytype=32&curr_page=1
        http://search.51job.com/jobsearch/search_result.php?fromJs=1&jobarea=020000&funtype=0100&industrytype=32&keywordtype=2&lang=c&stype=2&postchannel=0000&fromType=1&confirmdate=9
        :return:
        """

        # 全局爬虫参数：实际项目中，建议在__init__中设置
        gConfig.set(CFG_HTTP_ENCODING, "gbk")
        gConfig.set(CFG_DOWN_MAXPAGENUM, 2)  # test
        gConfig.set(CFG_DOWN_MAXNUM, 10)  # test
        gConfig.set(CFG_DOWN_ROOT,PROJECT_ROOT)
        gConfig.set(CFG_DOWN_INDEX, "www_51job_com/jd/shanghai/internet/se")  # 索引需要统一设计，多级，便于存储检索

        # 列表配置，有固定的几个key
        JdListConf = {
            TAG_LIST_PAGE_PATTERN: "&curr_page=",
            TAG_LIST_ITEMS: "#resultList>div.el",
            TAG_LIST_NO_RESULT: u"对不起，没有该查询结果",  # 可以不需要
            TAG_LIST_TOTAL_PAGE_NUM: CssElement("#resultList > div.dw_tlc > div:nth-child(5)",
                                                handler=RegexHandler(r".*?/(.*)")),
        }
        # http://jobs.51job.com/all/co3197016.html
        # 公司详情页配置
        companyDetailConf = {
            "id": CssElement("#hidCOID", "value"),  # 直接取属性
            "name": CssElement("div.tHeader.tHCop > div > h1"),  # 直接取元素
            "key_attr": CssElement("div.tHeader.tHCop > div > p.ltype"),  # 直接取元素
            "info": CssElement("div.tCompany_full > div.tBorderTop_box.bt > div > div > div.in > p"),  # 直接取元素
            "address": CssElement("div.tCompany_full > div:nth-child(2) > div > p",
                                  handler=RegexHandler(ur"公司地址：(.*)")),  # 用正则取值
        }

        def normalizeSalary(parent, css, attr, callBackParams):
            """
            这里做了一步清洗，只是为了做specialHandler的说明
            specialHanlder主要是为了处理难提取的内容
            :param parent:
            :param css:
            :param attr:
            :param callBackParams:
            :return:
            """
            value = Extractor.getValue(parent, css, attr)
            # todo, 将价格归一化成月薪数字
            print(callBackParams)
            return value

        def getCompanyDetail(url, conf, callBackParams):
            return self.downOne(url, conf)

        JdItemConf = {
            "title": CssElement(".t1"),  # 直接取元素
            "title_url": CssElement(".t1>span>a", "href"),  # 直接取属性
            "company": CssElement(".t2", handler=RegexHandler(ur"(.*)有限公司")),  # 用正则取值
            "company_url": CssElement(".t2>a", "href", handler=NextLevelHandler(companyDetailConf, getCompanyDetail,
                                                                                {"key": "testNextLevelHandler"})),
            # 处理下一级url
            "salay": CssElement(".t4", handler=SpecialHandler(normalizeSalary, {"key": "testSpecialHandler"})),  # 对
            "issue_date": CssElement(".t5"),  # 直接取元素
        }
        beginUrl = "http://search.51job.com/jobsearch/search_result.php?fromJs=1&jobarea=020000&funtype=0100&industrytype=32&keywordtype=2&lang=c&stype=2&postchannel=0000&fromType=1&confirmdate=9"
        urlMgr = PageUrlOffer(self.http, self.parser, beginUrl)
        self.downLists(JdListConf, JdItemConf, HourlySaveLocalHandler, urlMgr)
        # 任务完成 需要调用JobDone
        self.jobDone()

    def antiBlockTest1(self):
        """
        反屏蔽测试,直接屏蔽信息测试
        爬jobui,如果太频繁,有可能弹出登陆界面,或者显示反扒信息
        :return:
        """
        from spiderx.common.utility.antiBlockUtil import AntiBlockStrategy

        strategy = gConfig.get(CFG_AB_STRATEGY) if gConfig.get(CFG_AB_STRATEGY,None) else "postpone 2;changeAccount"
        #changeAccount 必须自己实现
        class MyAntiBlockStrategy(AntiBlockStrategy):
            def __init__(self,strategy):
                AntiBlockStrategy.__init__(self,strategy)
            def changeAccount(self):
                logInfo("implement:change account,relogin,...")

        antiBlocksConf = [
            # 测试直接屏蔽,这里只是做测试,所以blockInfo 是用正常的信息
            {
                # url 标示key,*表示任何页面
                'key': ['*'],
                # 可以有多个info，每个info对应一个strategy
                'blockInfo': [
                    {
                        "info":u"We're sorry but the page",
                        "strategy": MyAntiBlockStrategy(strategy)
                    }
                    ],

            },
        ]

        # setp 1,添加antiBlock配置
        self.addAntiBlock(antiBlocksConf)
        # 这个conf只是随便设置
        conf1 = {
            "logo": CssElement("div.company-logo > a > img", "src"),
            "name": CssElement("#companyH1 > a")
        }

        # 测试直接屏蔽,出错后会自动重启session,默认重试10次
        self.downOne("http://www.jobui.com/cmp?keyword=%E4%B8%8A%E6%B5%B7%E4%BF%A1%E7%A4%BC", conf1)
        if self.antiBlock.isNeedExit():
            logInfo(u"重试10次失败 \n 无法继续处理，人工干预")
            return


    def antiBlockTest2(self):
        """
        爬jobui,拿不到正确信息，超过MAX,可能是页面结构变化，或者是被blocked
        :return:
        """
        from spiderx.common.utility.antiBlockUtil import AntiBlockStrategy
        gConfig.set(CFG_JOB_EMAIL,"hbfhero@163.com")#这是测试使用，线上环境无需设置
        strategy = gConfig.get(CFG_AB_STRATEGY) if gConfig.get(CFG_AB_STRATEGY,None) else "postpone 10"

        class MyAntiBlockStrategy(AntiBlockStrategy):
            def __init__(self,strategy):
                AntiBlockStrategy.__init__(self,strategy)
            def changeAccount(self):
                logInfo("blockElement strategy:change account,relogin,...")
        antiBlocksConf = [

            # 测试blockElement,结果是报告页面情况有问题,blockElement
            {
                'key': ['jobui.com/company/'],  #
                'blockInfo': None,  # 如果有该info，直接使用
                # 不确定有blockInfo时，用element判断,可以取多个element，但blockElement 出错也有可能是页面结构变化
                #strategy可选
                "blockElement":{
                    "elements":[({
                                     "name": CssElement("#navTab > a:first", None, None)},
                                 u"公司介绍",#元素文本内容
                                ),
                    ],
                    "strategy":MyAntiBlockStrategy(strategy),#strategy可以为空，默认处理直接返回退出爬虫
                    "maxCheckNum":2,#重复出现2次就认为block
                },

            },
        ]

        # setp 1
        self.addAntiBlock(antiBlocksConf)
        # 这个conf只是随便设置
        conf1 = company_detail_conf = {
            "logo": CssElement("div.company-logo > a > img", "src"),
            "name": CssElement("#companyH1 > a")
        }

        # step 2 测试blockElement,结果是报告页面情况有问题
        for i in range(gConfig.get(CFG_BLOCK_MAXCHECK) + 4):  #
            self.downOne("http://www.jobui.com/company/13132726/", conf1)
            err = self.checkDownStatus(i+1)
            if err:
                logInfo("sampletest:bocked,exit!")
                break
        if self.antiBlock.isNeedExit():
            logInfo(u"block by element,pls check the content to identify the block info! \n 无法继续处理，人工干预")
            return

    def failTest(self, str, str2):
        """
        测试任务失败case
        :param str:
        :param str2:
        :return:
        """
        logInfo("this is a fail test: %s %s" % (str, str2))
        self.jobFail()

    def downFile(self):
        """
        down file,
        :return:
        """
        gConfig.set(CFG_DOWN_INDEX, "pinyin_sogou_com/dict/437")  # 索引需要统一设计，多级，便于存储检索
        gConfig.set(CFG_HTTP_OUTFORMAT, "file")  #
        url = "http://download.pinyin.sogou.com/dict/download_cell.php?id=20614&name=dota%20DOTA%E3%80%90%E5%AE%98%E6%96%B9%E6%8E%A8%E8%8D%90%E3%80%91"
        self.downOneDetail(url,None,FileSaveHandler(fileName="dota"))

if __name__ == '__main__':
    callMethod(SampleSpider, sys.argv[1:])
