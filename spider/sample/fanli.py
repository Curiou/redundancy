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
import json
import os
import sys
import time
sys.path.append(".")
from selenium.webdriver.common.by import By

from spiderx.common.utility.crawlerUtil import findElement
from spiderx.common.utility.resultUtil import BaseResultHandler, HourlySaveLocalHandler
from superbase.utility.safeUtil import safeReg1
from superbase.utility.timeUtil import getTimestamp, getTimestampBySec, ts2seconds

from spiderx.common.constant import CFG_HTTP_ENGINE, TAG_LIST_ITEMS, CFG_DOWN_INDEX, CFG_HTTP_BROWSER, \
    CFG_HTTP_BROWSERMODE
from superbase.utility.mailUtil import Mail


from spiderx.common.baseCrawler import BaseCrawler
from superbase.utility.logUtil import logInfo, logException, logError, logDebug
from superbase.constant import CFG_JOB_BATCH, CFG_JOB_NAME, \
    CFG_JOB_DEBUG, CFG_LOG_FILE_NAME
from superbase.globalData import gConfig, PROJECT_ROOT
from superbase.utility.ioUtil import printDict, str2line, gzipOneFile
from spiderx.common.utility.parseUtil import CssElement, ListElement
from superbase.utility.processUtil import callMethod



class Fanli(BaseCrawler):
    """
    返利网下载淘宝优惠券
    演示目的:
    1,配置selenium+phantomjs
    2,afterLoad 处理，这里是拖动处理，动态加载
    """

    def __init__(self, params=None):
        # 添加前面两个配置只是为了调试方便
        myCfg = {
            CFG_JOB_BATCH: "fanli_test20140717",
            CFG_JOB_NAME: "fanli",
            # CFG_HTTP_ENCODING:"gbk",
            CFG_HTTP_ENGINE: "selenium",
            CFG_HTTP_BROWSER:"chrome",#selenium默认用phantomjs做browser
            CFG_HTTP_BROWSERMODE:"1",#selenium默认用phantomjs做browser
            CFG_DOWN_INDEX:"www_fanli_com/coupon"
        }
        BaseCrawler.__init__(self, params, myCfg)
        resultFile = gConfig.get(CFG_LOG_FILE_NAME).replace(".txt", "_result.txt")
        self.resultFile = os.path.join(PROJECT_ROOT + "log/", resultFile)
        self.file = open(self.resultFile, "w")
        import re
        self.urlPattern = re.compile(r"[url,go]=(http.*)")
        self.patterns = [
            {
                "url": re.compile(r"url=(http.*)"),
                "sellerId": re.compile(r"seller_?[I,i]d%3D(.*?)%"),
                "couponId": re.compile(r"activity_?[I,i]d%3D(.*?)[&,%]"),
                "productId": re.compile(r"Epid-(.*?)%"),
                "discount": re.compile(ur"满(.*?)减(.*)"),
            },
            {
                "url": re.compile(r"go=(http.*)"),
                # "sellerId": re.compile(r"[sellerId,seller_id]%3D(.*?)%"),
                "couponId": re.compile(r"activity_?[I,i]d%3D(.*?)[&,%]"),
                "productId": re.compile(r"itemId%3D(.*?)[&,%]"),
                "discount": re.compile(ur"(\d+)"),
            },
        ]

    def _enableScroll(self, length, num):
        from spiderx.common.utility.httpUtil import ScrollDownHandler
        self.http.setAfterLoad(ScrollDownHandler(intervalLength=length,
                                                 totalScroll=num))  # waitElement=(By.CSS_SELECTOR, "div.footer > div > ul.ft-nav")))

    def downCoupon(self, url):
        """
        有三种页面:
        1,旗舰店/专卖店:
            淘宝:http://super.fanli.com/brand-5311?spm=super_home.pc.bid-5311
            非淘宝:http://super.fanli.com/brand-32665?spm=super_home.pc.bid-32665
        2,品牌普通店集合:http://super.fanli.com/brand-1181?spm=super_home.pc.bid-1181
        3,http://super.fanli.com/brand-63591?pid=13276411402&spm=super_home.pc.pid-13276411402~bid-63591&lc=super_abtest_14071c
            
        :param url:
        :return:
        """
        self._enableScroll(length=8000, num=3)
        result = {}
        content = self.http.get(url)
        self.extractor.getResultByContent(content, {"groups": ListElement(
            "div.gather-wrap> div.container > div.gather-floor-content",
            itemCssElement={"url": CssElement("a.gather-link", "href")}
        )
        },
                                          result
                                          )
        if result["groups"]:  # mode2,brands
            for item in result["groups"]:
                logInfo("group mode:begin download %s" % item["url"])
                self.downCoupon(item["url"])
        else:
            self.extractor.getResultByContent(content, {"mode": CssElement("a.coupon-link>p.detail")}, result)
            pageMode = 1 if result["mode"] else 0
            def handleCoupon(result):
                        try:
                            patterns = self.patterns[pageMode]
                            if not result["url"]:
                                return logError("handleCoupon:url is null")
                            url = safeReg1(patterns["url"], result["url"], "url")
                            if "taobao.com" in url or "tmall.com" in url:
                                begin, end = result.get("time").split(",")
                                result2 = {
                                    "beginTime": getTimestampBySec(float(begin)),
                                    "endTime": getTimestampBySec(float(end)),
                                }
                                result2["info"] = result["val"]
                                result2["url"] = url

                                if pageMode == 0:
                                    result2["sellerId"] = safeReg1(patterns["sellerId"], result["url"], "sellerId")
                                    result2["couponId"] = safeReg1(patterns["couponId"], result["url"], "couponId")
                                    result2["productId"] = safeReg1(patterns["productId"], result["url"], "productId")
                                    result2["limit"], result2["discount"] = patterns["discount"].search(result["val"]).groups()
                                else:
                                    result2["couponId"] = safeReg1(patterns["couponId"], result["url"], "couponId")
                                    result2["productId"] = safeReg1(patterns["productId"], result["url"], "productId")
                                    result2["discount"] = safeReg1(patterns["discount"], result["url"], "discount")
                                    result2["sellerId"] = result2["limit"] = "",

                                return result2
                            else:
                                logError("the url is not taobao or tmall!")
                        except Exception:
                            logException(result.get("url", "no_url"))
            class CouponHandler(HourlySaveLocalHandler):
                def __init__(self):
                    HourlySaveLocalHandler.__init__(self)
                def preProcess(self,result):
                    result = HourlySaveLocalHandler.preProcess(self,result)
                    return handleCoupon(result)

            if pageMode == 1:  # 非正常模式
                listItemConf = {
                    "time": CssElement(None, "data-time"),
                    "url": CssElement("a.coupon-link", "href"),
                    "val": CssElement("a.coupon-link>p.coupon"),
                }
            else:
                listItemConf = {
                    "time": CssElement(None, "data-time"),
                    "url": CssElement("a.item-coupon", "href"),
                    "val": CssElement("a.item-coupon"),
                }

            self.downOneList2(url, content, listConf={TAG_LIST_ITEMS: "[class*=J_super_item]"},
                              listItemConf=listItemConf,
                              resultHandler=CouponHandler()
                              )

    def down(self):
        """
        1,下载brands list
        2，处理 item with coupon
        要点：
            页面简单，scroll down到底部，取全部list item,
        :return:
        """

        num = gConfig.get("fanli.scrollNum", 20)  # 全量可以设置高点,如60
        self._enableScroll(4000, num)
        url = "http://super.fanli.com/?spm=global.pc.buid-super#J_brand_area"
        brands = []

        def handleList(result):
            if result.get("coupon", None):
                if result.get("isNew", None):  # is new brand with coupon
                    brands.append(result)

        # J_today_brand_wrap > div:nth-child(1) > div:nth-child(1) > i
        self.downOneList(url, listConf={TAG_LIST_ITEMS: "#J_today_brand_wrap > div"},
                         listItemConf={
                             "brandId": CssElement("div:nth-child(1) > a", "data-id"),
                             "url": CssElement("div:nth-child(1) > a", "href"),
                             "isNew": CssElement("div:nth-child(1) > i.i-new"),
                             "brandName": CssElement("div.info>img.logo", "alt"),
                             "coupon": CssElement("div.info > div > div.coupon"),
                             "time": CssElement(None, "data-time")
                         },
                         resultHandler=BaseResultHandler(handleFunc=handleList))
        total = len(brands)
        for num, brand in enumerate(brands):
            logInfo("%s/%s down-%s,%s" % (num + 1, total, brand["brandName"], brand["coupon"]))
            self.downCoupon(brand['url'])

        self.jobDone()

    def updateResume(self):
        """
        python spider/sample/fanli.py update.time=600 updateResume
        :return:
        """
        while True:
            self.http.get("https://www.liepin.com/sh/")
            driver = self.http.driver
            driver.find_element_by_xpath('//*[@id="home"]/div[2]/div[1]/div/div/section[2]/form/div[3]/p/a').click()
            time.sleep(5)
            driver.find_element_by_xpath('//*[@id="home"]/div[2]/div[1]/div/div/section[1]/div[1]/form/div[1]/input').send_keys("hbfhero@sina.com")
            driver.find_element_by_xpath('//*[@id="home"]/div[2]/div[1]/div/div/section[1]/div[1]/form/div[2]/input').send_keys("pwdh8f_liepin")
            driver.find_element_by_xpath('//*[@id="home"]/div[2]/div[1]/div/div/section[1]/div[1]/form/input[3]').click()
            time.sleep(10)
            num = 0
            while num < 300000:
                e = findElement(driver,By.XPATH,'// *[ @ id = "home"] / div[3] / div[2] / div[1] / div[4] / ul / li[1] / a')
                if e:
                    e.click()
                    time.sleep(gConfig.get("update.time",300))
                    self.http.get("https://c.liepin.com/")
                    logDebug("update %s" % num)
                    num += 1
                else:
                    logError("try again!")
                    break

    def updateResume2(self):
        """
        python spider/sample/fanli.py update.time=600 updateResume2
        :return:
        """
        self.http.get("https://www.lagou.com/")
        driver = self.http.driver
        e = findElement(driver,By.XPATH,'//*[@id="changeCityBox"]/ul/li[2]/a')
        if e:#弹出地域选择
            e.click()
        driver.find_element_by_xpath('//*[@id="lg_tbar"]/div/ul/li[1]/a').click()
        driver.find_element_by_xpath('/html/body/section/div[1]/div[1]/ul/li[1]').click()
        driver.find_element_by_xpath('/html/body/section/div[1]/div[2]/form/div[1]/input').send_keys("hbfhero@sina.com")
        driver.find_element_by_xpath('/html/body/section/div[1]/div[2]/form/div[2]/input').send_keys("pwdh8f_lagou")
        driver.find_element_by_xpath('/html/body/section/div[1]/div[2]/form/div[5]/input').click()

        time.sleep(10)
        num = 0
        while num<300000:
            driver.find_element_by_xpath('//*[@id="lg_tbar"]/div/ul/li[2]/a').click()#简历
            time.sleep(5)
            driver.find_element_by_xpath('//*[@id="workExperience"]/div[2]/div[2]/div[2]/div[1]/div[1]/div[2]/div/em').click()#edit
            time.sleep(5)
            driver.find_element_by_xpath('//*[@id="currentUpJobForm"]/div/div[6]/input').click()#edit
            time.sleep(gConfig.get("update.time",300))
            logDebug("update %s"%num)
            num+=1

if __name__ == '__main__':
    callMethod(Fanli, sys.argv[1:])
