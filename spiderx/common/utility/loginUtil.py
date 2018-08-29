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
import random
import time
from PIL import Image
import requests

from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By

from spiderx.common.constant import ACCOUNT_TPE_COOKIE, CFG_DOWN_WEBSITE, CFG_SERVER_COOKIE, ACCOUNT_TPE_WEBSITE, \
    CFG_SERVER_PROXY, CFG_SERVER_PROXYAPI
from spiderx.common.utility.dbUtil import LocalDb

from superbase.constant import CFG_DB_BUSINESS, CFG_JOB_ENABLE, CFG_DB_MONITOR, CFG_DB_OFFSET
from superbase.globalData import PROJECT_ROOT, gTop, gConfig
from superbase.utility.accountUtil import AccountManager
from superbase.utility.ioUtil import md5, getExtIP
from superbase.utility.logUtil import logException, logInfo, logError
from superbase.utility.mysqlUtil import createDb2
from superbase.utility.safeUtil import TryTime
from superbase.utility.timeUtil import getTimestamp

VCODE_PATH = PROJECT_ROOT + "temp/vcode/"  # PROJECT_ROOT-->项目根


class VCode(object):

    def filePath(self):
        # 获取时间戳
        ts = getTimestamp()
        # 路径-->项目根+"temp/vcode/"
        tmpDir = VCODE_PATH
        # 测试一条路径是否存在。返回False，用于中断符号链接
        if not os.path.exists(tmpDir):
            os.makedirs(tmpDir)  # mkdir类似

        # 随机数  如果需要大量的 请用uuid.uuid4()
        rint = random.randint(1000, 9999)
        # 图片名称=本地时间戳+随机数
        imgName = "%s_%d.png" % (ts, rint)
        return tmpDir + imgName

    def saveImage(self, driver, element, rect=None):
        """
        :param driver: 驱动程序
        :param element: 元素
        :param rect: None
        :return:
        """
        # http://stackoverflow.com/questions/10848900/how-to-take-partial-screenshot-frame-with-selenium-webdriver
        """
        1）sudo apt-get install libjpeg-dev
        2）sudo apt-get install libfreetype6-dev
        3）sudo  easy_install PIL #pip install Pillow
        """

        def element_screenshot(driver, element, filename):
            """
            元素位置
            :param driver:
            :param element:
            :param filename:
            :return:
            """
            if element:
                bounding_box = (  # 边界框
                    int(element.location['x']),  # left 位置左
                    int(element.location['y']),  # upper 位置上
                    int(element.location['x'] + element.size['width']),  # right 位置右
                    int(element.location['y'] + element.size['height'])  # bottom 位置下
                )
            else:
                bounding_box = rect  # rect-->None
            return bounding_box_screenshot(driver, bounding_box, filename)

        def bounding_box_screenshot(driver, bounding_box, filename):
            """
            调整图像
            :param driver:
            :param bounding_box:
            :param filename:
            :return:
            """
            # Python Imaging Library是Python平的图像处理标准库
            from PIL import Image
            # 保存当前窗口到PNG图像文件的屏幕截图。
            driver.save_screenshot(filename)
            # 打开并识别给定的图像文件。
            base_image = Image.open(filename)
            # 从这个图像返回一个矩形区域。盒子里是一个4元组定义左、上、右和下像素坐标。
            cropped_image = base_image.crop(bounding_box)
            # 调整尺寸
            base_image = base_image.resize(cropped_image.size)
            # 定义图形大小
            base_image.paste(cropped_image, (0, 0))
            # 保存
            base_image.save(filename)
            return base_image

        fn = self.filePath()
        element_screenshot(driver, element, fn)
        return fn

    def joinImages(self, image1, image2, standard=1):
        fn = self.filePath()
        # 打开图片
        pic_fole_head1 = Image.open(image1)
        # 图片大小
        width1, height1 = pic_fole_head1.size

        pic_fole_head2 = Image.open(image2)
        width2, height2 = pic_fole_head2.size

        if standard != 1:
            toImage = Image.new('RGBA', (width2, height1 + height2))
            tmppic1 = pic_fole_head1.resize((width2, height1))
            toImage.paste(tmppic1, (0, 0))
            toImage.paste(pic_fole_head2, (0, height1))
            toImage.save(fn)
            return fn
        toImage = Image.new('RGBA', (width1, height1 + height2))
        tmppic2 = pic_fole_head2.resize((width1, height2))
        toImage.paste(pic_fole_head1, (0, 0))
        toImage.paste(tmppic2, (0, height1))
        toImage.save(fn)
        return fn

    @staticmethod
    def vCodeCheck(driver, element, accountHelper, **params):
        """
        :param element:
        :param type:
        :return:
        """
        path = VCode.saveVcodeImage(driver, element)
        return accountHelper.getVCode(path, **params)

    @staticmethod
    def testVcode():
        """
        测试 code
        :return:
        """
        # 浏览器驱动->phantomjs
        driver = webdriver.PhantomJS()
        driver.get("http://hr.zhaopin.com/hrclub/index.html")
        # 匹配数据
        userName = driver.find_element(By.CSS_SELECTOR,
                                       "#form1 > ul:nth-child(1) > li:nth-child(1) > label:nth-child(1) > input:nth-child(1)")
        pwd = driver.find_element(By.CSS_SELECTOR,
                                  "#form1 > ul:nth-child(1) > li:nth-child(2) > label:nth-child(1) > input:nth-child(1)")
        vcode = driver.find_element(By.CSS_SELECTOR,
                                    "#form1 > ul:nth-child(1) > li:nth-child(3) > label:nth-child(1) > input:nth-child(1)")
        userName.send_keys("shanghaiwacai")
        pwd.send_keys("wacai2015")
        # driver.get("http://rd2.zhaopin.com/s/loginmgr/picturetimestamp.asp?t=1429060977000")
        url = "http://rd2.zhaopin.com/s/loginmgr/picturetimestamp.asp?t=%d" % (int(time.time() * 1000))
        VCode.saveVcodeImage(driver, driver.find_element(By.CSS_SELECTOR, "#vimg"))

    @staticmethod
    def saveVcodeImage(driver, element):
        """
        :param driver:
        :param element:
        :return:
        """
        # TODO:


ACCOUNT_WORK = 1
ACCOUNT_UNWORK = 2
ACCOUNT_OCCUPIED = 3
ACCOUNT_ERROR = 4
ACCOUNT_INVALID = 11


class Accounter(object):
    def __init__(self, delAc=False):
        """

        :param acType: cookie,webSite,...
        """
        params = AccountManager().getAccount(CFG_DB_BUSINESS)
        self.db = createDb2("loginHelperDb", params, dictCursor=1)
        self.curMaxId = 0
        self.offset = gConfig.get(CFG_DB_OFFSET, 0)
        self.history = []
        self.delAc = delAc

    @classmethod
    def getMd5Idx(cls, phone, pwd, source):
        return md5("%s-%s-%s" % (source, phone, pwd))

    def getAccounts(self, source, status=ACCOUNT_WORK, maxNum=0, maxId=0):
        """
        :param source: 网站名，e.g,www_tianyancha_com
        :param cond2: 通常就是 "id>xx"
        :return:data={
            count:5,
            data:[{id:xx,val:{},cookie:xx},...]
            msg:"",
            code:0 #0 表示成功
        }
        """

        def _get(source, status=ACCOUNT_WORK, maxNum=0, maxId=0):
            if maxNum == 0:  # unwork账号不做限制，work默认每次1个
                maxNum = 1 if status == ACCOUNT_WORK else 1000
            limit = "%s,%s" % (self.offset, maxNum)
            sql = "select id,val,cookie from account where status=%s and source='%s' and id>%s order by id limit %s" % (
                status, source, maxId, limit)
            logInfo(sql)
            data = self.db.query(sql)
            if data:
                self.curMaxId = data[-1]["id"]
                data = list(data)
                if status == ACCOUNT_WORK:

                    ids = ",".join(['%s' % item['id'] for item in data])
                    cond = "where id in (%s)" % ids
                    if not self.delAc:
                        self.db.update("account", {"status": ACCOUNT_OCCUPIED}, cond)
                        curTime = time.time()
                        for id, val, cookie in data:
                            self.history.append((curTime, id))
                    else:
                        self.delAccount(cond=cond)

                return data
            else:
                return None

        result = _get(source, status, maxNum, maxId)
        if not result and self.curMaxId:  # 账号取完,但不排除前面的账号已经恢复,所以重试一次
            logInfo("all accounts are fetched,try again from the start")
            self.curMaxId = 0
            result = _get(source, status, maxNum, self.curMaxId)
        return result

    def getMoreAccounts(self, source, status=ACCOUNT_WORK, maxNum=0):
        """
        本地账号用完时，用该方法取得更多
        :param source:
        :param status:
        :param maxNum:
        :return:
        """
        return self.getAccounts(source, status, maxNum, self.curMaxId)

    def getAccountStatus(self, id):
        return self.db.getOne("select status from account where id=%s" % id)

    def updateAccount(self, id, status=ACCOUNT_WORK):
        """
        更新账号状态
        :param id:
        :param status:
        :return:True or False
        """
        self.db.update("account", {"status": status, 'upTime': getTimestamp()}, "where id=%s" % (id))
        logInfo("update account {} to {}".format(id, status))

    def delAccount(self, cond=None):
        """
        :param cond:
        :return:True or False
        """
        self.db.delete("account", cond)

    def checkError(self):
        """
        检查取账号是否正常
        :return: True有错
        """
        interval = gConfig.get("account.checkInterval", 300)  # 检测区间300s
        limit = gConfig.get("account.checkLimit", 10)  # 最大数10个
        if len(self.history) > limit:
            diff = self.history[-1][0] - self.history[-10][0]
            if diff < interval:
                logError("getAccount too frequently!!-%s" % diff)
                return True
        return False

    def getUsedAccountNum(self):
        return len(self.history)

    def getAccountNum(self, source, status):
        return self.db.getOne("select count(1) from account where status=%s and source='%s'" % (status, source))


class CookieAccounter(Accounter):
    """
        正常使用cookie流程:
        mgr = CookieManager()
        md5Idx,cookie = mgr.getOne(webName)

        #每次去getOne cookie前，如果当前有在使用的cookie，记得先delOne
        if cookie but cookie has gone:#cookie失效,直接删除
            mgr.delOne(md5Idx)
            cookie = None

        if not cookie:
            if 爬虫有login能力:
                cookie = login()
                mgr.addOne(cookie,webName)
            else:
                #报错退出
                self.jobHandleError(ERR_LOGIN_FAIL)


    """

    def __init__(self, delAc=False):
        Accounter.__init__(self, delAc)

    def addAccount(self, webAccountId, cookie):
        """
        :param webAccountId:
        :param cookie: 字符串
        :return:
        """

        self.db.update("account", {"cookie": cookie, "inTime": getTimestamp()}, "where id=%s" % webAccountId)

    def addAccount2(self, source, cookie):
        """
        :param source: 具体网站如 www_51job_com,就是CFG_DOWN_WEBSITE
        :param cookie: 字符串
        :return:
        """

        self.db.insert("account", {"cookie": cookie, 'source': source, "inTime": getTimestamp()})


class WebAccounter(Accounter):
    """
    usage:
    每个爬虫在生命周期里维护一个WebAccounter,
    默认每次取5个，按id递增
    """

    def __init__(self, delAc=False):
        Accounter.__init__(self, delAc)

    def addAccount(self, phone, pwd, source):
        """
        :param phone:
        :param pwd:
        :param source: like www_tianyancha_com
        :return:
        """

        self.db.insert("account", {
            "val": json.dumps({"phone": phone, "password": pwd}),
            "source": source,
            "md5Idx": Accounter.getMd5Idx(phone, pwd, source)
        })


class verification(object):
    def __init__(self, driver):
        self.driver = driver

    def slide(self, source, num1, num2):
        """
        滑动验证只能滑动到底的
        :param source: 定位的元素
        :param num1，num2:滑动的距离
        :return:
        """
        ActionChains(self.driver).drag_and_drop_by_offset(source, num1, num2).perform()

    def picClick(self, element, a, b):
        """

        :param element:
        :param a:
        :param b:
        :return:
        """
        action = ActionChains(self.driver).move_to_element_with_offset(element, int(a), int(b))
        action.click_and_hold()
        time.sleep(random.random())
        action.release().perform()


class ProxyManager(object):
    def __init__(self):
        if gConfig.get("env")=="ONLINE":
            params = AccountManager().getAccount(CFG_DB_BUSINESS)
            if params:
                self.db = createDb2("loginHelperDb", params, dictCursor=1)
        self.proxyServerList = AccountManager().getAccount(CFG_SERVER_PROXY)
        self.proxyServer = self.proxyServerList[2]
        self.proxyList = []
        self.usedIP = 0
        if gConfig.get(CFG_SERVER_PROXYAPI, None) == "zhima":
            self.website = {
                "www_tianyancha_com": "tyc",
                "www_qichacha_com": "qcc",
            }
            self.proxyId = 0
            self.currWeb = self.website[gConfig.get(CFG_DOWN_WEBSITE)]
            self.currProxyInfo = None

    def proxyFromDB(self):
        self.currProxyInfo = self.db.getRow(
            "select id,ip,port,{} from proxy WHERE {} = (SELECT min({}) FROM proxy WHERE id != {})". \
                format(self.currWeb, self.currWeb, self.currWeb, self.proxyId))
        self.usedIP += 1
        times = int(self.currProxyInfo[self.currWeb]) + 1
        self.proxyId = self.currProxyInfo["id"]
        self.db.update("proxy", {self.currWeb: times}, "where id = {}".format(self.currProxyInfo["id"]))
        proxy = u"http://{}:{}".format(self.currProxyInfo["ip"], self.currProxyInfo["port"])
        logInfo("currProxy is " + proxy)
        return proxy

    def giveBackProxy(self):
        try:
            times = self.db.getOne("select {} from proxy where id = {}".format(self.currWeb, self.proxyId))
            times = int(times) - 1
            if times < 0:
                times = 0
            self.db.update("proxy", {self.currWeb: times}, "where id = {}".format(self.proxyId))
            self.currProxyInfo = None
            logInfo("give back proxy successfully!")
        except:
            logInfo("proxy maybe delete by proxyPoolManager")

    def getProxyFromAPI(self):
        if gConfig.get(CFG_SERVER_PROXYAPI, "mogu") == "zhima":
            if self.currProxyInfo:
                self.giveBackProxy()
            return self.proxyFromDB()

        elif gConfig.get(CFG_SERVER_PROXYAPI, "mogu") == "mogu":
            retryNum = 0
            maxTry = 10
            while retryNum < maxTry:
                if not self.proxyList:
                    res = requests.get(self.proxyServer)
                    ck_json = res.content
                    logInfo(ck_json)
                    ck_dict = json.loads(ck_json)
                    if ck_dict["code"] == "3001":
                        time.sleep(5)
                    if ck_dict["code"] == "0":
                        self.proxyList = ck_dict["msg"]
                if self.proxyList:
                    ck = self.proxyList.pop()
                    ipPort = "http://{}:{}".format(ck["ip"], ck["port"])
                    if self.chekIP(ipPort):
                        return ipPort
                retryNum += 1
                time.sleep(2)
            logInfo("try to get proxies is disabled!!!!check out!!!")

    def chekIP(self, ipPort):
        proxy = {
            "http": ipPort
        }
        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36'
        }
        for url in ["http://www.baidu.com",
                    "https://www.so.com/",
                    "https://www.sogou.com/",
                    "http://www.youdao.com/",
                    "https://cn.bing.com/"]:
            try:
                res = requests.get(url, proxies=proxy, headers=headers, timeout=8)
            except:
                continue
            if res.status_code == 200:
                # logInfo(res.text)
                return True
        return False
