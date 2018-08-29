# -*- coding:utf-8 -*-
# -------------------------------------------------------------------------------
# Name:
# Author:      bhuang$
# -------------------------------------------------------------------------------
import codecs
import glob
import gzip
import json
import sys

import time

import os

import psutil
from lxml import etree
sys.path.append(".")
from jobManager.manage.node import Role
from spiderx.common.constant import CFG_HTTP_BROWSERMODE
from superbase.utility.jobUtil import JobUtil
from superbase.utility.safeUtil import TryTime
from superbase.utility.timeUtil import getTimestampBySec



from spiderx.common.utility.httpUtil import SeleniumAgent, RequestsAgent
from spiderx.common.utility.parseUtil import Extractor
from superbase.utility.logUtil import logInfo, logCritical, logException, logDebug
from superbase.utility.mailUtil import Mail


from spiderx.common.utility.loginUtil import CookieAccounter, WebAccounter, ACCOUNT_UNWORK, ACCOUNT_WORK, \
    ACCOUNT_OCCUPIED
from superbase.constant import CFG_ACCOUNT_PROVIDER, CFG_DB_MONITOR, CFG_DB_BUSINESS, JT_CLEANER, CFG_JOB_ID
from superbase.globalData import gConfig, gTop, PROJECT_ROOT
from superbase.utility.ioUtil import md5, getExtIP, delOverTimeFiles, str2line
from superbase.baseClass import BaseClass
from superbase.utility.processUtil import callMethod, assert2, isProcessAlive, showProcess, killProcess, runProcess, \
    ProcssLock, TAsyncJob


class TestX(BaseClass):
    def __init__(self,params=""):

        BaseClass.__init__(self,params)
        gConfig.set(CFG_ACCOUNT_PROVIDER,"spideraccount.samanager")

    def testCookie(self):
        mgr = CookieAccounter()
        mgr.addAccount(257,"test-cookie;wer=23")

    def testMd5(self):

        print(md5(u"中国啥地方sdf"))
        print(md5("中国啥地方sdf"))

    def testAccount2(self,acType="webSite"):
        """

        :param acType:
        :return:
        """
        #gConfig.set("account.checkInterval",10)#十秒内
        source = "test_cxx_com"
        ac = WebAccounter()
        #for i in range(40):ac.addAccount(phone="1380000000%s"%i,pwd="test",source=source)

        for i in range(11):#
            ac.getAccounts(source)
        print ac.checkError()
        time.sleep(12)
        ac.getAccounts(source)
        print ac.checkError()

    def testAccount(self,acType="webSite"):
        gConfig.set(CFG_DB_BUSINESS,"loginHelperDb")
        source = "www_tianyancha_com"#"""""test_cxx_com"
        if acType=="cookie":
            ids = (465,466,467,468,469,470)
            ac = CookieAccounter()
            for i in ids:
                ac.addAccount(i,"test-cookue-%s"%i)
        else:
            ac = WebAccounter()
            a = ac.getAccounts(source="www_tianyancha_com")
            #ac.updateAccount(a['id'])
            for i in range(2):
                ac.addAccount(phone="1380000000%s"%i,pwd="test",source=source)

        workAccounts = ac.getMoreAccounts(source,status=ACCOUNT_WORK,maxNum=5)
        for idx,ac2 in enumerate(workAccounts):
            ac.updateAccount(ac2['id'],ACCOUNT_UNWORK)
            if idx>1:break

        unworkAccounts = ac.getAccounts(source,status=ACCOUNT_UNWORK,maxNum=5)
        assert2(len(unworkAccounts)==3)
        occuAccounts = ac.getMoreAccounts(source,status=ACCOUNT_OCCUPIED,maxNum=5)
        assert2(len(occuAccounts)==2)

        for ac2 in unworkAccounts:
            ac.updateAccount(ac2['id'],ACCOUNT_WORK)
        workAccounts = ac.getMoreAccounts(source,maxNum=5)
        assert2(len(workAccounts)==1)
        print("done")


    def testMail(self):
        Mail.sendEmail("文本处理有问题","pls check it",["hbfhero@163.com"])

    @TryTime(3)
    def testIP(self, selenium=0, browser='chrome'):
        selenium = int(selenium)
        from spiderx.common.utility.httpUtil import RequestsAgent
        from spiderx.common.constant import CFG_HTTP_BROWSER
        gConfig.set(CFG_HTTP_BROWSER,browser)
        http = RequestsAgent() if not selenium else SeleniumAgent()
        parser = etree.HTMLParser(encoding="utf-8")
        extractor = Extractor(http, parser)
        ips = []
        for i in range(2):
            ip=getExtIP(extractor)
            logInfo("%s:%s"%(i,ip))
            http.setNewSession()
            ips.append(ip)
        logInfo("~~~~~~!!!!!!!!!!!!!the unique ip is ---------------%s"%(len(set(ips))))


    def testExcept(self):
        for i in range(2):
            try:
                1/0
            except Exception, e:
                logException()
        2/0
    def testJobEnd(self):
        #Job().end()
        from jobManager.sites.dispatcher import Dispatcher
        Dispatcher().tianchaUrl()

    def testBrowser(self,browser="chrome",headless="no"):
        logInfo("test-browser-begin")
        from spiderx.common.constant import CFG_HTTP_BROWSER
        gConfig.set(CFG_HTTP_BROWSER,browser)
        gConfig.set(CFG_HTTP_BROWSERMODE,headless)
        http = SeleniumAgent()
        http.get("http://www.baidu.com")
        #showProcess()
        time.sleep(5)
        logInfo("test-browser-done")

    def testDb(self):
        gConfig.set("env","ONLINE")
        name = gTop.get(CFG_DB_MONITOR).getOne("select name from job limit 1")
        logInfo("testing-name-%s"%name)
        from superbase.utility.aliyun import AliYun
        AliYun().upFile(os.path.join(PROJECT_ROOT,"log/spiderx.log"))

    def killProcess(self,pid):
        #showProcess()
        pid = int(pid)
        print killProcess(pid)

    def showProcess(self,exe="python"):
        logDebug(showProcess(exe))

    def testHttpCfg(self):
        a = RequestsAgent()
        a.setCfg("encoding","abc")
        print(a.outFormat+a.encoding)


    def delFiles(self,root):
        delOverTimeFiles(root)

    def testNewMachine(self):
        self.testDb()
        self.testIP()
        self.testIP(1)
        self.testIP(1,browser="phantomjs")

    @TryTime(3)
    def testExc(self):
        logInfo("testing---")
        a=1/0

    def testTryTime(self):
        self.testExc()

    def testSync(self):
        from spiderx.common.utility.resultUtil import SyncPoint
        sp = SyncPoint("www_test_com/kk/hello/test")
        from spiderx.common.constant import CFG_DOWN_SYNCINFO
        sp.saveSyncPoint({CFG_DOWN_SYNCINFO:{'b':1,'c':2}})
        p = sp.getSyncPoint()
        print(p)

    def testRMI(self):
        from jobManager.manage.rmi import RMI
        RMI().addResetJob(jobId=17858,status=200,beginTime='')

    def testCmd(self):
        f=codecs.open("g:/project/urlInfo453.html")
        content = f.read()

    def testReadAliyun(self):
        """
        :return:
        """
        from superbase.utility.aliyun import AliYun
        lines = AliYun().readJsonFromAliyun('downData/www_qichacha_com')
        for line in lines:
            logDebug(line['name'])

    def testAsyncJob(self):
         cmd2 = "python spiderx/testx.py testJob"
         TAsyncJob(cmd2,delay=10)
         logInfo("testAsyncJob done!")

    def testJob(self):
        """
        env=ONLINE,job.id=18230 testJob
        :return:
        """
        proc = psutil.Process(os.getpid())
        info = "\nkill Job %s--%s-%s"%(proc.pid," ".join(proc.cmdline()),getTimestampBySec(proc.create_time()))
        logInfo(info)
        return

        db = gTop.get(CFG_DB_MONITOR)
        gConfig.set("debug.sql",1)
        db.insert("block",{"jobName":"ttest"})
        db.update("block",{"jobName":"ttest2"},"where id=1")
        JobUtil().jobStatusInfo({
            "account":100,
            "ip":300,
            "num":3450
        })

    def getMissedBatch(self):
        db = gTop.get(CFG_DB_MONITOR)
        batches = [batch[0] for batch in db.query("select batch from job where name='tycdetail_fetcher' and status=100")]
        batches2 = [batch[0] for batch in db.query("select batch from batch where batch like 'tycdetail_%' and closed=0")]
        result = list(set(batches)-set(batches2))
        if result:
            sql="select id, name,status,batch,beginTime from job where batch in ('%s')"%("','".join(result))
            logDebug(sql)
            for id,name,status,batch,beginTime in db.query(sql):
                if status!=100:
                    db.update("job",{"status":2},"where id=%s"%id)
            for batch2 in result:
                db.update("batch",{"closed":0},"where batch='%s'"%batch2)

    def path1(self):
        print os.getcwd()
        print os.path.abspath(os.path.dirname(__file__))

    def backup(self,beginId,endId=286):
        """
        python spiderx\testx.py env=ONLINE backup 105 199
        python spiderx\testx.py env=ONLINE backup 21 99

        :param beginId:
        :param endId:
        :return:
        """
        dest = "e:/company"
        gConfig.set(CFG_DB_BUSINESS,"TYC")

        beginId = int(beginId)
        endId = int(endId)
        while beginId<=endId:
            try:
                db = gTop.get(CFG_DB_BUSINESS)
                company = "company_58_%s" % beginId

                fname = os.path.join(dest, "%s.txt" % company)
                offset1 = 0
                limit1 = 1000
                with codecs.open(fname, "w", encoding="utf-8") as f:
                    while True:
                        rows = db.query("select name,url from %s limit %s offset %s" % (company,limit1,offset1))
                        if not rows:
                            break
                        offset1 += limit1
                        for name, url in rows:
                            f.write("%s##%s\n" % (name, url))

            except Exception:
                logException()
            else:
                beginId+=1
                logInfo("backup-%s"%company)

    def extract(self):
        """

        :return:
        """
        root = "C:/tempAliyun/downData/www_liepin_com"
        root2 = "E:/shanghai51"

        with codecs.open("%s/shanghailiepin.txt"%root2, "w", encoding="utf-8")  as f2:
            f2.write("%s##%s##%s##%s##%s##%s\n"%("name","address","website","type","name2","info"))
            files = glob.glob("%s/*/*/*/*/*/*/*/*.gz"%root)
            total = 0
            for idx,fileName2 in enumerate(files):
                num = 0
                with gzip.open(fileName2, 'rb') as f:
                    for line in f:
                        try:
                            d = json.loads(line)
                            if u"上海" in d.get("name",""):
                                f2.write(u"%s##%s##%s##%s##%s##%s\n"%(
                                    d.get("name",""),
                                    d.get("address",""),
                                    d.get("website",""),
                                    d.get("type",""),
                                    d.get("businessLicense",""),
                                    str2line(d.get("info",""))
                                ))
                                num+=1
                        except Exception:
                            logException()
                total += num
                logInfo("%s:num=%s,total=%s"%(idx,num,total))


if __name__ == '__main__':
    callMethod(TestX, sys.argv[1:])