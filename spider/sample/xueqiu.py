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
import sys
import time

from spiderx.common.utility.resultUtil import HourlySaveLocalHandler, SyncPoint
from superbase.globalData import gTop, gConfig, PROJECT_ROOT
from superbase.utility.ioUtil import str2line, mkdir
from superbase.utility.logUtil import logInfo
from superbase.utility.timeUtil import getTimestamp

from spiderx.common.baseCrawler import BaseCrawler
from spiderx.common.utility.parseUtil import Extractor, CssElement, dehtml
from superbase.constant import CFG_DB_BUSINESS, CFG_JOB_BATCH, CFG_JOB_NAME, CFG_JOB_ENABLE
from spiderx.common.constant import CFG_HTTP_OUTFORMAT, CFG_HTTP_INITURL, CFG_DOWN_INCMODE, CFG_DOWN_INDEX, \
    CFG_DOWN_SYNCINFO, CFG_DOWN_SYNCBEGIN, CFG_DOWN_BRKMODE, CFG_DOWN_SYNCEND, CFG_DOWN_SYNCCURR, CFG_DOWN_ROOT
from superbase.utility.processUtil import callMethod

class Xueqiu(BaseCrawler):
    """
    爬取雪球大v帖子
    演示目的：
        1,  CFG_HTTP_OUTFORMAT 直接获取返回json
        2,  爬虫的增量/断点支持,一个爬虫如何支持增量由工程师自己决定
            自己决定syncInfo的结构
            如下是常规的：
            syncInfo={
                CFG_DOWN_SYNCBEGIN:xx,
                CFG_DOWN_SYNCCURR:xx,
                CFG_DOWN_SYNCEND:xx
            }
            xx可以是有序id，时间，页码等


    """
    def __init__(self,params=""):
        subCfg = {
            CFG_JOB_BATCH: "xueqiu_test20140717",
            CFG_JOB_NAME: "xueqiu",
            CFG_HTTP_OUTFORMAT:"json",
            CFG_HTTP_INITURL:"https://xueqiu.com/people",
            CFG_DOWN_INDEX:"www_xueqiu_com/bigv/post",
            "xueqiu.postSaveMinWord":200,#帖子文字数量少于200字不处理
        }
        BaseCrawler.__init__(self,params,subCfg)

    def post(self,userId='9650668145'):
        """
        获取帖子 9650668145 1761234358
        {"count":20,"statuses":[...],"total":474,"page":2,"maxPage":24}
        https://xueqiu.com/v4/statuses/user_timeline.json?user_id=9650668145&page=12&type=&_=1508802861537
        https://xueqiu.com/v4/statuses/user_timeline.json?user_id=1761234358&page=12&type=&_=1508802861537
        :param userId:
        :return:
        """
        #每个用户作为一个索引
        newIndex = gConfig.get(CFG_DOWN_INDEX)+"/%s"%userId
        gConfig.set(CFG_DOWN_INDEX,newIndex)
        urlTemp = "https://xueqiu.com/v4/statuses/user_timeline.json?user_id={userId}&page={pageIdx}&type=&_={ts}"
        syncPoint = SyncPoint() #第一条数据作为同步点，因为第一条数据的帖子最新
        #先获得同步点信息:
        sp = syncPoint.getSyncPoint()
        oldSyncInfo = sp.get("syncInfo",None) if sp else None
        syncInfo = syncPoint.getNewSyncInfoByDesc(oldSyncInfo)
        saveHandler = HourlySaveLocalHandler(syncPoint=syncPoint) #
        def save(result):
            posts = result["statuses"]
            if len(posts)==0:
                syncInfo[CFG_DOWN_SYNCEND] = syncInfo[CFG_DOWN_SYNCCURR]
                syncPoint.saveLastSyncInfo({CFG_DOWN_SYNCINFO:syncInfo})
                return 0
            saveNum = 0
            for post in posts:

                    result={
                        "xid":post["id"],
                        "userId":post["user_id"],
                        "commentId":post.get("commentId",0),
                        "topicId":post["retweet_status_id"],
                        "topicUser":post["retweeted_status"]["user_id"] if post["retweet_status_id"] else post["user_id"],
                        "replyCount":post["reply_count"],
                        "favCount":post["fav_count"],
                        "likeCount":post["like_count"],
                        "retweetCount":post["retweet_count"],
                        "viewCount":post["view_count"],
                        "inTime":post["created_at"],
                    }

                    test = True #测试，内容不做保存
                    if not test:
                        text = dehtml(post["text"])
                        idx = text.find("//@")
                        if idx>0:
                            text = text[:idx]
                        wordCount = len(text)
                        if wordCount>gConfig.get("xueqiu.postSaveMinWord",200):#超过200字的保存
                            result["info"] = text,
                        result["wordCount"] = wordCount
                        if post["reward_count"]>0:
                            reward = {
                                "count":post["reward_count"],
                                "users":post["reward_user_count"],
                                "amount":post["reward_amount"],
                            }
                            result["reward"] = json.dumps(reward)

                    if int(result["inTime"]) < int(syncInfo[CFG_DOWN_SYNCBEGIN]):
                        syncInfo[CFG_DOWN_SYNCCURR] = result["inTime"]
                        result[CFG_DOWN_SYNCINFO] = syncInfo
                        saveHandler.handle(result)
                        saveNum += 1

            return saveNum
        downNum = 0
        curPage = 0 # if gConfig.get(CFG_DOWN_INCMODE,1) else hasDowned/20+1 #该参数可用做断点使用
        while int(syncInfo[CFG_DOWN_SYNCCURR])>int(syncInfo[CFG_DOWN_SYNCEND]):
            url = urlTemp.format(userId=userId,pageIdx=curPage,ts=int(time.time()))
            result = self.downOne(url,None)
            downNum+=save(result)
            curPage+=1
            logInfo("user=%s,curPage=%s,downNum=%s"%(userId,curPage,downNum))

        self.jobDone()

    def test(self,idx):
        """
        1,可选user，不要太多页（1840488466，4691977921,4131145503）
        2,idx=1，设置begin，从begin点爬，人为中断
        3,idx=2，断点开始继续,还是人为中断
        4，idx=3，强制使用增量模式，从头开始爬到上次开始点
        :return:
        """
        gConfig.set(CFG_JOB_ENABLE,1)
        gConfig.set(CFG_DOWN_ROOT,PROJECT_ROOT)
        user = '4131145503'
        idx = int(idx)
        if idx==1:
            gConfig.set(CFG_DOWN_SYNCBEGIN,(time.time()-180*24*3600)*1000) #取半年前时间为存量开始点
            self.post(user)
        elif idx==2:
            logInfo("break point mode")
            self.post(user)
        else:
            gConfig.set(CFG_DOWN_INCMODE,1)
            logInfo("normal inc mode")
            self.post(user)

if __name__ == '__main__':
    callMethod(Xueqiu, sys.argv[1:])
