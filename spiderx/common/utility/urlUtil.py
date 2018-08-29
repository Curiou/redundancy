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
from superbase.globalData import gConfig
from superbase.utility.logUtil import logDebug, logError, logInfo, logException
from superbase.utility.timeUtil import getTimestamp

from spiderx.common.utility.parseUtil import Extractor
from spiderx.common.constant import TAG_LIST_TOTAL_PAGE_NUM, ERR_BLOCK, TAG_LIST_NO_RESULT, TAG_LIST_PAGE_PATTERN, \
    CFG_DOWN_MAXPAGENUM


class PageUrlOffer(object):
    """
    分页url提供者，通常用于提供多页格式的list
    """

    def __init__(self, http, parser, beginUrl):
        self.http = http
        self.parser = parser
        self.beginUrl = beginUrl

    def getPageNum(self, url, listConf):
        """翻页获取翻页页数的结果"""
        config = listConf
        # 不分页 TAG_LIST_TOTAL_PAGE_NUM总页数
        if not config.get(TAG_LIST_TOTAL_PAGE_NUM, None):  # 不分页
            return (1, None)
        content = self.http.get(url)  # 获取响应
        if self.http.isBlocked():  # antiBlockUtil返回blocked的值或False
            return (0, ERR_BLOCK)

        if TAG_LIST_NO_RESULT in config and content.find(config[TAG_LIST_NO_RESULT]) > 0:
            logDebug("%s url=%s\n%s" % (getTimestamp(), url, config[TAG_LIST_NO_RESULT]))
            return (0, None)
        result = {}
        # 解析并且翻页的结果
        Extractor(self.http, self.parser).getResultByContent(content,
                                                             {TAG_LIST_TOTAL_PAGE_NUM: config[TAG_LIST_TOTAL_PAGE_NUM]},
                                                             result)
        # 优化了一下
        # totalPageNum = int(result[TAG_LIST_TOTAL_PAGE_NUM].strip())
        if result[TAG_LIST_TOTAL_PAGE_NUM] != "":
            totalPageNum = int(result[TAG_LIST_TOTAL_PAGE_NUM].strip())
        else:
            totalPageNum = 1    # 此处改动 0 -->1

        return (totalPageNum, None)

    def getNextPageUrl(self, url, page, listConf):
        """
        访问下一个url结果
        如果url不符合你的要求可以重写此方法
        :param url:
        :param page:
        :param listConf:
        :return:
        """
        return url + "%s%s" % (listConf[TAG_LIST_PAGE_PATTERN], page) if listConf.has_key(
            TAG_LIST_PAGE_PATTERN) else url

    def pageUrls(self, listConf):
        """
        是自动拼接url以&str=
        list url's generator
        :return:
        """
        try:
            url = self.beginUrl
            totalPage, err = self.getPageNum(url, listConf)
            if err:
                logError("getPageNum error?%s,url=%s" % (err, url))
            logInfo("%s url=%s\ntotalPage=%s" % (getTimestamp(), url, totalPage))
            if int(gConfig.get(CFG_DOWN_MAXPAGENUM)):
                totalPage = min(int(totalPage), int(gConfig.get(CFG_DOWN_MAXPAGENUM)))
            for page in range(int(totalPage)):
                try:
                    url2 = self.getNextPageUrl(url, page + 1, listConf)
                    if self.http.isBlocked():
                        break
                    yield url2
                except Exception:
                    logException()
        except Exception, e:
            logException()
