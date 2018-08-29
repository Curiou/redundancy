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

TIME_OUT_SEC = 30
CLICK_FAKE_SEC = 1
SEARCH_DELAY_SEC = 2
JSON_DONE = 5
PARSE_OK2 = 4
REDOWNLOAD = 3
REPARSE = 2
PARSE_OK = 1
PARSE_UNPARSED = 0
PARSE_ERR = -1

DOWN_UNKNOWN_ERROR = -9
DOWN_MAX_LINKS_DONE = 200
DOWN_PAUSE_DOWNLOAD_LINK = 201
DOWN_ALL_LINKS_DONE = 202

DOWN_CANNOT_ITEM = "!!CAN_NOT_DOWNLOAD_ITEM"

LOGIN_FAIL_VERIFY = 400
LOGIN_FAIL_WRONG = 401
LOGIN_FAIL_BLOCK = 404
LOGIN_OK = 200

CA_STATUS_LOGIING = 1
CA_STATUS_LOGIN_ERR = 3
CA_STATUS_LOGIN_OK = 5

# 解析列表相关key
TAG_LIST_TOTAL_PAGE_NUM = "totalPageNum"  # 总页数
TAG_LIST_PAGE_SIZE = "pageSize"  # 每页的行数
TAG_LIST_NO_RESULT = "listNoResult"  # list 无结果的网站提醒
TAG_LIST_ITEMS = "listItems"  # list 行数组的模式
TAG_LIST_PAGE_PATTERN = 'pagePattern'  # 翻页的模式

# 爬虫配置，http、down相关
CFG_HTTP_INTERVAL = "http.interval"  # 爬取间隔 可以是数字如0.01, 10,也可以是range,如0.1-10
CFG_HTTP_PROXY_INTERVAL = "http.proxyInterval"  # 换代理的间隔，如4，表示每四次就换代理，默认为0，不换
CFG_HTTP_ACCOUNT_INTERVAL = "http.accountInterval"  # 换账号的间隔，如4，表示每四次就换账号，默认为0，不换
CFG_HTTP_UA = "http.ua"  # windows,mac,ios,android
CFG_HTTP_TIMEOUT = "http.timeout"  # 超时
CFG_HTTP_ENCODING = "http.encoding"  # 编码
CFG_HTTP_MAXREQUEST = "http.maxRequest"  # 一个session 最多请求次数
CFG_HTTP_UNESCAPE = "http.unescape"
CFG_HTTP_ENGINE = "http.engine"  # requests or selenium
CFG_HTTP_BROWSER = "http.browser"  # 浏览器
CFG_HTTP_BROWSERPIC = "http.browserPic" # 图片模式
CFG_HTTP_BROWSERMODE = "http.browserMode"  # 浏览器模式
CFG_HTTP_FIXUA = "http.fixua"  # 固定ua
CFG_HTTP_INITURL = "http.initUrl"  # new session时调用initurl,用于获得cookie
CFG_HTTP_INITCOOKIE = "http.initNeedCookie"  # new session时需要新的cookie ,1,0
CFG_HTTP_INITPROXY = "http.initNeedProxy"  # new session时需要新的proxy,1,0
CFG_HTTP_OUTFORMAT = "http.outformat"  # json,html,file

CFG_DOWN_MAXNUM = "down.maxNum"  # 下载最大数
CFG_DOWN_MAXPAGENUM = "down.maxPageNum"  # 下载最大页数
CFG_DOWN_INCMODE = "down.incMode"  # 增量模式
CFG_DOWN_BRKMODE = "down.brkMode"  # 断点模式
CFG_DOWN_INDEX = "down.index"  # 路径index,可以多级,逐级细分,{web_site_name}/index1/indexn,如www_51job_com/company/internet
CFG_DOWN_SYNCINFO = "down.syncInfo"  # 同步点信息
CFG_DOWN_INCPOINT = "down.incPoint"  # 增量起始点信息
CFG_DOWN_SYNCBEGIN = "down.syncBegin"  # 同步开始点信息
CFG_DOWN_SYNCCURR = "down.syncCurr"  # 同步当前点信息
CFG_DOWN_SYNCEND = "down.syncEnd"  # 同步结束点信息
CFG_DOWN_SYNCINTERVAL = "down.syncInterval"  # sync2remote的间隔，默认100
CFG_DOWN_FIRSTPAGE = "down.firstPage"  # 增量模式,初始页
CFG_DOWN_ACCOUNTID = "down.accountId"
CFG_DOWN_WEBSITE = "down.webSite"  # 网站全称，点号用下划线代替 www_51job_com
CFG_DOWN_IMAGE = "down.image"  # 是否要下载图片,默认False,用于phantomjs
CFG_DOWN_ROOT = "down.root"  # 下载数据根目录
CFG_BLOCK_MAXCHECK = "block.maxCheck"  # 最大量检查
CFG_AB_STRATEGY = "ab.strategy"  # antiblock strategy
CFG_AB_MAXRETRY = "ab.maxRetry"  #
CFG_DEBUG_PROGRESS = "debug.downProgress"  # 下载的进度指示
CFG_RESULT_FUNC = "result.resultFunc"  # save,log

CFG_SERVER_COOKIE = "server.cookie"  # cookie服务器
CFG_SERVER_PROXY = "server.proxy"  # proxy服务器
CFG_SERVER_PROXYAPI = "sever.proxyAPI" #proxyAPI
ACCOUNT_TPE_COOKIE = "cookie"  #
ACCOUNT_TPE_PROXY = "proxy"  # 代理
ACCOUNT_TPE_MONITORDB = "monitorDb"  # 监控数据库
ACCOUNT_TPE_WEBSITE = "webSite"  # 正常网站
ACCOUNT_TPE_CODESITE = "codeSite"  # 打码网站
ACCOUNT_TPE_PHONESITE = "phoneSite"  # 短信网站
ACCOUNT_TPE_MAIL = "mail"  # 邮件配置账户
# CFG_DB_BUSINESS = "db.business"  # 业务
# CFG_DB_MONITOR = "db.monitor"  # 监控

BROWSER_TPE_CHROME = "chrome"
BROWSER_TPE_FIREFOX = "firefox"
BROWSER_TPE_PHANTOMJS = "phantomjs"

ERR_BLOCK = -401
ERR_TIMEOUT = -402
ERR_NOCONTENT = -403
ERR_MAXNUM = -404
ERR_LOGIN_FAIL = -405
BLOCKED_INFO = 1
BLOCKED_ELEMENT = 2
BLOCKED_EXIT = -1

GD_HTTP_ENGINE = "gHttpEngine"
CFG_ISSUE_DATE = "issuedate" #51job 发行日期
