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
import datetime
import random
import re
import sys
import time
import traceback


def getCurTime(ts=None):
    """
    获取当前时间
    :param ts:
    :return: 格式: 2015-04-15 12:00:08
    """
    if not ts:
        ts = time.localtime()
    return time.strftime('%Y-%m-%d %H:%M:%S', ts)  # 组织好格式后返回


def getTimestamp(ts=None):
    """
    获取时间戳
    :param ts:
    :return: 格式: 20150415120008
    """
    if not ts:
        ts = time.localtime()
    return time.strftime('%Y%m%d%H%M%S', ts)  # 组织好格式后返回


def getDateTime(ts=None):
    """
    获取时间
    :param ts:
    :return: 格式 2017-12-27 13:55:29
    """
    if not ts:
        ts = time.localtime()
    return datetime.datetime(ts.tm_year, ts.tm_mon, ts.tm_mday, ts.tm_hour, ts.tm_min, ts.tm_sec)  # 组织好格式后返回


def getNextCronTime(crontabLine, base=None):
    """
    获取下一个时间
    :param crontabLine:
    :param base: 2017-12-27 13:55:29格式的时间
    :return: 格式: 20100901040200
    """
    if not base:
        base = getDateTime()
    from jobManager.tool.croniter import croniter
    dtime = croniter(crontabLine, base).get_next(datetime.datetime)  # 以cron格式提供对datetime对象的迭代
    # logInfo(dtime)
    return str(dtime).replace("-", "").replace(":", "").replace(" ", "")  # 组织好格式后返回


def getTimestampBySec(seconds=None):
    """
    通过秒数来获取时间戳
    :param seconds:
    :return: 格式: 20150415120008
    """
    if not seconds:
        seconds = time.time()
    return getTimestamp(time.localtime(seconds))  # 组织好格式后返回


def ts2seconds(ts):
    """
    将日期时间转化为Unix时间戳
    :param ts: 20150415120008
    :return:14090890.790
    """
    m = re.match(r"(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})", str(ts))
    strTime = "%s-%s-%s %s:%s:%s" % (m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6))
    return str2timestamp(strTime)


def timestampDiff(ts1, ts2):
    """
    计算两个时间的差值,返回结果单位为秒
    :param ts1: 时间1
    :param ts2: 时间2
    :return: 30
    """
    return ts2seconds(ts1) - ts2seconds(ts2)


def getHour(timestamp):
    """
    获取小时数
    :param timestamp:时间戳 要以字符串形式传入
    :return: 格式: 1514/35/55/34
    """
    m = re.match(r"(\d{4})(\d{2})(\d{2})(\d{2})", timestamp)
    return "%s/%s/%s/%s" % (m.group(1), m.group(2), m.group(3), m.group(4))


def getDay2(timestamp):
    """
    获取天数
    :param timestamp: 时间戳 要以字符串形式传入
    :return: 格式 1514-35-56
    """
    m = re.match(r"(\d{4})(\d{2})(\d{2})(\d{2})", timestamp)
    return "%s-%s-%s" % (m.group(1), m.group(2), m.group(3))


def str2timestamp(str, format='%Y-%m-%d %H:%M:%S'):
    """
    字符串格式日期转化为时间戳
    :param str: 2015-04-15 12:00:08
    :param format:格式
    :return:14090890.790
    """
    return time.mktime(time.strptime(str, format))


def createBatch(jobListName, ts):
    """

    :param jobListName:
    :param ts: 时间
    :return: 格式: qqq_time.struct_time(tm_year=2017, tm_mon=12, tm_mday=27, tm_hour=14, tm_min=25, tm_sec=51, tm_wday=2, tm_yday=361, tm_isdst=0)76376
    """
    #
    return "%s_%s%s" % (jobListName, ts, random.randint(10000, 99999))

def standard2ts(ts1):
    """
    :param ts1: u'2018-01-31T09:30:49.000Z'
    :return:
    """
    import re
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})T(\d{2})",ts1)
    return "%s%s%s%s" % (m.group(1), m.group(2), m.group(3), m.group(4))