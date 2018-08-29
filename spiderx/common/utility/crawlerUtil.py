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
import traceback
from HTMLParser import HTMLParser
from re import sub
from sys import stderr
from traceback import print_exc

from selenium.webdriver.support.ui import WebDriverWait
from superbase.utility.logUtil import logException
from spiderx.common.constant import CLICK_FAKE_SEC


def saveImage(driver, imgName):
    """
    保存图片
    :param driver:
    :param imgName:
    :return:
    """

    # webpath = gconfig.GIT_ROOT_WEB+"temp/"
    # driver.save_screenshot(imgName)
    # import platform
    # node = platform.node()
    # if node==gconfig.GIT_NODE:
    #     tmpFile = webpath+imgName
    #     driver.save_screenshot(tmpFile)
    # else:
    #     tmpDir = gconfig.ROOT_DATA+"temp/"
    #     if not os.path.exists(tmpDir):
    #         os.makedirs(tmpDir)
    #     tmpFile = tmpDir+imgName
    #     driver.save_screenshot(tmpFile)
    #     import processUtil
    #     processUtil.runProcess("scp %s %s:%s"%(tmpFile,gconfig.GIT_SERVER,webpath))


def getPageSync(driver, url, key, val,timeout=0):
    """
    获取页面同步
    :return:
    """
    element = None
    try:

        driver.get(url)
        if key:
            TIME_OUT_SEC = timeout if timeout else 10
            element = WebDriverWait(driver, TIME_OUT_SEC).until(lambda x: x.find_element(key, val))
    except Exception:
        logException()
    return element


def getElementSync(driver, keyVal,timeout=0):
    """
    获取元素同步
    :param driver:
    :param keyVal: (key,val),eg.（By.ID,"id1"）
    :return:
    """
    element = None
    try:
        key, val = keyVal
        TIME_OUT_SEC = timeout if timeout else 10
        # 直到 找到元素
        element = WebDriverWait(driver, TIME_OUT_SEC, 0.1, True).until(lambda x: x.find_element(key, val))
    except Exception:
        logException()
    return element


def u(s, encoding):
    """
    #编码
    :param s:
    :param encoding:
    :return:
    """
    if isinstance(s, unicode):
        # 实例化
        return s
    else:
        return unicode(s, encoding)


def clickWait(link, t=CLICK_FAKE_SEC):
    """
    #点击等待
    :param link:
    :param t:
    :return:
    """
    link.click()  # 点击链接
    time.sleep(t)  # 睡眠时间


def findElement(parent, key, val):
    """
    #查询元素
    :param parent:
    :param key:
    :param val:
    :return:
    """
    try:
        # 从父节点 查询元素
        return parent.find_element(key, val)
    except Exception:
        # myLog.debug("can not find "+val)
        print "can not find " + val
        return None


def findElements(parent, key, val):
    """
    #查询 所有元素
    :param parent:
    :param key:
    :param val:
    :return:
    """
    try:
        # 从父节点 查询所有元素
        return parent.find_elements(key, val)
    except Exception:
        # myLog.debug("can not find "+val)
        print "can not find " + val
        return None


def encoded_dict(in_dict):
    """
    对于传过来的dict类型 参数 进行解码和编码
    :param in_dict:
    :return:
    """
    out_dict = {}
    for k, v in in_dict.iteritems():
        if isinstance(v, unicode):
            # 编码
            v = v.encode('utf8')
        elif isinstance(v, str):
            # Must be encoded in UTF-8
            # 解码
            v.decode('utf8')
        out_dict[k] = v
    return out_dict


class _DeHTMLParser(HTMLParser):
    """
    #解析原网页 去掉不想要的字符
    """

    def __init__(self):
        HTMLParser.__init__(self)
        self.__text = []

    def handle_data(self, data):
        # 剃掉文本中所有 空格
        text = data.strip()
        if len(text) > 0:
            # 正则替换掉text中\t\r\n
            text = sub('[ \t\r\n]+', ' ', text)
            # 并添加到__text 列表中
            self.__text.append(text + ' ')

    def handle_starttag(self, tag, attrs):
        if tag == 'p':
            # 处理 p标签
            self.__text.append('\n\n')
        elif tag == 'br':
            # 处理 br标签
            self.__text.append('\n')

    def handle_startendtag(self, tag, attrs):
        if tag == 'br':
            # 处理br标签
            self.__text.append('\n\n')

    def text(self):
        # 拼接 并剔掉 __text中的空格
        return ''.join(self.__text).strip()


def dehtml(text):
    """

    :param text:
    :return:
    """
    try:
        # 解析html 去掉不想要的字符
        parser = _DeHTMLParser()
        # 向解析器提供数据。
        parser.feed(text)
        # 关闭
        parser.close()
        return parser.text()
    except:
        # 实际值类型<类型'文件>替换
        print_exc(file=stderr)
        return text


def fillInput(e, value):
    """
    填充输入
    :param e:
    :param value:
    :return:
    """
    # 清除
    e.clear()
    # 发送参数
    e.send_keys(value)
