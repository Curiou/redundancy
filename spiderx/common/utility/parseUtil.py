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
import re
from HTMLParser import HTMLParser
from StringIO import StringIO
from re import sub

from lxml import etree
from pyquery import PyQuery as pq
from superbase.globalData import gConfig
from superbase.utility.ioUtil import getPrintDict
from superbase.utility.logUtil import logException

from spiderx.common.utility.antiBlockUtil import AntiBlock
from superbase.constant import CFG_DEBUG_SAVEFILE, CFG_DEBUG_SAVEFILENAME
from spiderx.common.constant import CFG_HTTP_OUTFORMAT


def unescape(content):
    """
    处理转义字符，将类似于&lt;p&gt;，这种HTML转义符转义成<p>
    :param content: 请求网页的内容
    :return: 返回经过转义处理的网页内容
    """

    # 导入转义模块
    import HTMLParser
    html_parser = HTMLParser.HTMLParser()
    txt = html_parser.unescape(content)  # 进行转义
    return txt


class _DeHTMLParser(HTMLParser):
    """
    处理分行，分段
    """

    def __init__(self):
        HTMLParser.__init__(self)
        self.__text = []

    def handle_data(self, data):
        """
        处理字符串，将字符串两端，中间的空白字符去除
        :param data:
        :return:
        """
        text = data.strip()  # 去除字符串两端的空白字符
        if len(text) > 0:
            text = sub('[ \t\r\n]+', ' ', text)  # 替换字符串中 \t\r\n
            self.__text.append(text + ' ')  # 拼接

    def handle_starttag(self, tag, attrs):
        """
        处理html中的起始标签
        :param tag: 标签
        :param attrs: 此处用不到
        :return:
        """
        if tag == 'p':
            self.__text.append('\n\n')  # 遇到p标签时，换行两次
        elif tag == 'br':
            self.__text.append('\n')  # 遇到br时，换行一次

    def handle_startendtag(self, tag, attrs):
        """
        处理结尾标签
        :param tag:
        :param attrs:
        :return:
        """
        if tag == 'br':
            self.__text.append('\n\n')  # 遇到br时，换行两次

    def text(self):
        """
        拼接内容后去除两端空白字符
        :return:
        """
        return ''.join(self.__text).strip()


def dehtml(text):
    """
    接收相应的HTML内容，并进行解析
    :param text: HTML内容
    :return: 解析后的HTML内容
    """
    try:
        parser = _DeHTMLParser()
        parser.feed(text)  # 将html数据传给解析器进行解析
        parser.close()  # 当遇到文件结束标签后进行的处理方法
        return parser.text()
    except:
        return text


HTML_MULTI_LINE = 1


class Extractor:
    """
    下载内容提取（解析）器
    """
    HTML_MULTI_LINE = 1

    def __init__(self, http, parser):
        self.http = http
        self.parser = parser

    @staticmethod
    def getValue(item, css, attr,text=True):
        """
        :param item:
        :param css:可以为空,表示取item的attr
        :param attr:
        :return: 返回去除两端空白字符的值，如果没有则返回空字符串
        """
        if not attr:
            value = item(css).text() if text else item(css).html() # 如果没有属性，则取该节点的text文字
        else:
            value = item(css).attr(attr) if css else item.attr(attr)  # 有属性值，并且有css定位，则获取该css定位的值，如果没有css，就返回当前节点的属性值

        return value.strip() if value else ""

    @staticmethod
    def getResult(parent, template, result):
        """
        :param parent: pyquery
        :param template: key:value, value is one of CssElement,ListElement,EmmbedElement
        :param result: 保存结果的字典
        :return:
        """
        try:
            # 遍历传入元素的节点
            for key, element in template.items():
                value = element.parse(parent)  # 保存到字典中
                result[key] = value
        except Exception:
            logException()  # 记录异常到log日志

    def getResultByUrl(self, url, template, result):
        """
        获取请求页面的content数据
        :param url:
        :param template:
        :param result:
        :return: 返回请求页面的content数据
        """

        content = self.http.get(url)  # 对url发送get请求，获取内容

        if content in(u' ',u'',None):
            return None

        # 获取配置参数CFG_DEBUG_SAVEFILE，
        if gConfig.get(CFG_DEBUG_SAVEFILE):
            AntiBlock.saveWrongPage(content,
                                    gConfig.get(CFG_DEBUG_SAVEFILENAME))  # 如果有，,调用AntiBlock.saveWrongPage方法，记录错误页面的消息
        format = gConfig.get(CFG_HTTP_OUTFORMAT, "html")
        if format == "json":  # 返回格式是json,不用解析
            result.update(content)  # 更新
        elif format == "html":
            self.getResultByContent(content, template, result)  # 不符合条件，调用getResultByContent方法
        elif format == "file":
            result["file"] = content

        return content

    def getResultByContent(self, content, template, result):
        """
        通过网页源码获取结果
        :param content: 网页源码
        :param template:
        :param result: 保存结果
        :return:
        """
        root = pq(etree.parse(StringIO(content), self.parser).getroot())
        self.getResult(root, template, result)


class EmmbedElement(object):
    """
    嵌入式元素的结果获取
    """

    def __init__(self, embedDict):
        self.embedDict = embedDict

    def parse(self, parent):
        """
        解析获取数据
        :param parent: 上一级元素
        :return: 解析后得到的数据
        """
        result = {}  # 保存结果的字典
        Extractor.getResult(parent, self.embedDict, result)  # 调用Extractor.getResult方法，获取数据
        return result


class ListElement(object):
    """
    获取列级数据
    """

    def __init__(self, parentCss, itemCssElement):
        self.parentCss = parentCss
        self.itemCssElement = itemCssElement

    def parse(self, parent):
        """
        解析获取列级数据
        :param parent: 上一级元素
        :return: 返回一个列表，保存所获取的列表节点
        """
        parent2s = parent(self.parentCss)
        array = []  # 保存结果

        # 遍历节点，通过CSS提取器获取结果
        for idx, tr in enumerate(parent2s):
            result2 = {}
            Extractor.getResult(pq(tr), self.itemCssElement, result2)
            array.append(result2)
        return array


class CssElement(object):
    """
    最常用的解析方式就是用CSS Selector，请学习pyquery
    """

    def __init__(self, css, attr=None, handler=None,text=True):
        """
        :param css: element css selector，可通过chrome 调试器获得基本路径
        :param attr:如果提取的是element的attribute，
        :param handler:None 就是取值，否则用合适的handler，如RegexHandler，NextLevelHandler
                最灵活是设定SpecialHandler，自己处理
        :param text:默认返回文本，如果False，返回html
        :return:
        """
        self.css = css
        self.attr = attr
        self.handler = handler
        self.text = text

    def parse(self, parent):
        """
        解析网页，获取数据
        :param parent: 父级节点
        :return: 解析网页后获取的值
        """
        value = ""
        try:
            if not self.handler:
                # 如果没有传入获取数据值的方法
                value = Extractor.getValue(parent, self.css, self.attr,self.text)
            else:
                # 传入自定义解析数据的方法
                value = self.handler.handle(parent, self.css, self.attr)
        except Exception:
            logException("selector-error:css=%s attr=%s" % (self.css, self.attr))  # 记录异常到log日志
        return value


class RegexHandler(object):
    """
    对CSS Selector处理后的数据进行正则处理
    """

    def __init__(self, pat):
        """
        正则处理
        :param pat: compiled pattern
        """
        self.debugInfo = pat
        self.pat = re.compile(pat)  # 编译正则表达式

    def handle(self, parent, css, attr):
        """
        解析获取数据
        :param parent: 上一级
        :param css: css定位路径
        :param attr: 选取的属性
        :return: 返回正则处理后的值
        """
        value = ""
        try:
            value = Extractor.getValue(parent, css, attr)  # 通过CSS Selector获取数据
            m = self.pat.search(value)  # 进行正则匹配
            value = m.group(1).strip()  # 选取匹配的第一个元素，病去除两端空白
        except Exception:
            logException(
                "regex-error:value=%s css=%s attr=%s pat=%s" % (value, css, attr, self.debugInfo))  # 记录异常到log日志
        return value


class NextLevelHandler(object):
    """
    处理下一级url,如detail页面
    """

    def __init__(self, nextLevelConf, nextLevelFunc, callBackParams=None):
        """
        :param nextLevelConf: 下一个页面的解析配置
        :param nextLevelFunc: func(url,conf,callBackParams)
        :param callBackParams: nextLevelFunc 需要的其他回调参数
        :return:
        """
        self.conf = nextLevelConf
        self.func = nextLevelFunc
        self.otherParams = callBackParams

    def handle(self, parent, css, attr):
        """
        解析获取数据
        :param parent: 上一级
        :param css: css定位下一级url的路径
        :param attr: 属性
        :return: 获取的值
        """
        value = ""
        try:
            url = Extractor.getValue(parent, css, attr)  # 通过CSS Selector获取下一级页面的url
            value = self.func(url, self.conf, self.otherParams)  # 通过回调函数获取下一级页面的数据
        except Exception:
            logException()  # 记录异常到log日志
        return value


class SpecialHandler(object):
    """
    如果无法简单地通过regex or nextLevel 来处理，就完全手动处理
    """

    def __init__(self, func, callBackParams=None):
        """
        :param func: 处理函数，prototype func(parent, css, attr,callBackParams)
        :param callBackParams: 回调参数
        """
        self.func = func
        self.otherParams = callBackParams

    def handle(self, parent, css, attr):
        """
        解析获取数据
        :param parent: 上一级
        :param css: css定位路径
        :param attr: 属性
        :return: 获取的值
        """
        value = ""
        try:
            value = self.func(parent, css, attr, self.otherParams)  # 通过回调函数获取页面的数据
        except Exception:
            logException()  # 记录异常到log日志
        return value


if __name__ == '__main__':

    try:
        # obj = Extractor()
        content = """
            <html>
                <p>this is a test
                <div id="atest_JK">hello</div>
                <div id="atest_JK2">world</div>
                </p>
                <table>
                    <tr><td>sliu</td><td>ceo</td><td>50</td><td>huyin</td></tr>
                    <tr><td>syuan</td><td>cto</td><td>35</td><td>zhuan</td></tr>
                    <tr><td>bhuang</td><td>coo</td><td>15</td><td>fan</td></tr>
                </table>
                <div><p>wacai<p><h2>50<h2></div>
            </html>
            """
        parser = etree.HTMLParser(encoding='utf-8')
        root = pq(etree.parse(StringIO(content), parser).getroot())

        trs = root("[id*=JK]")
        result = {}

        print(getPrintDict(result))
        # applyFunc(obj, sys.argv[1], sys.argv[2:])

    except Exception:
        logException()
