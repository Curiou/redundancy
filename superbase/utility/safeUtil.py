# -*- coding: utf-8 -*-
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
import functools

import time
from httplib import HTTPException



from superbase.utility.logUtil import logException

from superbase.globalData import gConfig


def exceptionWrap(func):
    @functools.wraps(func)
    def __decorator(*args):
        try:
            return func(*args)
        except Exception, e:
            logException()
        return 0

    return __decorator


def strMaxLen(obj1, maxLen=100):
    """
    str 最大长度
    :param obj1:目标文件1
    :param maxLen:最大长度
    :return:
    """
    if obj1 and isinstance(obj1, basestring):
        # 取前100
        ret = obj1[:maxLen]
        return ret
    else:
        return obj1


def safeGetItem(collection, idx, default="", maxLen=100):
    """
    获取项目
    :param collection:集合
    :param idx:
    :param default:默认的
    :param maxLen:最大长度
    :return:
    """
    ret = collection[idx] if collection and len(collection) > idx else default
    if isinstance(ret, basestring) and maxLen and len(ret) > maxLen:
        return ret[:maxLen]
    return ret


def safeGetDictItem(dict1, key, default="", maxLen=100):
    """
    获取Dict类型项
    :param dict1:
    :param key:
    :param default: 默认的
    :param maxLen: 最大长度
    :return:
    """
    ret = dict1.get(key, default)
    if isinstance(ret, basestring) and maxLen and len(ret) > maxLen:
        return ret[:maxLen]
    return ret


def mergeArray(resultArray, newArray, keys=None):
    """
    合并两个数组
    :param resultArray:　被合并的ａｒｒａｙ
    :param newArray: 新数据，被合并到resultArray
    :param keys=(itemKey,subKey): 具体用法参见cp2person person.merge方法
           itemkey，空表示ｖａｌｕｅ类型的ａｒｒａｙ，否则是ｄｉｃｔ类型的ａｒｒａｙ ｅｇ．person.cells vs person.cps
           subKey: 如果为空，就是简单ｄｉｃｔ，全部ｍｅｒｇｅ，否则根据ｋｅｙ对应ｖａｌｕｅ类型，ｅｇ．person.followed,可递归
    :return:
    """
    itemKey, subKey = keys if keys else (None, None)
    for item in newArray:
        if item not in resultArray:
            if not itemKey:
                if item not in resultArray:
                    resultArray.append(item)
            else:
                upsertArrayItem(resultArray, item, (itemKey, item[itemKey]), subKey)


def upsertArrayItem(array, newItem, (key, value), mergeItemKey=None):
    """
    插入数组项
    :param array:数组
    :param newItem:新的项目
    :param mergeItemKey: (key1,subkey),eg.person.followed=>('cfas','cfa_id))
    :return:
    """
    update = False
    for idx, item in enumerate(array):
        value2 = item.get(key, None)
        if value2 and value2 == value:  #
            if not mergeItemKey:
                array[idx] = newItem
            else:
                key1, subkey = mergeItemKey
                val = newItem.get(key1, None)
                if val:
                    if isinstance(val, list):
                        target = getItemByKey(array[idx], key1, [])
                        mergeArray(target, val, subkey)

            update = True
            break
    if not update:
        array.append(newItem)


def getArrayRecordItem(parent, arrayName, key, idx=0, defVal=""):
    """
    获取数组记录项
    :param parent: eg. uc.standard
    :param arrayName: eg. names
    :param key: eg. full
    :param idx: eg.0
    :return:
    """
    result = ""
    array = parent.get(arrayName, [])
    if array:
        item = safeGetItem(array, idx)
        if item:
            result = item.get(key, "")
    ret = result if result else defVal
    return ret


def getItemByKey(parent, key, itemType):
    """
    用于upsert时，取parent
    :param parent:
    :param key:
    :param default: 默认的
    :return:
    """
    item = parent.get(key, None)
    if item == None:
        parent[key] = itemType
    return parent[key]


def singleton(cls):
    """
    单例
    :param cls:
    :return:
    """
    instances = {}

    def get_instance():
        if cls not in instances:
            instances[cls] = cls()
        return instances[cls]

    return get_instance()


def isOnDevelop():
    """
    在开发
    :return:
    """
    # local 本地
    # upper 字符串中的小写字母转为大写字母。
    return gConfig.get("env").upper() == "LOCAL"


def groupByKey(kvlist):
    """
    :param kvlist: [(k,v),]
    :return: {k1:[v1,v2],...}
    """
    byName = lambda x: x[0]
    data = sorted(kvlist, key=byName)
    result = {}
    from itertools import groupby
    for m, n in groupby(data, key=byName):
        result[m] = [item[1] for item in n]
    return result


def getCleanItem(clean, ori, maxLen=100):
    """
    获取清理好的项目
    :param clean:清理
    :param ori:
    :param maxLen:最大长度
    :return:
    """
    if clean:
        result = clean
    else:
        result = ori
    return result[:maxLen] if result else None


def safeEval(data, ret=None):
    """
    :param data: e.g {'a':1}
    :return: None if error else the result
    """
    try:
        if data:
            import ast
            # literal_eval 安全评估表达式节点或包含Python的字符串表达式。
            return ast.literal_eval(data)
    except Exception:
        from superbase.utility.logUtil import logException
        logException(data)

    return ret


def safeReg1(reg, str, tag):
    """
    :param reg: pattern
    :param str:
    :param tag: for debug
    :return:
    """
    try:
        # search搜索 str
        return reg.search(str).group(1)
    except Exception:
        logException("regError:%s--%s" % (tag, str))


def TryTime(maxTry, tsleep=1):
    """
    多用于网络请求
    :param maxTry:
    :param tsleep:
    :return:
    """

    def wrap1(func):
        @functools.wraps(func)
        def __decorator(*params):
            tryTime = 0
            while tryTime < maxTry:
                try:
                    return func(*params)
                except Exception:
                    logException("url=%s"%params[0])
                tryTime += 1
                time.sleep(tsleep)

        return __decorator

    return wrap1


def DriverTryTime(maxTry=0, tsleep=1):
    """
    多用于网络请求
    :param maxTry:
    :param tsleep:
    :return:
    """
    from selenium.common.exceptions import WebDriverException
    def wrap1(func):
        @functools.wraps(func)
        def __decorator(*params):
            tryTime = 0
            if maxTry:
                while tryTime < maxTry:
                    try:
                        return func(*params)
                    except (WebDriverException, HTTPException):
                        logException()
                    tryTime += 1
                    time.sleep(tsleep)
            else:
                while True:
                    try:
                        return func(*params)
                    except (WebDriverException, HTTPException):
                        logException("tryTime=>%d" % tryTime)
                    tryTime += 1
                    time.sleep(tsleep)

        return __decorator

    return wrap1
