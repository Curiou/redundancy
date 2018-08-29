# -*- coding:utf-8 -*-
# -------------------------------------------------------------------------------
# Name:
# Author:      bhuang$
# -------------------------------------------------------------------------------
import sys
import time
sys.path.append(".")
from superbase.utility.logUtil import logInfo
from superbase.globalData import gConfig
from superbase.baseClass import BaseClass
from superbase.utility.processUtil import callMethod


class Sample3(BaseClass):
    """
    this sample is used for testing split and collect
    the web site is 58.cn

    """

    def __init__(self, params=None):
        # 添加这两个配置只是为了调试方便
        myCfg = {
            # CFG_JOB_BATCH:"split_test20140717",
            # CFG_JOB_NAME:"split",
            "env": "DEV"
        }
        BaseClass.__init__(self, params, myCfg)

    def test1(self, val1, val2):
        """
        这里演示的是：
        1，全局配置参数test.size的使用
        2，脚本调用的case
        python superbase/sample1.py "env=DEV,test.size=101"  test1 hello world
        python superbase/sample1.py "env=DEV,test.size=99"  test1 hello world

        :return:
        """
        size = gConfig.get("test.size", 0)
        if size > 100:
            logInfo("size=%s-%s" % (size,val1))
        else:
            logInfo("size=%s-%s" % (size,val2))


if __name__ == '__main__':
    callMethod(Sample3, sys.argv[1:])
