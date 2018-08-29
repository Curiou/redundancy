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
from superbase.globalData import gTop, gConfig, PROJECT_ROOT

from superbase.constant import INPUT_PARAM_COMMA, CFG_JOB_BATCH, INVALID_BATCHID, INVALID_JOB_NAME, \
    CFG_JOB_NAME, CFG_LOG_FILE_NAME, GD_CFG_IN, GD_LOGGER
from superbase.utility.ioUtil import getPrintDict
from superbase.utility.logUtil import createLogger, logInfo


class BaseClass(object):
    """
    baseClass 目前两大作用
    1,配置参数统一处理,传入的参数params可以统一以字符串输入,各分量用逗号分隔,如"beginTime=20151103150000,logLevel=20,inteval=0.5,browser=firefox"
    2,配置一个logger,子类可以override这个logger if needed
    """

    def __init__(self, params=None, subConfigDict=None):
        """
        :param params: 权限最高输入config,可覆盖所有,通常是命令行传入
        :param subConfigDict: 权限次高config,可覆盖父类,通常是子类固定设置或者用于
        :return:
        """

        newCfg = params or subConfigDict
        if newCfg:
            # 统一配置访问入口,会整合global,class,and input,可以用gConfig 统一访问
            # parseParams中转 把传递过来的str类型的配置 转换成dict类型的配置
            configIn = self.parseParams(params)  # input first
            if subConfigDict:
                # 如果subConfigDict非空 则把 处理好的配置参数 添加到subConfigDict
                subConfigDict.update(configIn)  # subClass second
            else:
                subConfigDict = configIn
            # 把配置添加到 全局数据单点配置中
            gTop.get(GD_CFG_IN).update(configIn)
            # 把子配置 更新到全局配置
            gConfig.update(subConfigDict)  #
            # 工作环境
            # upper()返回转换为大写的字符串的副本。
            gConfig.set("env", gConfig.get("env").upper())  # make sure capital


        # 创建日志记录器
        createLogger(gConfig)


    def parseParams(self, params):
        """
        这个方法主要是为方便子类 如果需要覆盖
        :param params:
        :return:
        """
        # 中转 把传递过来的str类型的配置 转换成dict类型的配置
        main_params = self.params2dict(params)
        return main_params

    def params2dict(self, params):
        """
        配置参数的解析
        把传递过来的str类型的配置 转换成dict类型的配置
        :param params:
        :return:
        """
        config = {}
        if params:
            params = params.split(",")  # 以逗号的为分割点
            for param in params:
                if param:
                    try:
                        eq = param.find("=")  # 查询参数中是否有 "="
                        key = param[:eq].strip()  # 等号左边的key
                        value = param[eq + 1:].strip()  # 等号右边的key

                        value = value.replace(INPUT_PARAM_COMMA, ",")  # 逗号在参数传递中以##替代
                        if value.isdigit():  # 检测字符串是否只由数字组成
                            try:
                                # 如果字符中有小数点 就是 转换为float类型 否则 为int类型
                                value = float(value) if value.find(".") >= 0 else int(value)
                            except Exception:
                                pass
                        # 把处理好的配置参数 并以dict写入到 config中
                        config[key] = value
                    except Exception:
                        print("param parse Error:%s" % param)

        return config

    def dict2params(self, dict1):
        """
        把传进来的dict类型 转换成str类型
        :param dict1:
        :return:
        """
        params = ""
        if dict1:
            # 正常的value
            def normalValue(value):
                # 把value中的","替换"##"
                value = str(value).replace(",", "##")
                return value

            # 拼接 把dict1字典遍历key,vaule出来 并与key,normalValue(value)进行拼接
            params = ",".join(["%s=%s" % (key, normalValue(value)) for key, value in dict1.items()])
        return params
