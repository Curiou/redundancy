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
from superbase.baseClass import BaseClass
from superbase.utility.jobUtil import JobUtil


class BaseJob(BaseClass,JobUtil):
    """
    baseClass 目前两大作用
    1,配置参数统一处理,传入的参数params可以统一以字符串输入,各分量用逗号分隔,如"beginTime=20151103150000,logLevel=20,inteval=0.5,browser=firefox"
    2,配置一个logger,子类可以override这个logger if needed
    """

    def __init__(self, params=None,subCfg=None):
        BaseClass.__init__(self,params,subCfg)
        JobUtil.__init__(self)

