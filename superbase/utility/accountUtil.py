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
import sys

sys.path.append(".")
from superbase.constant import CFG_ACCOUNT_PROVIDER
from superbase.globalData import gConfig

class AccountManager(object):
    """
    账号管理策略：
    AM只提供接口，通过配置传入实际provider name，动态load
    一个环境（DEV，TEST，ONLINE）配置一个git，每个git一个provider
    """
    def __init__(self,provider=None):
        """
        :param provider: module or full name of module,eg, xxAccount.provider
        """
        self.provider = gConfig.get(CFG_ACCOUNT_PROVIDER,provider)
        if isinstance(self.provider,basestring):
            import importlib
            self.provider = importlib.import_module(self.provider)

    def getAccount(self,accountKey,**params):
        """
        :param accountKey:定义在CFG
        :param params:
        :return:返回账号dict
        """
        if self.provider:
            return self.provider.getAccount(accountKey,**params)



