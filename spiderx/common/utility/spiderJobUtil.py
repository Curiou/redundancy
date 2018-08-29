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

from spiderx.common.utility.resultUtil import SyncPoint
from superbase.constant import ET_RUN_UNKNOWN, CFG_JOB_ENABLE
from superbase.globalData import gConfig
from superbase.utility.jobUtil import JobUtil

sys.path.append(".")


class SpiderJobUtil(JobUtil):
    def __init__(self):
        JobUtil.__init__(self)

    def jobBegin(self):
        """
        #工作开始
        :return:
        """
        JobUtil.jobBegin(self)
        return SyncPoint()

    def jobDone(self,lastResult=None):
        """
        #工作完成
        :return:
        """
        JobUtil.jobDone(self)
        # 并检查同步
        sp = SyncPoint()
        if lastResult:
            sp.saveLastSyncInfo(lastResult)
        sp.checkSync()

    def jobFail(self):
        """
        #工作失败
        :return:
        """
        JobUtil.jobFail(self)
        if self.job:
            SyncPoint().checkSync()

    def jobHandleError(self, errType=ET_RUN_UNKNOWN):
        """
        #工作处理错误
        :param errType: ET_RUN_UNKNOWN=404
        :return:
        """
        JobUtil.jobHandleError(self,errType)
        if self.job:
            SyncPoint().checkSync()

