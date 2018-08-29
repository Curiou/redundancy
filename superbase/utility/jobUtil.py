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

import functools


from superbase.constant import ET_RUN_UNKNOWN, CFG_JOB_ENABLE, CFG_JOB_NAME, INVALID_JOB_NAME, CFG_JOB_ID
from superbase.globalData import gConfig

sys.path.append(".")
from superbase.utility.logUtil import logException, logInfo, logError


def JobWrap(self):
    def wrap1(func):
        @functools.wraps(func)
        def __decorator(*params):

            try:
                self.jobBegin()
                return func(*params)
            except Exception, e:
                self.jobError = e
                logException()
            finally:
                if self.jobError:
                    self.jobFail()
                    logError("jobFail because:%s"%self.jobError)
                else:
                    self.jobDone()

        return __decorator
    return wrap1
class JobUtil(object):
    def __init__(self):
        #有jobId 才有jobEnable
        gConfig.set(CFG_JOB_ENABLE, gConfig.get(CFG_JOB_ID,0))
        if gConfig.get(CFG_JOB_ENABLE, 0):
            # jobManger 任务管理器
            from jobManager.job import Job
            self.job = Job()
        else:
            self.job = None
        self.jobError = None

    def jobBegin(self):
        """
        #工作开始
        :return:
        """
        self.jobError = None
        if self.job:
            self.job.begin()


    def jobDone(self):
        """
        #工作完成
        :return:
        """
        # 并检查同步
        if self.job:
            self.job.done()
        logInfo("--job done")

    def jobFail(self):
        """
        #工作失败
        :return:
        """
        if self.job:
            self.job.fail()
        logInfo("--job fail")
    def jobClose(self):
        """
        End 是指jobGroup-end(不再有batch),而Done是指batch-job 结束
        :return:
        """
        if self.job:
            self.job.end()
        logInfo("--job close")

    def jobHandleError(self, errType=ET_RUN_UNKNOWN):
        """
        #工作处理错误
        :param errType: ET_RUN_UNKNOWN=404
        :return:
        """
        if self.job:
            self.job.prepareForErrorHandle(errType)

    def jobHearBeat(self):
        if self.job:
            self.job.heartBeat()

    def jobStatusInfo(self,data):
        if self.job:
            self.job.addStatusInfo(data)