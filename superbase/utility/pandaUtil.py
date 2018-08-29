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
import sys



sys.path.append(".")
from superbase.baseClass import BaseClass
from superbase.utility.aliyun import AliYun
from superbase.utility.processUtil import callMethod
import pandas as pd
import numpy as np
from superbase.utility.logUtil import logDebug


class MyPanda(BaseClass):

    def __init__(self,params=""):
        BaseClass.__init__(self,params)

    def fromOSS(self,aliDir, timeRange=None):
        """

        :param aliDir: downData/www_tianyancha_com/detail/company_1
        :param timeRange: fmt:2018020100-2018020200
        :return:
        """
        lines = AliYun().readJsonFromAliyun(aliDir,timeRange)
        df = pd.DataFrame(line for line in lines)
        return df

    def printSchema(self,df):
        """
        模拟spark的做法，用100个随机row推测
        :param df:
        :return:
        """
        from superbase.utility.ioUtil import printDict
        df2 = df.sample(frac=0.1).head(100)
        result = {}
        #x:series,x.values:ndarray,values[0]:list
        df2.apply(lambda x:result.update(x.values[0][0]),axis=1)
        printDict(result)


    def test(self):
        df = self.fromOSS("downData/www_tianyancha_com/detail/company_1","2018022700-2018022800")
        def stat(record):
            """
            record 就是每个json结果
            :param record:
            :return:
            """
            business_info = record.get("business_info ", {})
            #数量
            annualReport_Num = len(business_info.get("annualReport",[]))
            #是否为空,1,0
            has_tianyan_risk = 1 if business_info.get("tianyan_risk",None) else 0
            #长度
            company_name_len = len(record.get("company_title", {}).get("company_name", ""))
            result = {
                "annualReport_Num":annualReport_Num,
                "has_tianyan_risk":has_tianyan_risk,
                "company_name_len":company_name_len
            }
            return result#(annualReport_Num,has_tianyan_risk,company_name_len)
        #TODO:series->list->df,应该可以直接优化成series->df
        data = df.apply(lambda x:stat(x.values[0][0]),axis=1).tolist()
        df2 = pd.DataFrame.from_dict(data)
        info = df2.describe()
        logDebug("""
        #####################\n
        this is the stat sample,you can try it and save the resulut to db for further visualization\n
        #####################\n
        %s\n
        #####################\n
        """%info)

        result = info.to_dict()
        #TODO,save the result










if __name__ == '__main__':
    callMethod(MyPanda, sys.argv[1:])
