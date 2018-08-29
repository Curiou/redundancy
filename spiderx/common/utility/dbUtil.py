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

import os
import sys

from spiderx.common.constant import CFG_DOWN_WEBSITE
from superbase.baseClass import BaseClass
from superbase.constant import CFG_JOB_ENABLE
from superbase.globalData import PROJECT_ROOT, gConfig
from superbase.utility.ioUtil import mkdir
from superbase.utility.mysqlUtil import Sqlite
from superbase.utility.processUtil import callMethod

tables = {

    "syncPoint": """ 
        
        CREATE TABLE  IF NOT EXISTS syncPoint(
               id            TEXT    PRIMARY KEY NOT NULL,
               idx           TEXT    NOT NULL,
               result        TEXT    NOT NULL,
               syncInfo      TEXT    ,
               upTime        TEXT    NOT NULL
        );
        """,


}


class LocalDb(BaseClass):
    """
    本地数据库
    每个爬虫维护一个local sqlite db，用于保存同步信息，cookie等
    """

    def __init__(self):
        BaseClass.__init__(self)
        self.sqlite = Sqlite()
        # 创建本地数据库
        self.createLocalDb()

    def getDb(self):
        """
        :return:
        """
        return self.sqlite

    def createTable(self, name, sql=None, forceNew=0):
        """
        创建表
        :param name:
        :param sql:
        :param forceNew:
        :return:
        """

        forceNew = int(forceNew)
        if forceNew:
            # execute 执行sql命令
            self.sqlite.execute("drop table %s" % name)
        if not sql:
            sql = tables[name]
        self.sqlite.execute(sql)

    def createLocalDb(self):
        """
         #创建本地数据库
        create the local sqlite db
        PROJECT_ROOT/{workDir}/localDb/local.db 存储路径
        :return:
        """
        # 工作目录
        workDir = "spider/localDb" if gConfig.get(CFG_JOB_ENABLE, 0) == 0 else "sites/localDb"
        # 路径=根目录+工作目录
        path = os.path.join(PROJECT_ROOT, workDir)
        # 创建路径文件夹
        mkdir(path)
        # 文件名称
        fileName = os.path.join(path, "local.db")
        # 连接数据库
        self.sqlite.reset(fileName)
        for table in tables.values():
            self.sqlite.execute(table)


if __name__ == '__main__':
    callMethod(LocalDb, sys.argv[1:])
