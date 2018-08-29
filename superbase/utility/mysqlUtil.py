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
import sqlite3
import threading
import traceback
import Queue
from threading import Thread

import MySQLdb

from superbase.globalData import gConfig
from superbase.utility.accountUtil import AccountManager
from superbase.utility.logUtil import logInfo, logError, logException, logDebug
from superbase.constant import CFG_DB_DISABLE, GD_ACCOUNTINFO, CFG_DB_DICTCURSOR


def createDb(dbNameKey, dbParams=None):
    """
    创建数据库
    :param dbNameKey: 目前只有db.monitor数据监控 and db.business数据业务
    :param dbParams: DEV and TEST 有默认参数，ONLINE需要通过jobManager分配
    :return:
    """
    from superbase.globalData import gConfig, gTop
    # CFG_DB_DISABLE 禁用,deprecated
    if gConfig.get(CFG_DB_DISABLE, 0):
        return
    # 获取数据监控或者数据业务
    dbName = gConfig.get(dbNameKey)

    try:
        from superbase.globalData import gConfig
        db_params = AccountManager().getAccount(dbNameKey) if not dbParams else dbParams
        if not db_params:
            return

        db = createDb2(dbName,db_params,dictCursor=gConfig.get(CFG_DB_DICTCURSOR,0))
        # 把数据业务或者数据监控和mysql连接 配置到 全局数据单点控制 中
        gTop.set(dbNameKey, db)
    except Exception:
        logException()

def createDb2(dbName,db_params,dictCursor=0):

    from superbase.utility.mysqlUtil import Mysql
    # 建立连接
    db = Mysql(database=dbName,
               host=db_params['host'],  # 连接的ip
               user=db_params['user'],  # 账号
               port=db_params['port'],  # 端口mysql默认端口3306 mongodb默认端口27017
               passwd=db_params['passwd'],
               dictCursor=dictCursor
               )
    return db

class DBWrapper(object):
    """
    数据库封装
    """

    def __init__(self):
        self.valueHolder = "%s"
        self.autoCommit = True
        self.cursors = []

    def setAutoCommit(self,enable):
        self.autoCommit = enable

    def close(self):
        """
        #关闭数据库 回收资源
        :return:
        """
        try:
            if self.conn:
                self.conn.close()
        except Exception:
            logException()
        self.conn = None

    def _getValue(self, value):
        """
        #获取 值
        :param value: value值
        :return:
        """
        if isinstance(value, basestring):
            result = "'%s'" % value
        else:
            result = str(value)
        return result

    def insert(self, table, params, retId=False):
        """
        插入数据
        :param table:表
        :param params:参数
        :param retId:
        :return:
        """
        fields = []  # 字段
        valuesPlaces = []  # 值的位置
        values = []  # 值
        for key, value in params.items():
            # 把传过来的参数遍历出来 key放在fields列表，value放在values列表中
            fields.append(key)
            valuesPlaces.append(self.valueHolder)
            values.append(value)

        # 插入 sql语句
        sql = "insert into {table} ({keys}) values ({values2})".format(table=table, keys=",".join(fields),
                                                                       values2=",".join(valuesPlaces))
        #logDebug("insert=-sql:%s"%sql)
        # 把values列表转换成values元组
        values = tuple(values)
        # 执行 插入sql命令
        cur = self.safeExecute(sql, values)
        self.commitTransaction(cur)
        if retId:
            lastId = self.getOne("select max(id) from %s " % (table))
            return lastId

    def update(self, table, params, condition=""):
        """
        更新数据
        :param table:
        :param params:
        :param condition:条件
        :return:
        """

        str1 = []
        values = []
        for key, value in params.items():
            # 把传过来的参数遍历出来 key与"="与valueHolder 拼接并添加到str1列表中，value添加到values列表中
            str1.append(key + "=" + self.valueHolder)
            values.append(value)
        # 拼接字段
        fields = ",".join(str1)
        # 更新的sql语句
        sql = "update %s set  %s %s" % (table, fields, condition)
        # 把values列表转换成values元组
        values = tuple(values)
        # 执行 更新sql命令
        cur = self.safeExecute(sql, values)
        self.commitTransaction(cur)
        from superbase.globalData import gConfig
        if gConfig.get("debug.sql", None):
            str1 = []
            for key, value in params.items():
                str1.append("`%s`='%s'" % (key, value))
            params2 = ",".join(str1)
            sql = "update {table} set {params} {condition}".format(table=table, params=params2, condition=condition)
            # 打印sql 语句
            logInfo(sql)

    def commitTransaction(self,cur):
        if self.autoCommit:
            # 关闭
            self.closeCursor(cur)
            # 数据提交
            self.conn.commit()
        else:
            self.cursors.append(cur)

    def commitTransaction2(self):
        for cur in self.cursors:
            self.closeCursor(cur)
        self.conn.commit()



    def query(self, sql):
        """
        查询数据
        :param sql:
        :return:
        """
        # 执行 查询sql命令
        cur = self.safeExecute(sql)
        # 查询所有
        data = []
        if cur:
            data = cur.fetchall()
            # 关闭
            self.closeCursor(cur)
        return data

    def closeCursor(self, cur):
        """
        关闭游标
        :param cur:掌舵者
        :return:
        """
        try:
            if cur:
                cur.close()
        except Exception, e:
            logException()


    def delete(self, table, condition):
        """
        删除数据
        :param table:
        :param condition:条件
        :return:
        """
        # 执行 删除sql命令
        cur = self.safeExecute("delete from %s %s" % (table, condition))
        self.commitTransaction(cur)

    def getOne(self, sql):
        """
        查询一个 数据
        :param sql:
        :return:
        """
        data = self.query(sql)
        ret = None
        if data:
            ret = data[0].values()[0] if self.dictCursor else data[0][0]
        return ret

    def getRow(self, sql):
        """
        查询一行 数据
        :param sql:
        :return:
        """
        data = self.query(sql)
        return [] if not data else data[0]

    def execute(self, sql):
        """
        数据库 执行sql语句的执行函数
        :param sql:
        :return:
        """

        # 打印 所执行sql 命令
        #print sql
        # 执行 sql语句
        cur = self.safeExecute(sql)
        # 查询所有
        data = cur.fetchall()
        self.commitTransaction(cur)
        return data

    def update2(self, table, params, condition=""):
        """
        把传过来的condition条件 在 table中 查询
        如果存在 则更新数据
        没有 则插入数据
        :param table:
        :param params:
        :param condition:
        :return:
        """
        result = self.getOne("select count(1) from %s %s" % (table, condition))
        if result:  # update
            self.update(table, params, condition)
        else:
            self.insert(table, params)


class Mysql(DBWrapper):
    """
    mysql 数据库
    """

    def __init__(self, database, host, user, passwd, port=3306,dictCursor=0):
        DBWrapper.__init__(self)
        # 创建连接
        self.curCfg = (database, host, user, passwd, port)
        self.dictCursor = dictCursor
        # 数据库 掌舵者
        self.conn = None
        self.resetDb()

    def resetDb(self):
        # 连接数据库
        tryTime = 10  # try 10次连接
        while tryTime > 0:
            try:
                self.close()
                database, host, user, passwd, port = self.curCfg
                from superbase.globalData import gConfig

                if self.dictCursor:
                    from MySQLdb.cursors import  DictCursor
                    self.conn = MySQLdb.connect(host=host, user=user, passwd=passwd,
                                                db=database, port=port, charset="utf8",
                                                cursorclass = DictCursor
                                                )
                else:
                    self.conn = MySQLdb.connect(host=host, user=user, passwd=passwd,
                                                db=database, port=port, charset="utf8"
                                                )
                if self.conn:
                    return

            except Exception:
                logException("tryTime-%s" % tryTime)
            tryTime -= 1

    def __enter__(self):
        """
        开始
        :return:
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        退出
        :param exc_type:
        :param exc_val:
        :param exc_tb:
        :return:
        """
        # print("close the db")
        self.close()

    def safeExecute(self, sql, values=None):
        """
        数据库 执行sql语句的 安全执行函数
        :param sql:
        :param values:
        :return:
        """

        # 同步 执行
        def asyncExe():
            # Queue 即队列，用来在生产者和消费者线程之间的信息传递
            q = Queue.Queue()

            def exe(conn, sql, values):
                try:
                    # 创建执行的游标
                    cur = conn.cursor()
                    # 如果values列表 非空
                    if values:
                        # 执行sql命令
                        cur.execute(sql, values)
                    else:
                        cur.execute(sql)
                    if gConfig.get("debug.sql",0):
                        logDebug(cur._last_executed)
                    # 把创建的游标 放到队列中 put 发送 出去
                    q.put(("ok", cur))
                except Exception:
                    err = traceback.format_exc()
                    # 错误 放到队列中 put 发送 出去
                    q.put(("error", err))

            # 开启多线程
            t1 = threading.Thread(target=exe, name='asyncExeSql', args=(self.conn, sql, values))
            # 开始
            t1.start()
            t1.join(30)  # 默认超时为30秒
            if t1.isAlive():  # 超时了
                return ("error", "it is !timeout!")
            else:
                # 获取消息队列 中的信息
                return q.get()

        try:
            ret = asyncExe()
            code, result = ret
            if code == "ok":
                return result
            else:
                err = result
                if err.find("MySQL server has gone away") >= 0 or \
                        err.find("Lost connection") >= 0 or \
                        err.find("Can't connect to MySQL") >= 0 or \
                        err.find("Unknown MySQL server host") >= 0 or \
                        err.find("!timeout!") >= 0:
                    self.resetDb()
                    return asyncExe()[1]
                else:
                    logException(err)
        except Exception:
            logException(ret)
        return None

    def escape(self, str):
        """
        :param str:
        :return:
        """
        if str.find("'") >= 0:
            # 引用字符串str中的任何sql解释字符。
            return MySQLdb.escape_string(str)
        else:
            return str


class Sqlite(DBWrapper):
    """
    Sqlite一个轻量级别数据库
    """

    def __init__(self):
        DBWrapper.__init__(self)
        self.valueHolder = "?"

    def reset(self, dbName):
        """
        #连接数据库
        :param dbName:
        :return:
        """
        self.conn = sqlite3.connect(dbName)

    def safeExecute(self, sql, values=None):
        """
        sql执行函数
        :param sql:
        :param values:
        :return:
        """
        try:
            # 创建 游标
            cur = self.conn.cursor()
            if values:
                # 执行sql命令
                cur.execute(sql, values)
            else:
                cur.execute(sql)
            return cur
        except Exception:
            logException()
