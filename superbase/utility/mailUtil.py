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

import email
import os
import poplib
import smtplib
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import email.Header

from superbase.utility.accountUtil import AccountManager
from superbase.utility.ioUtil import gzipOneFile

from superbase.utility.logUtil import logException, getLogFileName, logInfo

from superbase.baseClass import BaseClass
from superbase.utility.processUtil import callMethod

# TODO,will be replaced by db
MAIL_SERVER = "mail.Server"
# 设置邮箱的账号密码授权码等
def getMailCfg():
   return AccountManager().getAccount(MAIL_SERVER)

class Mail(BaseClass):
    """
    继承BaseClass
    """

    def __init__(self, params="", cfg=None):
        BaseClass.__init__(self, params, cfg)
        # init cfg

    @staticmethod
    def send(f_address, t_address, msg):
        """
        发送
        SMTP服务是需要授权码
        :param f_address: 文件位置
        :param t_address:  时间节点
        :param msg: 错误的消息
        :return:
        """
        mailCfg = getMailCfg()
        s = smtplib.SMTP_SSL(mailCfg["smtp"][0], mailCfg["smtp"][1])
        s.login(mailCfg['From'], mailCfg['Pwd'])
        # 发送错误的性息
        error = s.sendmail(f_address, t_address, msg)
        # 打印错误的性息
        print error
        s.quit()

    @staticmethod
    def sendEmail(title, content, t_address=None, fmt="plain", attaches=None):
        """
        发送多个附件的邮件
        :param title:
        :param content:
        :param t_address:[mail list]
        :param fmt:
        :param attaches: [(filename,path)],...]
        :return:
        """
        mailCfg = getMailCfg()
        try:
            # 创建发送多个附件的邮件对象
            main_msg = MIMEMultipart()
            text_msg = MIMEText(content, fmt, 'utf-8')
            main_msg.attach(text_msg)
            main_msg['From'] = mailCfg['From']
            if not t_address:
                t_address = []
            t_address.extend(mailCfg["Admin"])

            main_msg['To'] = ";".join(t_address)
            main_msg['Subject'] = title
            main_msg['Date'] = email.Utils.formatdate()
            if attaches:
                for filename, path in attaches:
                    att1 = MIMEText(open(path, 'rb').read(), 'base64', 'gb2312')
                    att1["Content-Type"] = 'application/octet-stream'
                    att1["Content-Disposition"] = 'attachment; filename="%s"' % filename
                    main_msg.attach(att1)
            # 得到格式化后的完整文本
            fullText = main_msg.as_string()
            Mail.send(mailCfg['From'], t_address, fullText)
        except Exception:
            logException()

    @staticmethod
    def recvEmail():
        """
        设置email对象
        :return:
        """
        mailCfg = getMailCfg()
        M = poplib.POP3_SSL(mailCfg["pop"][0], mailCfg["pop"][1])
        M.user(mailCfg['From'])
        M.pass_(mailCfg['Pwd'])
        return M

    @staticmethod
    def report(title, content, t_address, jobName=None, batch=None, needLog=1):
        """
        报告
        :param title: 邮件标题
        :param content: 邮件内容
        :param t_address: 收件地址
        :param needLog: 0,no log,1,send job logFile,2,send node log,3 both
        :return:
        """
        attaches = []
        if needLog:
            try:
                jobLog, nodeLog = getLogFileName(batch, jobName)
                logInfo("jobLog=%s\nnodeLog=%s"%(jobLog,nodeLog))
                def getOneAttach(log):
                    if os.path.exists(log):
                        size = os.path.getsize(log)
                        if size > 1024 * 500:  # >500k,gizp it
                            log = gzipOneFile(log)
                        fname = os.path.split(log)[1]
                        attaches.append((fname, log))
                        logInfo("add one attach%s,size=%s"%(log,size))
                if needLog & 1:
                    getOneAttach(jobLog)
                if needLog & 2:
                    getOneAttach(nodeLog)
            except Exception:
                logException()
        # 发送邮件
        Mail.sendEmail(title, content, t_address, attaches=attaches)


if __name__ == '__main__':
    callMethod(Mail, sys.argv[1:])
