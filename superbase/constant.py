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

JOB_RESULT_OK = "!SpiderX_Task_Done!"
# job status

JS_SUBMITTED = 1  # requester 提交
JS_ASSIGNED = 2  # manager 已经assign worker
JS_READY = 3  # worker已经分配资源(如账号),可以准备work 或者已经launched但未确认是否运行
JS_ERROR = 404  # 某种错误原因无法执行,通常就是blocked
JS_CLOSED = 500  # job/batch 被关闭
JS_RUNNING = 100
JS_PAUSED = 4  # TBD,not used so far
JS_FAILED = 505  # fail 不是 error,可以用于质量检测,质/量不合格,可以视为错误
JS_KILLED = 911  # 被强杀
JS_LOST = 301  # 没有心跳
JS_DONE = 200
JS_LOCKING = 66  # 任务在状态间转移时,加上lock,防止重复调度,TODO:全局锁

# job type
JT_SPIDER = "spider"
JT_CLEANER = "clean"
JT_TRAINER = "trainer"
JT_SPLITER = "splitter"
JT_COLLECTOR = "collector"
JT_SAVE2OSS = "save2oss"

# NODE type
NT_WORKER = "worker"
NT_FOREMAN = "foreman"

# NODE status
NS_HANG = 400  # 挂起
NS_RUNNING = 100
NS_LOST = 301  # 没有heartbeat
NS_CLOSED = 500  # 被废弃的节点，等待删除

# role status
RS_RUNNING = NS_RUNNING
RS_STOP = 200
RS_PAUSE = 4
RS_LOST = NS_LOST  # 没有heartbeat
RS_HANG = NS_HANG
RS_KILL = 911 #这个用于强制杀角色不再分配新任务，但又能保证当前所有任务退出

# error handle status
EHS_REPORT = 1
EHS_HANDLE = 2

# error type
ET_READY_NOTRUN = 400  #
ET_RUN_LOST = 401  #
ET_RUN_HANG = 402  #
ET_RUN_BLOCK = 403  #
ET_RUN_UNKNOWN = 404
ET_ROLE_LOST = 301
# remote method invoke
RMI_REQUEST = 2
RMI_RUNNING = 100
RMI_RESPONSE = 200

EXCEPTION_PREFIX = "!myex!"  # 异常的前缀
INVALID_JOB_NAME = 'undef_job'  # 无效的 工作名称
INVALID_BATCHID = "no_batchId"  # 没有分批处理的id





CFG_BATCH_CANCLE = "batch.cancel"  # 用于submit jobList时关闭现有batch

CFG_LOG_CONSOLE_LEVEL = "log.consoleLevel"  # 控制台标准
CFG_LOG_FILE_LEVEL = "log.fileLevel"  # 文件标准
CFG_LOG_DB_LEVEL = "log.dbLevel"  # 数据库标准
CFG_LOG_FILE_NAME = "log.fileName"  # 文件名称
CFG_LOG_MAX_EXCEPTION = "log.maxException"  # 最大异常
CFG_LOG_DISABLE_DB = "log.disableDb"  # 禁用数据库
CFG_LOG_DISABLE_FILE = "log.disableFile"  # 禁用文件

CFG_NODE_CHECKTIME = "node.checkTime"  # 健康检查时间
CFG_NODE_WORKS = "node.works"  # 默认节点可以做任何工作，可以传入JT_SPIDER,JT_TRAINER等限定用途，多个工作类型间用逗号隔开

CFG_DB_DICTCURSOR = "db.dictCursor" #是否用dictCursor
CFG_DB_DISABLE = "db.disable"  # 禁用
CFG_DB_MONITORNAME = "db.monitorName"  # 监控名称
CFG_DB_BUSINESSNAME = "db.businessName"  # 业务名称
CFG_DB_BUSINESS = "db.business"  # 业务
CFG_DB_MONITOR = "db.monitor"  # 监控
CFG_DB_OFFSET = "db.offset"  #
CFG_DB_LIMIT = "db.limit"  #
CFG_DEBUG_LOCALNODE = "debug.localNode"  # 本地节点
CFG_DEBUG_LOCALTEST = "debug.localTest"  # 本地测试
CFG_DEBUG_SAVEFILE = "debug.saveFile"  # 存储下载文件
CFG_DEBUG_SAVEFILENAME = "debug.saveFileName"  # 存储下载文件名称

CFG_ERROR_CALLBACKIDX = "error.callbackIdx"  # 一个回调数字,用于表征一个任务的错误处理
CFG_ERROR_READY_INTERVAL = "error.readyInterval"  # ready后无法进入running的最大时间间隔,默认120s
CFG_ERROR_RUNNING_INTERVAL = "error.runInterval"  # run后没有更新日志的的最大时间间隔,默认1800s

CFG_ACCOUNT_PROVIDER = "account.provider" #账户提供者

INPUT_PARAM_COMMA = "##"  # 逗号在参数传递中以##替代


def IS_ERROR(ret):
    return ret != 0

CFG_JOB_NAME = "job.name"  # 工作名称
CFG_JOB_BATCH = "job.batch"  # 分批处理
CFG_JOB_ID = "job.id"  # 工作id
CFG_JOB_DEBUG = "job.debug"  # debug mode,only test the crawler itself,and exclude database
CFG_JOB_HEARTBEAT = "job.heartBeat"  # 任务的心跳间隔,这个也作为需要健康检查的标识
CFG_JOB_ENABLE = "job.enable"  # 启用任务调度，目前只在online/test时候用
CFG_JOB_EMAIL = "job.email"  # job负责人email
CFG_JOB_TABLE = "job.table"  # job相关的业务表
CFG_JOB_IDRANGE = "job.idRange"  # job要处理的id range
CFG_JOB_BEGINTIME = "job.beginTime"  # 工作开始时间
CFG_JOB_RUNTIME = "job.runTime"  # 工作 运行时间
CFG_JOB_NODE = "job.node"  # 工作节点
CFG_JOB_CHANGENODE = "job.changenode"  # 该node不可使用
CFG_JOB_CHECKTIME = "job.checkTime"  # 健康检查时间,默认600s
CFG_JOB_AUTOSWITCHNODE = "job.autoSwitchNode"  # 节点故障时，自动切换节点，默认不允许
CFG_NODE_IPTYPE = "node.type"  # 节点类型

NODE_TPE_AUTOIP = "node.autoIP"
NODE_TPE_PROXYIP = "node.proxyIP"
NODE_TPE_FIXEDIP = "node.fixedIP"

GD_JOB_ERROR = "jobError"  # 工作错误
GD_LOGGER = "logger"  # #日志记录器
GD_CFG_IN = "cfgIn"  # 配置在
GD_SUB = "subGlobal"  # 子类全局
GD_SPIDER = "subSpider"  # 子类爬虫全局
GD_TRAINER = "subTrainer"  # 子类训练器全局
GD_ACCOUNTINFO = "accountHelper"  # 全局账号管理

LOG_ALWAYS="log911-forever!!"
CMD_LOCK_RELEASE = "!!_cmd_lock_released_now!"