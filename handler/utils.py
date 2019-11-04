# -*- coding:utf-8 -*-

FMT_NAME = "name"
FMT_LEVELNO = "levelno"
FMT_LEVELNAME = "levelname"
FMT_PATHNAME = "pathname"
FMT_FILENAME = "filename"
FMT_MODULE = "module"
FMT_LINENO = "lineno"
FMT_FUNCNAME = "funcName"
FMT_CREATED = "created"
FMT_ASCTIME = "asctime"
FMT_MSECS = "msecs"
FMT_RELATIVECREATED = "relativeCreated"
FMT_THREAD = "thread"
FMT_THREADNAME = "threadName"
FMT_PROCESS = "process"
FMT_PROCESSNAME = "processName"
FMT_MESSAGE = "message"


# logging定义的日志原始字段
LOGGING_FORMAT_NAME = (
    FMT_NAME,               # loggger名称
    FMT_LEVELNO,            # 数字形式日志级别
    FMT_LEVELNAME,          # 日志级别
    FMT_PATHNAME,           # 调用日志记录函数的源码文件的全路径
    FMT_FILENAME,           # pathname的文件名部分
    FMT_MODULE,             # 模块名
    FMT_LINENO,             # 行号
    FMT_FUNCNAME,           # 调用函数
    FMT_CREATED,            # 日志发生的时间戳
    FMT_ASCTIME,            # 日志事件发生的时间
    FMT_MSECS,              # 日志事件发生事件的毫秒部分
    FMT_RELATIVECREATED,    #
    FMT_THREAD,             # 线程ID
    FMT_THREADNAME,         # 线程名称
    FMT_PROCESS,            # 进程ID
    FMT_PROCESSNAME,        # 进程名称
    FMT_MESSAGE
)


DEFAULT_FORMAT = [
    FMT_NAME,
    # FMT_LEVELNAME,
    # FMT_PATHNAME,
    # FMT_FUNCNAME,
    # FMT_LINENO,
    FMT_ASCTIME,
    FMT_PROCESS,
    FMT_MESSAGE,
]

DEFAULT_EXCHANGE = "python.topic.logging"


class LogOriginFieldError(Exception):
    def __init__(self, msg=None):
        if msg is None:
            msg = "logging origin field error."
        super(LogOriginFieldError, self).__init__(msg)
