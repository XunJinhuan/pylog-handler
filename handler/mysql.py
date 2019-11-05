# -*- coding: utf-8 -*-

"""
This is a logging handler of mysql.
Currently, MySQL and SQLite databases are supported after testing
"""

import os
import sys
import time
import json
import traceback
from logging import Handler, NOTSET
from sqlalchemy.orm import sessionmaker, mapper
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, JSON, Boolean
from sqlalchemy.ext.declarative import declarative_base
from .utils import LOGGING_FORMAT_MAPPER, FMT_MESSAGE, FMT_ASCTIME, FMT_LEVELNAME, FMT_CREATED

_mysql_fail_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "log/msqlhandler_error.log")


# 默认的日志表的用户自定义字段
DEFAULT_FIELDS = {
    "user": "String(30)",      # 用户名
    "url": "String(500)",      # 用户访问的url
    "method": "String(10)",    # 方法
    "status": "Integer",       # 状态响应码
    "address": "String(32)",   # 用户地址
    "browser": "String(200)",  # 浏览器标识符
    "os": "String(50)",        # 用户操作系统
    "host": "String(32)",      # 服务器地址
    "port": "Integer",         # 服务器端口
}


DEFAULT_ORIGIN_FIELDS = [FMT_LEVELNAME]


DEFAULT_MESSAGE_MAPPER = {
    FMT_MESSAGE: "String(500)"
}


class LogTableError(Exception):
    def __init__(self, msg=None):
        if msg is None:
            msg = "log tabale error"
        super(LogTableError, self).__init__(msg)


class LogFieldNameConflictError(Exception):
    def __init__(self, msg=None):
        if msg is None:
            msg = "Same field name in log table"
        super(LogFieldNameConflictError, self).__init__(msg)


class LogOriginFieldError(Exception):
    def __init__(self, msg=None):
        if msg in None:
            msg = "logging original field error"
        super(LogOriginFieldError, self).__init__(msg)


def check_type(field, value, field_mapper):
    """
    Check whether the value type is correct
    field: field name in log table
    value: value of field
    field_mapper: mapper between field name and value type in log tabel, you can reference DEFAULT_FIELDS.
    """
    if isinstance(value, (int, float)) and field_mapper.get(field, "").startswith("Integer"):
        return True
    elif isinstance(value, str) and field_mapper.get(field, "").startswith("String"):
        return True
    elif isinstance(value, dict) and field_mapper.get(field, "").startswith("JSON"):
        return True
    elif isinstance(value, bool) and field_mapper.get(field, "").startswith("Boolean"):
        return True
    else:
        return False


Base = declarative_base()


def get_model(name, engine):
    """get table model by tabel name"""
    Base.metadata.reflect(engine)
    table = Base.metadata.tables[name]
    table_model = type(name, (object,), dict())
    mapper(table_model, table)
    Base.metadata.clear()
    return table_model


class MySQLHandler(Handler):
    def __init__(self, uri, table_name, level=NOTSET, origin_field=None, new_field=None):
        """
        myql log handler, send log to mysql database(or other sql database)
        :param uri:            str  mysql uri
        :param table_name:     str  table name of saving log
        :param level:               log level
        :param origin_field:   list format string fields of defined by logging,
                                    for example 'asctime', 'levelname',...., but can't user 'message' field,
                                    temporarily only support 'Integer', 'String', 'JSON' and 'Boolean' type.
        :param new_field:      dict Custom fields contained in the data table，eg: "user": "String(30)", these fields
                                    come from logging origin field message.If new_field only on field 'message'(eg:
                                    new_field={"message": "String(500)"}), will use log message as value as value,
                                    otherwise ignore 'message' field if this field in new_field.
                                    Temporarily only support 'Integer', 'String', 'JSON' and 'Boolean' type.
        """
        self.engine = create_engine(uri, pool_recycle=6 * 3600)  # pool will reconnect mysql database after 6 hour
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()
        metadata = MetaData()
        if origin_field is None:
            self.origin_field = DEFAULT_ORIGIN_FIELDS
        else:
            self.origin_field = dict()
            for field in origin_field:
                if field not in LOGGING_FORMAT_MAPPER.keys():
                    msg = "The element {0} in origin_field is not logging format field, \
                    there are logging format fields: {0}".format(field, LOGGING_FORMAT_MAPPER)
                    raise LogOriginFieldError(msg)
                self.origin_field[field] = LOGGING_FORMAT_MAPPER.get(field)
        self.new_field = {}
        new_field = new_field if new_field else DEFAULT_MESSAGE_MAPPER
        for field, value in new_field.items():
            if field in LOGGING_FORMAT_MAPPER.keys():
                raise LogFieldNameConflictError("new_field has same field with logging format field:{0}".format(field))
            elif field == FMT_MESSAGE:
                if len(new_field) == 1:
                    self.new_field = new_field
            else:
                self.new_field[field] = value
        result = []
        self.fields = list(self.origin_field.items()) + list(self.new_field.items())
        for name, type_name in self.fields:
            result.append(Column(name, eval(type_name)))
        result = tuple(result)
        Table(table_name, metadata, Column('id', Integer, primary_key=True), *result)
        metadata.create_all(self.engine)
        self.LogModel = get_model(table_name, self.engine)
        super(MySQLHandler, self).__init__(level)

    def emit(self, record):
        self.acquire()
        try:
            log_data = self.LogModel()
            data = record.__dict__.copy()
            data[FMT_ASCTIME] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(data.get(FMT_CREATED)))
            data[FMT_MESSAGE] = data.get("msg")
            for field in self.origin_field:
                setattr(log_data, field, data.get(field))
            message = data.get(FMT_MESSAGE)
            if len(self.new_field) and FMT_MESSAGE in self.new_field:
                if isinstance(message, dict):
                    message = json.dumps(message, ensure_ascii=True)
                else:
                    message = "{0}".format(message)
                message = {FMT_MESSAGE: message}
            if isinstance(message, dict):
                for field, value in message.items():
                    if field not in self.new_field:
                        continue
                    if check_type(field, value, self.new_field):
                        setattr(log_data, field, value)
            self.session.add(log_data)
            self.session.commit()
        except Exception:
            self.handleError(record)
            traceback.print_exc(file=sys.stdout)
        self.release()

    def close(self):
        self.session.commit()
        self.session.close()


if __name__ == "__main__":
    pass




