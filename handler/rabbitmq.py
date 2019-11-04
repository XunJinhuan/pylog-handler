# -*- coding:utf-8 -*-

"""
This is a logging handler of rabbitmq.
"""

import sys
import json
import time
import pika
import logging
import traceback
from datetime import datetime
from .utils import (
    DEFAULT_EXCHANGE,
    DEFAULT_FORMAT,
    LOGGING_FORMAT_NAME,
    FMT_MESSAGE,
    FMT_ASCTIME,
    FMT_CREATED,
    LogOriginFieldError
)


class RabbitmqHandler(logging.Handler):
    def __init__(
            self,
            appname: str,
            uri: str,
            exchange: str = DEFAULT_EXCHANGE,
            fields: list = None,
            level=logging.NOTSET,
            routing_key: list = None
    ):
        """
        Rabbitmq Handler emit log to rabbitmq
        :param appname: application name
        :param uri: rabbitmq url
        :param exchange: exhange name. exhange type is topic.
        :param fields: format string fields of defined by logging, for example 'asctime', 'message', 'levelname',....
        :param level: log level
        :param routing_key: fields list of routing key. If param is None, routing_key = ["name", "levelname"],
        eg: appname.loggername.INFO, appname.loggername.ERROR. The element of list can be
        format string fields of defined by logging or fields of message where message is dict.
        If fields is not in log data, will use "*" replace. If element of routing_key is fields of message,
        should use dot separate, eg: 'message.field1.fields2', 'message.field1'.
        """
        super(RabbitmqHandler, self).__init__(level)
        if len(appname) > 100:
            raise ValueError("application name is to long")
        self.appname = appname
        self.uri = uri
        self.exchange = exchange

        if fields is None:
            self.origin_fields = DEFAULT_FORMAT
        else:
            for field in fields:
                if field not in LOGGING_FORMAT_NAME:
                    raise LogOriginFieldError("logging doesn't define formatting field {0}".format(field))
            self.origin_fields = set(fields)
            if "msg" in self.origin_fields:
                self.origin_fields.discard("msg")
                self.origin_fields.add(FMT_MESSAGE)
            self.origin_fields = list(self.origin_fields)
        if routing_key is None:
            routing_key = ["name", "levelname"]
        if not routing_key or not isinstance(routing_key, list):
            raise ValueError("routing_key should be a no empty list")
        if FMT_MESSAGE in routing_key:
            raise ValueError("message fild cat as routing key")
        self.routing_key = routing_key

        self.connection = None
        self.channel = None
        self.routing = None

        self.is_exchange_declared = False

        self.connect()
        self.is_closed = False

    def connect(self):
        """
        connent rabbitmq server
        """
        param = pika.URLParameters(self.uri)
        self.connection = pika.BlockingConnection(param)
        self.channel = self.connection.channel()
        sys.stdout.write('%s - stdout - [rabbitmq] Connect success.\n' % datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        if self.is_exchange_declared is False:
            self.channel.exchange_declare(
                exchange=self.exchange,
                exchange_type="topic",
                passive=False,
                durable=True,
                auto_delete=False
            )
            self.is_exchange_declared = True

    def get_routing_key(self, data: dict):
        origin_data = data
        routing = self.appname + "."
        for field in self.routing_key:
            value = None
            keys = field.split(".")
            data = origin_data
            for index, key in enumerate(keys):
                value = data.get(key)
                data = value
                if not isinstance(data, dict):
                    if index == len(keys) - 1 and value is not None:
                        value = str(value)
                    break
            if not isinstance(value, str):
                value = "*"
            routing += str(value) + "."
        routing = routing[:-1]
        if len(self.appname) < 255 < len(routing):
            routing = self.appname + ".*" * len(self.routing_key)
        if len(routing) > 255:
            routing = routing[:255]
        return routing

    def _emit(self, record):
        log_data = dict()
        origin_data = record.__dict__.copy()
        origin_data[FMT_ASCTIME] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(origin_data.get(FMT_CREATED)))
        origin_data[FMT_MESSAGE] = origin_data.get("msg")
        for field in self.origin_fields:
            log_data[field] = origin_data.get(field)
        routing = self.get_routing_key(origin_data)
        self.channel.basic_publish(
            exchange=self.exchange,
            routing_key=routing,
            body=json.dumps(log_data, ensure_ascii=False),
            properties=pika.BasicProperties(delivery_mode=2)
        )

    def emit(self, record):
        self.acquire()
        count = 2
        while count > 0:
            try:
                if self.is_closed is True:
                    self.connect()
                    self.is_closed = False
                self._emit(record)
                break
            except Exception:
                self.handleError(record)
                traceback.print_exc(file=sys.stdout)
                self.close()
                self.is_closed = True
                count -= 1
        self.release()

    def close(self):
        """
        clear when closing
        """
        try:
            if self.channel and self.channel.is_closed is False:
                self.channel.close()
                self.channel = None
            if self.connection and self.connection.is_closed is False:
                self.connection.close()
                self.connection = None
        finally:
            pass
