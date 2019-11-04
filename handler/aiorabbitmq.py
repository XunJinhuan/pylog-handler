# -*- coding:utf-8 -*-

"""
This is a logging handler of rabbitmq.
asynchronous connect to rabbitmq and send log.
"""

import sys
import json
import time
import asyncio
import aiormq
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


class AioRabbitmqHandler(logging.Handler):
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
        super(AioRabbitmqHandler, self).__init__(level)
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

        self.is_exchange_declared = False
        self.is_closed = True
        self.init_connection = asyncio.Queue(maxsize=1)
        self.loop = None

    async def connect(self):
        self.connection = await aiormq.connect(self.uri)
        self.channel = await self.connection.channel()
        msg = '{0} - [rabbitmq] Connect to {1} success.\n'.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), self.uri)
        sys.stdout.write(msg)
        if self.is_exchange_declared is False:
            await self.channel.exchange_declare(
                exchange=self.exchange,
                exchange_type="topic",
                passive=False,
                durable=True,
                auto_delete=False
            )
            self.is_exchange_declared = True

    async def rabbit_connect(self):
        if self.is_closed:
            await self.init_connection.put(1)
            if self.is_closed:
                await self.connect()
                self.is_closed = False
        if not self.init_connection.empty():
            try:
                self.init_connection.get_nowait()
            except asyncio.QueueEmpty:
                pass

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

    async def _emit(self, record):
        log_data = dict()
        origin_data = record.__dict__.copy()
        origin_data[FMT_ASCTIME] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(origin_data.get(FMT_CREATED)))
        origin_data[FMT_MESSAGE] = origin_data.get("msg")
        for field in self.origin_fields:
            log_data[field] = origin_data.get(field)
        routing = self.get_routing_key(origin_data)
        await self.channel.basic_publish(
            exchange=self.exchange,
            routing_key=routing,
            body=json.dumps(log_data, ensure_ascii=False).encode("utf-8"),
            properties=aiormq.spec.Basic.Properties(delivery_mode=2)
        )

    async def con_close(self):
        try:
            if self.connection:
                await self.connection.close()
        finally:
            self.connection = None
            self.channel = None

    async def base_publish(self, record):
        try:
            if self.is_closed is True:
                await self.rabbit_connect()
            await self._emit(record)
            return True
        except Exception as e:
            sys.stdout.write("publish log error, maybe connection closed:{0}\n".format(e))
            traceback.print_exc()
            await self.con_close()
            self.is_closed = True
            return False

    async def publish(self, record):
        res = await self.base_publish(record)
        if not res:  # unsuccessfully, maybe connection is closed, publish again
            await self.base_publish(record)

    def emit(self, record):
        self.acquire()
        try:
            self.loop = asyncio.get_event_loop()
            self.loop.create_task(self.publish(record))
        except Exception:
            self.handleError(record)
            traceback.print_exc()
        self.release()
