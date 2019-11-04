# Python Log Handler
There is some python log handler than emit log to rabbitmq or mysql.

## Usage examples
### synchronization rabbitmq
```python
import logging
from logging.config import dictConfig

LOGGING = {
    'version': 1,
    'handlers': {
        "rabbitmq": {
            "level": "INFO",
            "class": "handler.rabbitmq.RabbitmqHandler",
            "appname": "log-test",
            "uri": "amqp://root:123456@127.0.0.1:5672/test",
            "exchange": "python.topic.logging",
            "fields": ["message", "levelname"],
            "routing_key": ["levelname", "message.field"]
        }
    },
    'loggers': {
        "rabbitmq": {
            "handlers": ["rabbitmq"],
            "level": "INFO",
            'propagate': False
        }
    }
}

dictConfig(LOGGING)

logger = logging.getLogger("rabbitmq")

logger.info({"field": "aa"})
```

### asynchronous rabbitmq
```python
import logging
import asyncio
from logging.config import dictConfig

LOGGING = {
    'version': 1,
    'handlers': {
        "aiocrabbitmq": {
            "level": "INFO",
            "class": "handler.aiorabbitmq.AioRabbitmqHandler",
            "appname": "log-test",
            "uri": "amqp://root:123456@127.0.0.1:5672/test",
            "exchange": "python.topic.logging",
            "fields": ["message", "levelname"],
            "routing_key": ["levelname", "message.field"]
        }
    },
    'loggers': {
        "aiohandler": {
            "handlers": ["aiocrabbitmq"],
            "level": "INFO",
            'propagate': False 
        },
    }
}


async def main():
    dictConfig(LOGGING)
    logger = logging.getLogger("aiohandler")
    logger.info({"field": "aa"})
    await asyncio.sleep(3)  # wait send log send finish


if __name__ == '__main__':
    asyncio.run(main())
```
