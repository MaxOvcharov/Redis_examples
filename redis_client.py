import asyncio
from collections import abc
from functools import wraps

import aioredis

from custom_errors import RedisConnectionLost
from settings import logger, REDIS_RECONNECT_DELAY, REDIS_RECONNECT_RETRIES
from utils import deserialize_json, serialize_json


class RedisClient:
    """ Redis connection pool client """

    def __init__(self, loop, conf):
        self.loop = loop
        self.conf = conf
        self.rd = None

    @classmethod
    async def connect(cls, **options):
        self = cls(**options)
        await self.init_connection()
        return self

    async def init_connection(self):
        self.rd = await aioredis.create_pool(
            (self.conf['host'], self.conf['port']),
            db=self.conf['db'],
            password=self.conf['password'],
            maxsize=self.conf['maxsize'],
            minsize=self.conf['minsize'],
            loop=self.loop
        )

    async def close_connection(self):
        self.rd.close()
        await self.rd.wait_closed()
        logger.debug('Redis closing pool connection...')

async def rd_client_factory(loop, conf, client=RedisClient):
    """ Abstract Redis client factory """
    rd = await client.connect(loop=loop, conf=conf)
    return rd


def serializer(value, encode=True, native_type=str, full=False):
    """
    Serialize saving data into string format.

    :param value: serialize data
    :type value: json.encoder.JSONEncoder or dict
    :param encode: if True - convert to JSON,
      else - Python dict
    :param object native_type: dict key type
    :param bool full: if False - full serialization for object

    :return: JSON объект or dict
    :rtype: json.encoder.JSONEncoder or dict
    """
    if encode:
        converter = serialize_json
    else:
        converter = deserialize_json

    if not full and isinstance(value, abc.Mapping):
        # Serialize only values into dict
        return {native_type(k): converter(v) for k, v in value.items()}

    return converter(value)


def acquire_connection(retry_delay=REDIS_RECONNECT_DELAY,
                       num_retries=REDIS_RECONNECT_RETRIES):
    """
    Gets connection from pool and do reconnect if ConnectionClosedError raise.

    :param int retry_delay: delay between retries
    :param int num_retries: number of retries

    :return: function to decorate
    :rtype: object
    """
    def wrapper(coro):

        @wraps(coro)
        async def release(self, *args, **kwargs):
            for _ in range(num_retries):
                try:
                    async with self.pool.get() as connection:
                        self._connection = connection
                        return await coro(self, *args, **kwargs)
                except aioredis.errors.ConnectionClosedError:
                    logger.error('Connection to redis lost. Retry after %s s.', retry_delay)
                    await asyncio.sleep(retry_delay)
                finally:
                    self._connection = None

            raise RedisConnectionLost

        return release
    return wrapper


class RedisClient:
    """
    This is a Redis client with reconnection. This class
      includes some methods which encapsulate aioredis logic.
    """
    def __init__(self, loop, conf):
        """
        Initialises Redis client by configuration params

        :param loop: asyncio EventLoop
        :type loop: asyncio.unix_events._UnixSelectorEventLoop
        :param dict conf: params from config file

        :return: None
        """
        self.loop = loop
        self.conf = conf
        self.pool = None
        self._connection = None

    @classmethod
    async def connect(cls, **options):
        """
        This method creates Redis client.

        :return obj self: RedisClient instance
        """
        self = cls(**options)
        await self._init_connect()
        return self

    async def _init_connect(self, retry_delay=REDIS_RECONNECT_DELAY,
                            num_retries=REDIS_RECONNECT_RETRIES):
        """
        This method create Redis client by config params.
          If Redis server refused connection do retries.

        :param int retry_delay: delay between retries
        :param int num_retries: number of retries

        :return: None
        """
        for _ in range(num_retries):
            try:
                self.pool = await aioredis.create_pool(
                    (self.conf['host'], self.conf['port']),
                    db=self.conf['db'],
                    password=self.conf['password'],
                    encoding=self.conf['encoding'],
                    minsize=self.conf['minsize'],
                    maxsize=self.conf['maxsize'],
                    loop=self.loop)
                break
            except ConnectionRefusedError:
                logger.error(
                    'Cant establish connection to redis. Retry after %s s.', retry_delay)
            await asyncio.sleep(retry_delay)

    async def close_connection(self):
        """
        This method close connection to the Redis server.

        :return: None
        """
        self.pool.close()
        await self.pool.wait_closed()
        logger.debug("Redis connection pool closing...")

    @staticmethod
    def _serialize(value, full=False):
        """
        This method serialize data into JSON object.

        :param dict value: serialize data
        :param bool full: if False - full serialization for object

        :return: JSON object
        :rtype: json.encoder.JSONEncoder
        """
        return serializer(value, encode=True, full=full)

    @staticmethod
    def _deserialize(value):
        """
        This method deserialize JSON object to Python dict.

        :param value: deserialize data
        :type value: json.encoder.JSONEncoder

        :return: Python object(dict)
        :rtype: dict
        """
        if value is None:
            return value
        return serializer(value, encode=False)

    # Commands for STRING type

    @acquire_connection()
    async def getv(self, key, use_serializer=False):
        """
        Данный метод предназначен для получения данных по
          ключу из Redis(string type)

        :param str key: значение ключа
        :param bool use_serializer: если True - необходимо
          сериализовать результат, False - вернуть без изменений

        :return: результат запроса
        :rtype: str
        """
        logger.debug('redis.getv: key=%s', key)
        value = await self._connection.get(key)
        return self._deserialize(value) if use_serializer else value

    @acquire_connection()
    async def getsetv(self, key, value, use_serializer=False):
        """
        Данный метод предназначен для записи/обновлении данных по
          ключу из Redis(string type). Если данные были изменены
          возвращается старое значение.

        :param str key: значение ключа
        :param str value: данные для записи в кэш
        :param bool use_serializer: если True - необходимо
          сериализовать результат, False - вернуть без изменений

        :return: результат запроса
        :rtype: str
        """
        logger.debug('redis.getsetv: key=%s, value=%s', key, value)
        res_value = await self._connection.getset(key, value)
        return self._deserialize(res_value) if use_serializer else res_value

    @acquire_connection()
    async def setv(self, key, value, ttl=None, use_serializer=False):
        """
        Данный метод предназначен для записи данных по
          ключу в Redis(string type). Возможно установка
          времени жизни данных в кэше.

        :param str key: значение ключа
        :param str, dict value: данные для записи в кэш
        :param int ttl: время жизни данных в кэш
        :param bool use_serializer: если True - необходимо
          сериализовать результат, False - вернуть без изменений

        :return: None
        """
        logger.debug('redis.setv: key=%s, value=%s, ttl=%s', key, value, ttl)
        value = self._serialize(value, full=True) if use_serializer else value
        await self._connection.set(key, value, expire=ttl)

    @acquire_connection()
    async def setnx(self, key, value, use_serializer=False):
        """
        Данный метод предназначен для записи данных, если они
          отсутствуют в кэш, по ключу в Redis(string type).

        :param str key: значение ключа
        :param str, dict value: данные для записи в кэш
        :param bool use_serializer: если True - необходимо
          сериализовать результат, False - вернуть без изменений

        :return: если 0 - данные уже существуют, 1 - данные записаны.
        :rtype: int
        """
        logger.debug('redis.setnx: key=%s, value=%s', key, value)
        value = self._serialize(value, full=True) if use_serializer else value
        return await self._connection.setnx(key, value)

    @acquire_connection()
    async def mgetv(self, keys, use_serializer=False):
        """
        Данный метод предназначен для получения данных по
          ключам из Redis(string type)

        :param list keys: список значений ключей
        :param bool use_serializer: если True - необходимо
          сериализовать результат, False - вернуть без изменений

        :return: результат запроса
        :rtype: list
        """
        logger.debug('redis.mget: keys=%s', ','.join(keys))
        values = await self._connection.mget(*keys)
        return [self._deserialize(v) if use_serializer else k for k, v in zip(keys, values)]

    @acquire_connection()
    async def msetv(self, pairs, ttl=None, use_serializer=False):
        """
        Данный метод предназначен для записи данных по
          ключам из Redis(string type). Возможно установка
          времени жизни данных в кэше.

        :param list, dict pairs: Список или словарь пара (ключ, значение)
        :param int ttl: время жизни данных в кэш
        :param bool use_serializer: если True - необходимо
          сериализовать результат, False - вернуть без изменений

        :return: None
        """
        logger.debug('redis.mset: pairs=%s, ttl=%s', pairs, ttl)
        pairs = self._serialize(pairs) if use_serializer else pairs

        pipe = self._connection.pipeline()
        pipe.mset(pairs.items())

        if ttl is not None:
            for key in pairs:
                pipe.expire(key, ttl)

        await pipe.execute()

    # "Базовые" методы для работы с данными в Redis

    @acquire_connection()
    async def expire(self, key, ttl):
        """
        Данный метод предназначен для установки времени
          жизни данных в Redis.

        :param str key: значение ключа
        :param int ttl: время жизни данных в кэш

        :return: None
        """
        logger.debug('redis.expire: key=%s, ttl=%s', key, ttl)
        return await self._connection.expire(key, ttl)

    @acquire_connection()
    async def keys(self, pattern):
        """
        Данный метод предназначен для получения данных
          из Redis по маске к имени ключа.

        :param str pattern: маске к имени ключа

        :return: список значений подходящих к маске
        :rtype: list
        """
        logger.debug('redis.keys: pattern="%s"', pattern)
        return await self._connection.keys(pattern)

    @acquire_connection()
    async def delete(self, *keys):
        """
        Данный метод предназначен для удаления данных
          из Redis по ключам.

        :param list keys: список значений ключей

        :return: None
        """
        logger.debug('redis.delete: keys=%s', ','.join(keys))
        await self._connection.delete(*keys)

    @acquire_connection()
    async def flushdb(self):
        """
        Данный метод предназначен для удаления всех данных из db в Redis.

        :return: None
        """
        logger.debug('redis.flushdb')
        await self._connection.flushdb()

    @acquire_connection()
    async def flushall(self):
        """
        Данный метод предназначен для удаления всех данных из всех db в Redis.

        :return: None
        """
        logger.debug('redis.flushall')
        await self._connection.flushall()

    @acquire_connection()
    async def multi_exec(self):
        """
        Данный метод предназначен создания объекта транзакции в Redis.

        :return: объекта транзакции
        :rtype: aioredis.commands.transaction.TransactionsCommandsMixin#multi_exec
        """
        logger.debug('redis.multi_exec')
        return self._connection.multi_exec()

    # Методы для работы с типом "HASH"

    @acquire_connection()
    async def hset(self, key, field, value, use_serializer=True):
        """
        Данный метод предназначен для записи данных в хэш-таблицу,
          хранящююся в Redis по ключу.

        :param str key: значение ключа
        :param str field: значение ключа в хэш-таблице
        :param str value: записываемое значение в хэш-таблицу
        :param bool use_serializer: если True - необходимо
          сериализовать результат, False - вернуть без изменений

        :return: если 1 - создано новая пара(field:value),
          если 0 - обновлена существующая пара(field:value)
        :rtype: int
        """
        logger.debug('redis.hset: key=%s, field=%s, value=%s', key, field, value)
        value = self._serialize(value) if use_serializer else value
        return await self._connection.hset(key, field, value)

    @acquire_connection()
    async def hmset(self, key, pairs, ttl=None, use_serializer=True):
        """
        Данный метод предназначен для записи данных в хэш-таблицу,
          хранящююся в Redis по ключам.

        :param str key: значение ключа
        :param str pairs: пара(field:value) в хэш-таблице
        :param int ttl: время жизни данных в кэш
        :param bool use_serializer: если True - необходимо
          сериализовать результат, False - вернуть без изменений

        :return: None
        """
        logger.debug('redis.hmset: key=%s', key)
        values = self._serialize(pairs) if use_serializer else pairs

        pipe = self._connection.pipeline()
        pipe.hmset_dict(key, values)

        if ttl is not None:
            pipe.expire(key, ttl)

        await pipe.execute()

    @acquire_connection()
    async def hget(self, key, field, use_serializer=True):
        """
        Данный метод предназначен для получения данных из хэш-таблицы,
          хранящейся в Redis по ключу.

        :param str key: значение ключа
        :param str field: значение ключа в хэш-таблице
        :param bool use_serializer: если True - необходимо
          десериализовать результат, False - вернуть без изменений

        :return: значение из хэш-таблицу по ключу
        :rtype: str
        """
        logger.debug('redis.hget: key=%s, field=%s', key, field)
        value = await self._connection.hget(key, field)
        return self._deserialize(value) if use_serializer else value

    @acquire_connection()
    async def hmget(self, key, fields, use_serializer=True):
        """
        Данный метод предназначен для получения данных из хэш-таблицы,
          хранящейся в Redis по ключу.

        :param str key: значение ключа
        :param tuple fields: значения ключей в хэш-таблице
        :param bool use_serializer: если True - необходимо
          десериализовать результат, False - вернуть без изменений

        :return: значения из хэш-таблицу по ключам
        :rtype: list
        """
        logger.debug('redis.hget: key=%s, field=%s', key, fields)
        value = await self._connection.hmget(key, *fields)
        return self._deserialize(dict(zip(fields, value)) if use_serializer else value)

    @acquire_connection()
    async def hgetall(self, key, use_serializer=True):
        """
        Данный метод предназначен для получения всех данных
          из хэш-таблицы, хранящейся в Redis.

        :param str key: значение ключа
        :param bool use_serializer: если True - необходимо
          десериализовать результат, False - вернуть без изменений

        :return: значения из хэш-таблицу по ключу
        :rtype: list
        """
        logger.debug('redis.hgetall: key=%s', key)
        values = await self._connection.hgetall(key)
        return self._deserialize(values) if use_serializer else values

    @acquire_connection()
    async def hdel(self, key, fields, use_serializer=True):
        """
        Данный метод предназначен для удаления данных по ключу
          из хэш-таблицы, хранящейся в Redis.

        :param str key: значение ключа
        :param str fields: значения ключей в хэш-таблице
        :param bool use_serializer: если True - необходимо
          десериализовать результат, False - вернуть без изменений

        :return: если > 1 - количество удаленных сообщений,
          если 0 - удаляемое сообщение отсутствует
        :rtype: int
        """
        logger.debug('redis.hdel: key=%s, field=%s', key, fields)
        value = await self._connection.hdel(key, fields)
        return self._deserialize(value) if use_serializer else value
