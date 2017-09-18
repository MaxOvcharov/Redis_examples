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
        Get the value of a key

        :param str key: key name
        :param bool use_serializer: if True - deserialize result

        :return: result
        :rtype: str
        """
        logger.debug('redis.getv: key=%s', key)
        value = await self._connection.get(key)
        return self._deserialize(value) if use_serializer else value

    @acquire_connection()
    async def getsetv(self, key, value, use_serializer=False):
        """
        Set the string value of a key and return its old value.

        :param str key: key name
        :param str value: new value
        :param bool use_serializer: if True - deserialize result

        :return: result
        :rtype: str
        """
        logger.debug('redis.getsetv: key=%s, value=%s', key, value)
        res_value = await self._connection.getset(key, value)
        return self._deserialize(res_value) if use_serializer else res_value

    @acquire_connection()
    async def setv(self, key, value, ttl=None, use_serializer=False):
        """
        Set the string value of a key.

        :param str key: key name
        :param str, dict value: value for save
        :param int ttl: time to live for key
        :param bool use_serializer: if True - serialize result

        :return: None
        """
        logger.debug('redis.setv: key=%s, value=%s, ttl=%s', key, value, ttl)
        value = self._serialize(value, full=True) if use_serializer else value
        await self._connection.set(key, value, expire=ttl)

    @acquire_connection()
    async def setnx(self, key, value, use_serializer=False):
        """
        Set the value of a key, only if the key does not exist.

        :param str key: key name
        :param str, dict value: value for save
        :param bool use_serializer: if True - serialize result

        :return: if 0 - data exist, 1 - set data.
        :rtype: int
        """
        logger.debug('redis.setnx: key=%s, value=%s', key, value)
        value = self._serialize(value, full=True) if use_serializer else value
        return await self._connection.setnx(key, value)

    @acquire_connection()
    async def mgetv(self, keys, use_serializer=False):
        """
        Get the values of all the given keys.

        :param list keys: list of keys
        :param bool use_serializer: if True - deserialize result

        :return: result
        :rtype: list
        """
        logger.debug('redis.mget: keys=%s', ','.join(keys))
        values = await self._connection.mget(*keys)
        return [self._deserialize(v) if use_serializer else k for k, v in zip(keys, values)]

    @acquire_connection()
    async def msetv(self, pairs, ttl=None, use_serializer=False):
        """
        Set multiple keys to multiple values.

        :param list, dict pairs: list or dict with key-value pairs
        :param int ttl: time to live for keys
        :param bool use_serializer: if True - serialize result

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

    # Generic commands

    @acquire_connection()
    async def expire(self, key, ttl):
        """
        Set a timeout on key.

        :param str key: list of keys
        :param int ttl: time to live for keys

        :return: None
        """
        logger.debug('redis.expire: key=%s, ttl=%s', key, ttl)
        return await self._connection.expire(key, ttl)

    @acquire_connection()
    async def keys(self, pattern):
        """
        Returns all keys matching pattern.

        :param str pattern: regex for key

        :return: list of matching keys
        :rtype: list
        """
        logger.debug('redis.keys: pattern="%s"', pattern)
        return await self._connection.keys(pattern)

    @acquire_connection()
    async def delete(self, *keys):
        """
        Delete a key.

        :param list keys: list of keys

        :return: None
        """
        logger.debug('redis.delete: keys=%s', ','.join(keys))
        await self._connection.delete(*keys)

    @acquire_connection()
    async def flushdb(self):
        """
        Remove all keys from all databases.

        :return: None
        """
        logger.debug('redis.flushdb')
        await self._connection.flushdb()

    @acquire_connection()
    async def flushall(self):
        """
        Remove all keys from the current database.

        :return: None
        """
        logger.debug('redis.flushall')
        await self._connection.flushall()

    @acquire_connection()
    async def multi_exec(self):
        """
        Returns MULTI/EXEC pipeline wrapper.

        :return: multy_exec pipeline
        :rtype: aioredis.commands.transaction.TransactionsCommandsMixin#multi_exec
        """
        logger.debug('redis.multi_exec')
        return self._connection.multi_exec()

    # Методы для работы с типом "HASH"

    @acquire_connection()
    async def hset(self, key, field, value, use_serializer=True):
        """
        Set the string value of a hash field.

        :param str key: key name
        :param str field: dict key
        :param str, dict value: value for save
        :param bool use_serializer: if True - serialize result

        :return: if '1' - created new pair (field:value),
          если '0' - updated old pair(field:value)
        :rtype: int
        """
        logger.debug('redis.hset: key=%s, field=%s, value=%s', key, field, value)
        value = self._serialize(value) if use_serializer else value
        return await self._connection.hset(key, field, value)

    @acquire_connection()
    async def hmset(self, key, pairs, ttl=None, use_serializer=True):
        """
        Set multiple hash fields to multiple values.

        :param str key: key name
        :param list, dict pairs: list or dict of pairs(field:value)
        :param int ttl: time to live for keys
        :param bool use_serializer: if True - serialize result

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
        Get the value of a hash field.

        :param str key: key name
        :param str field: dict key
        :param bool use_serializer: if True - deserialize result

        :return: result
        :rtype: str
        """
        logger.debug('redis.hget: key=%s, field=%s', key, field)
        value = await self._connection.hget(key, field)
        return self._deserialize(value) if use_serializer else value

    @acquire_connection()
    async def hmget(self, key, fields, use_serializer=True):
        """
        Get the values of all the given fields.

        :param str key: key name
        :param list fields: list of keys
        :param bool use_serializer: if True - deserialize result

        :return: list of values
        :rtype: list
        """
        logger.debug('redis.hget: key=%s, field=%s', key, fields)
        value = await self._connection.hmget(key, *fields)
        return self._deserialize(dict(zip(fields, value)) if use_serializer else value)

    @acquire_connection()
    async def hgetall(self, key, use_serializer=True):
        """
        Get all the fields and values in a hash.

        :param str key: key name
        :param bool use_serializer: if True - deserialize result

        :return: list of values
        :rtype: list
        """
        logger.debug('redis.hgetall: key=%s', key)
        values = await self._connection.hgetall(key)
        return self._deserialize(values) if use_serializer else values

    @acquire_connection()
    async def hdel(self, key, fields):
        """
        Delete one or more hash fields.

        :param str key: key name
        :param list fields: list of keys

        :return: if res > '1' - number of deleted fields,
          else '0' - no one fields were deleted
        :rtype: int
        """
        logger.debug('redis.hdel: key=%s, field=%s', key, fields)
        return await self._connection.hdel(key, fields)

