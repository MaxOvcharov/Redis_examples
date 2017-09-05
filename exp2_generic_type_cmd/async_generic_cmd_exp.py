# -*- coding: utf-8 -*-
"""
    Simple example of Redis Generic commands using async lib - aioredis
"""
import asyncio
import datetime as dt
import os

from redis_client import rd_client_factory
from settings import BASE_DIR, logger
from utils import load_config


class RedisGenericCommands:
    def __init__(self, rd):
        self.rd = rd

    async def run_generic_cmd(self):
        await self.rd_del_cmd()
        await self.rd_dump_cmd()
        await self.rd_exists_cmd()
        await self.rd_expire_cmd()
        await self.rd_expireat_cmd()
        await self.rd_keys_cmd()

    async def rd_del_cmd(self):
        """
        Removes the specified keys. A key is ignored if it does not exist.
          Return value:
            The number of keys that were removed.

        :return: None
        """
        key1, key2, key3 = 'key_1', 'key_2', 'key_3'
        value1, value2 = 'TEST1', 'TEST2'
        with await self.rd as conn:
            await conn.mset(key1, value1, key2, value2)
            res = await conn.delete(key1, key2, key3)
        frm = "GENERIC_CMD - 'DELETE': KEY 1,2,3- {0}, DEL_NUM - {1}\n"
        logger.debug(frm.format([key1, key2, key3], res))

    async def rd_dump_cmd(self):
        """
        Serialize the value stored at key in a Redis-specific format and
          return it to the user. The returned value can be synthesized
          back into a Redis key using the RESTORE command.

        :return: None
        """
        key1 = 'key_1'
        value1 = 'TEST1'
        with await self.rd as conn:
            await conn.set(key1, value1)
            res1 = await conn.dump(key1)
            await conn.delete(key1)
            await conn.restore(key1, 0, res1)
            res2 = await conn.get(key1)
            await conn.delete(key1)
        frm = "GENERIC_CMD - 'DUMP': KEY- {0}, SERIALIZE - {1}, DESERIALIZE - {2}\n"
        logger.debug(frm.format(key1, res1, res2))

    async def rd_exists_cmd(self):
        """
        Returns if key(s) exists.

        :return: None
        """
        key1, key2, key3 = 'key_1', 'key_2', 'not_exist_key'
        value1, value2, value3 = 'TEST1', 'TEST2', 'TEST3'
        with await self.rd as conn:
            await conn.mset(key1, value1, key2, value2)
            res = await conn.exists(key1, key2, key3)
            await conn.delete(key1, key2, key3)
        frm = "GENERIC_CMD - 'EXISTS': KEY- {0}, EXISTS_KEYS_NUM - {1}\n"
        logger.debug(frm.format([key1, key2, key3], res))

    async def rd_expire_cmd(self):
        """
        Set a timeout on key. After the timeout has expired, the key
          will automatically be deleted. A key with an associated
          timeout is often said to be volatile in Redis terminology.
          The timeout will only be cleared by commands that delete or
          overwrite the contents of the key, including DEL, SET, GETSET
          and all the *STORE commands. This means that all the operations
          that conceptually alter the value stored at the key without
          replacing it with a new one will leave the timeout untouched.
          For instance, incrementing the value of a key with INCR, pushing
          a new value into a list with LPUSH, or altering the field value
          of a hash with HSET are all operations that will leave the
          timeout untouched.

        :return: None
        """
        key = 'key'
        value = 'TEST'
        time_of_ex = 10
        with await self.rd as conn:
            await conn.set(key, value)
            await conn.expire(key, time_of_ex)
            await asyncio.sleep(2)
            ttl1 = await conn.ttl(key)
            await conn.set(key, value)
            ttl2 = await conn.ttl(key)
            await conn.delete(key)
        frm = "GENERIC_CMD - 'EXPIRE': KEY- {0}, BEFORE_EX - ({1} sec), AFTER_EX - ({2} sec)\n"
        logger.debug(frm.format(key, ttl1, ttl2))

    async def rd_expireat_cmd(self):
        """
        EXPIREAT has the same effect and semantic as EXPIRE, but instead of
          specifying the number of seconds representing the TTL (time to live),
          it takes an absolute Unix timestamp (seconds since January 1, 1970).
          A timestamp in the past will delete the key immediately.

        :return: None
        """
        key = 'key'
        value = 'TEST'
        date_of_ex = (dt.datetime.now() + dt.timedelta(days=1)).timestamp()
        with await self.rd as conn:
            await conn.set(key, value)
            await conn.expireat(key, date_of_ex)
            await asyncio.sleep(2)
            ttl1 = await conn.ttl(key)
            await conn.set(key, value)
            ttl2 = await conn.ttl(key)
            await conn.delete(key)
        frm = "GENERIC_CMD - 'EXPIREAT': KEY- {0}, BEFORE_EX - ({1} sec), AFTER_EX - ({2} sec)\n"
        logger.debug(frm.format(key, ttl1, ttl2))

    async def rd_keys_cmd(self):
        """
        Returns all keys matching pattern.
          Supported glob-style patterns:
            h?llo matches hello, hallo and hxllo
            h*llo matches hllo and heeeello
            h[ae]llo matches hello and hallo, but not hillo
            h[^e]llo matches hallo, hbllo, ... but not hello
            h[a-b]llo matches hallo and hbllo
          Use \ to escape special characters if you want to match them verbatim.

        :return: None
        """
        key1, key2, key3, key4 = 'one', 'two', 'three', 'four'
        value1, value2, value3, value4 = 1, 2, 3, 4
        pattern = '*o*'
        with await self.rd as conn:
            await conn.mset(key1, value1, key2, value2, key3, value3, key4, value4)
            res = await conn.keys(pattern)
            await conn.delete(key1, key2, key3, key4)
        frm = "GENERIC_CMD - 'KEYS': KEY- {0}, PATTERN - {1}, RES - {2}\n"
        logger.debug(frm.format([key1, key2, key3, key4], pattern, res))


def main():
    # load config from yaml file
    conf = load_config(os.path.join(BASE_DIR, "config_files/dev.yml"))
    # create event loop
    loop = asyncio.get_event_loop()
    rd_conn = loop.run_until_complete(rd_client_factory(loop=loop, conf=conf['redis']))
    rgc = RedisGenericCommands(rd_conn.rd)
    try:
        loop.run_until_complete(rgc.run_generic_cmd())
    except KeyboardInterrupt as e:
        logger.error("Caught keyboard interrupt {0}\nCanceling tasks...".format(e))
    finally:
        loop.run_until_complete(rd_conn.close_connection())
        loop.close()

if __name__ == '__main__':
    main()