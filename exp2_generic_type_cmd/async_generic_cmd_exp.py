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
    def __init__(self, rd1, rd2, conf=None):
        self.rd1 = rd1
        self.rd2 = rd2
        self.rd_conf = conf

    async def run_generic_cmd(self):
        # await self.rd_del_cmd()
        # await self.rd_dump_cmd()
        # await self.rd_exists_cmd()
        # await self.rd_expire_cmd()
        # await self.rd_expireat_cmd()
        # await self.rd_keys_cmd()
        # await self.rd_migrate_cmd()
        # await self.rd_move_cmd()
        await self.rd_object_refcount_cmd()
        await self.rd_object_encoding_cmd()

    async def rd_del_cmd(self):
        """
        Removes the specified keys. A key is ignored if it does not exist.
          Return value:
            The number of keys that were removed.

        :return: None
        """
        key1, key2, key3 = 'key_1', 'key_2', 'key_3'
        value1, value2 = 'TEST1', 'TEST2'
        with await self.rd1 as conn:
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
        with await self.rd1 as conn:
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
        with await self.rd1 as conn:
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
        with await self.rd1 as conn:
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
        with await self.rd1 as conn:
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
        with await self.rd1 as conn:
            await conn.mset(key1, value1, key2, value2, key3, value3, key4, value4)
            res = await conn.keys(pattern)
            await conn.delete(key1, key2, key3, key4)
        frm = "GENERIC_CMD - 'KEYS': KEY- {0}, PATTERN - {1}, RES - {2}\n"
        logger.debug(frm.format([key1, key2, key3, key4], pattern, res))

    async def rd_migrate_cmd(self):
        """
        Atomically transfer a key from a source Redis instance to a destination
          Redis instance. On success the key is deleted from the original instance
          and is guaranteed to exist in the target instance.
        The command is atomic and blocks the two instances for the time required
          to transfer the key, at any given time the key will appear to exist in a
          given instance or in the other instance, unless a timeout error occurs.
          In 3.2 and above, multiple keys can be pipelined in a single call to MIGRATE
          by passing the empty string ("") as key and adding the KEYS clause.

        :return: None
        """
        key1, key2, key3, key4 = 'one', 'two', 'three', 'four'
        value1, value2, value3, value4 = 1, 2, 3, 4
        pattern = '*o*'
        with await self.rd1 as conn:
            await conn.mset(key1, value1, key2, value2, key3, value3, key4, value4)
            db1_res = await conn.keys(pattern)
            res = await conn.migrate(self.rd_conf['host'], self.rd_conf['port'], '*', 2, 5000)
            await conn.delete(key1, key2, key3, key4)

        with await self.rd2 as conn:
            db2_res = await conn.keys(pattern)
            await conn.delete(key1, key2, key3, key4)
        frm = "GENERIC_CMD - 'MIGRATE': KEY- {0}, MIGRATE_KEY - {1}, DB1_RES - {2}, DB2_RES - {3}\n"
        logger.debug(frm.format([key1, key2, key3, key4], pattern, db1_res, db2_res))

    async def rd_move_cmd(self):
        """
        Move key from the currently selected database (see SELECT) to
          the specified destination database. When key already exists
          in the destination database, or it does not exist in the
          source database, it does nothing. It is possible to use MOVE
          as a locking primitive because of this.

        :return: None
        """
        key1 = 'key_1'
        value1 = 'TEST1'
        with await self.rd1 as conn:
            await conn.set(key1, value1)
            db1_res = await conn.move(key1, 2)
            await conn.delete(key1)

        with await self.rd2 as conn:
            db2_res = await conn.get(key1)
            await conn.delete(key1)
        frm = "GENERIC_CMD - 'MOVE': KEY- %s, MOVE_RES - %s, DB2_RES - %s\n"
        logger.debug(frm, key1, db1_res, db2_res)

    async def rd_object_refcount_cmd(self):
        """
        The OBJECT command supports multiple sub commands:
          OBJECT REFCOUNT <key> returns the number of references of the
          value associated with the specified key. This command is
          mainly useful for debugging.

        :return: None
        """
        key1 = 'key_1'
        value1 = 'TEST1'
        with await self.rd1 as conn:
            await conn.set(key1, value1)
            res = await conn.object_refcount(key1)
            await conn.delete(key1)
        frm = "GENERIC_CMD - 'OBJECT REFCOUNT': KEY- %s, REFCOUNT - %s\n"
        logger.debug(frm, key1, res)

    async def rd_object_encoding_cmd(self):
        """
        The OBJECT command supports multiple sub commands:
          OBJECT ENCODING <key> returns the kind of internal representation
          used in order to store the value associated with a key.

        :return: None
        """
        key1 = 'key_1'
        value1, value2 = '100', '_car'
        with await self.rd1 as conn:
            await conn.set(key1, value1)
            res_int = await conn.object_encoding(key1)
            await conn.append(key1, value2)
            res_str = await conn.object_encoding(key1)
            await conn.delete(key1)
        frm = "GENERIC_CMD - 'OBJECT ENCODING': KEY- %s, BEFORE - %s, AFTER - %s\n"
        logger.debug(frm, key1, res_int, res_str)

def main():
    # load config from yaml file
    conf = load_config(os.path.join(BASE_DIR, "config_files/dev.yml"))
    # create event loop
    loop = asyncio.get_event_loop()
    rd1_conn = loop.run_until_complete(rd_client_factory(loop=loop, conf=conf['redis1']))
    rd2_conn = loop.run_until_complete(rd_client_factory(loop=loop, conf=conf['redis2']))
    rgc = RedisGenericCommands(rd1_conn.rd, rd2_conn.rd, conf=conf['redis2'])
    try:
        loop.run_until_complete(rgc.run_generic_cmd())
    except KeyboardInterrupt as e:
        logger.error("Caught keyboard interrupt {0}\nCanceling tasks...".format(e))
    finally:
        loop.run_until_complete(rd1_conn.close_connection())
        loop.run_until_complete(rd2_conn.close_connection())
        loop.close()

if __name__ == '__main__':
    main()
