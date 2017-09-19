# -*- coding: utf-8 -*-
"""
    Simple example of Redis Generic commands using async lib - aioredis
"""
import asyncio
import aioredis
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
        await self.rd_del_cmd()
        await self.rd_dump_cmd()
        await self.rd_exists_cmd()
        await self.rd_expire_cmd()
        await self.rd_expireat_cmd()
        await self.rd_keys_cmd()
        await self.rd_migrate_cmd()
        await self.rd_move_cmd()
        await self.rd_object_refcount_cmd()
        await self.rd_object_encoding_cmd()
        await self.rd_object_idletime_cmd()
        await self.rd_persist_cmd()
        await self.rd_pexpire_cmd()
        await self.rd_pexpireat_cmd()
        await self.rd_pttl_cmd()
        await self.rd_randomkey_cmd()
        await self.rd_rename_cmd()
        await self.rd_renamenx_cmd()
        await self.rd_restore_cmd()
        await self.rd_ttl_cmd()

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

    async def rd_object_idletime_cmd(self):
        """
        The OBJECT command supports multiple sub commands:
          OBJECT IDLETIME <key> returns the number of seconds since
          the object stored at the specified key is idle (not requested
          by read or write operations). While the value is returned in
          seconds the actual resolution of this timer is 10 seconds,
          but may vary in future implementations.

        :return: None
        """
        key1 = 'key_1'
        value1 = 'TEST1'
        with await self.rd1 as conn:
            await conn.set(key1, value1)
            await asyncio.sleep(3)
            res = await conn.object_idletime(key1)
            await conn.delete(key1)
        frm = "GENERIC_CMD - 'OBJECT IDLETIME': KEY- %s, IDLETIME - %s\n"
        logger.debug(frm, key1, res)

    async def rd_persist_cmd(self):
        """
        Remove the existing timeout on key, turning the key from volatile
          (a key with an expire set) to persistent (a key that will never
          expire as no timeout is associated).
          Return value:
            True if the timeout was removed.
            False if key does not exist or does not have an associated timeout.

        :return: None
        """
        key1 = 'key_1'
        value1 = 'TEST1'
        ttl = 10
        with await self.rd1 as conn:
            await conn.setex(key1, ttl, value1)
            res_1 = await conn.ttl(key1)
            pers_res1 = await conn.persist(key1)
            res_2 = await conn.ttl(key1)
            pers_res2 = await conn.persist(key1)
            await conn.delete(key1)
        frm = "GENERIC_CMD - 'PERSIST': KEY- %s, TTL_BEFORE - %s," \
              " TTL_AFTER - %s, PERS_BEFORE - %s, PERS_AFTER - %s\n"
        logger.debug(frm, key1, res_1, res_2, pers_res1, pers_res2)

    async def rd_pexpire_cmd(self):
        """
        This command works exactly like EXPIRE but the time to live
          of the key is specified in milliseconds instead of seconds.
          Return value:
            True if the timeout was set.
            False if key does not exist.

        :return: None
        """
        key1 = 'key_1'
        value1 = 'TEST1'
        pttl = 10000
        with await self.rd1 as conn:
            await conn.set(key1, value1)
            res_1 = await conn.pttl(key1)
            await conn.pexpire(key1, pttl)
            await asyncio.sleep(1)
            res_2 = await conn.pttl(key1)
            await conn.delete(key1)
        frm = "GENERIC_CMD - 'PEXPIRE': KEY- %s, PTTL_BEFORE - %s, PTTL_AFTER - %s\n"
        logger.debug(frm, key1, res_1, res_2)

    async def rd_pexpireat_cmd(self):
        """
        PEXPIREAT has the same effect and semantic as EXPIREAT, but the
          Unix time at which the key will expire is specified in
          milliseconds instead of seconds.
          Return value:
            True - if the timeout was set.
            False - if key does not exist.

        :return: None
        """
        key1 = 'key_1'
        value1 = 'TEST1'
        date_of_ex = (dt.datetime.now() + dt.timedelta(days=1)).timestamp()
        with await self.rd1 as conn:
            await conn.set(key1, value1)
            await conn.pexpireat(key1, round(date_of_ex) * 1000)
            await asyncio.sleep(1)
            res_1 = await conn.ttl(key1)
            res_2 = await conn.pttl(key1)
            await conn.delete(key1)
        frm = "GENERIC_CMD - 'PEXPIREAT': KEY- %s, TTL - %s sec, PTTL- %s msec\n"
        logger.debug(frm, key1, res_1, res_2)

    async def rd_pttl_cmd(self):
        """
        Like TTL this command returns the remaining time to live of a key
          that has an expire set, with the sole difference that TTL returns
          the amount of remaining time in seconds while PTTL returns it
          in milliseconds.
          Return value in case of error changed:
            -2 - if the key does not exist.
            -1 - if the key exists but has no associated expire.

        :return: None
        """
        key1 = 'key_1'
        value1 = 'TEST1'
        pttl = 10000
        with await self.rd1 as conn:
            res_1 = await conn.pttl(key1)
            await conn.set(key1, value1)
            res_2 = await conn.pttl(key1)
            await conn.pexpire(key1, pttl)
            await asyncio.sleep(1)
            res_3 = await conn.pttl(key1)
            await conn.delete(key1)
        frm = "GENERIC_CMD - 'PTTL': KEY - %s, PTTL_NO_KEY - %s, PTTL_NO_EXP - %s, PTTL - %s msec\n"
        logger.debug(frm, key1, res_1, res_2, res_3)

    async def rd_randomkey_cmd(self):
        """
        Return a random key from the currently selected database.
          Return value:
            Bulk string reply: the random key, or nil when the database is empty.

        :return: None
        """
        key1 = 'key_1'
        value1 = 'TEST1'
        with await self.rd1 as conn:
            res_1 = await conn.randomkey()
            await conn.set(key1, value1)
            res_2 = await conn.randomkey()
            await conn.delete(key1)
        frm = "GENERIC_CMD - 'RANDOMKEY': KEY- %s, BEFORE_RANDOMKEY - %s, AFTER_RANDOMKEY - %s\n"
        logger.debug(frm, key1, res_1, res_2)

    async def rd_rename_cmd(self):
        """
        Renames key to newkey. It returns an error when key does not exist.
          If newkey already exists it is overwritten, when this happens
          RENAME executes an implicit DEL operation, so if the deleted
          key contains a very big value it may cause high latency even
          if RENAME itself is usually a constant-time operation.

        :return: None
        """
        key1, key2 = 'key_1', 'new_key'
        value1 = 'TEST1'
        res1, res_rename1 = None, None
        try:
            with await self.rd1 as conn:
                await conn.set(key1, value1)
                res_rename1 = await conn.rename(key1, key2)
                res1 = await conn.get(key2)
                res_rename2 = await conn.rename(key1, key2)
        except aioredis.errors.ReplyError as e:
            await conn.delete(key1, key2)
            res_rename2 = e
        frm = "GENERIC_CMD - 'RENAME': KEY- %s, RENAME - %s," \
              " EXIST_KEY - %s, NOT_EXIST_KEY - %s\n"
        logger.debug(frm, [key1, key2], res1, res_rename1, res_rename2)

    async def rd_renamenx_cmd(self):
        """
        Renames key to newkey if newkey does not yet exist.
          It returns an error when key does not exist.
          Return value:
            True if key was renamed to newkey.
            False if newkey already exists.

        :return: None
        """
        key1, key2, key3 = 'key_1', 'new_key', 'exist_key'
        value1, value3 = 'TEST1', 'TEST3'
        res1, res_rename1, res_rename2, res_rename2 = None, None, None, None
        try:
            with await self.rd1 as conn:
                await conn.mset(key1, value1, key3, value3)
                res_rename1 = await conn.renamenx(key1, key2)
                res1 = await conn.mget(key2)
                res_rename2 = await conn.renamenx(key2, key3)
                res_rename3 = await conn.renamenx(key1, key2)
        except aioredis.errors.ReplyError as e:
            await conn.delete(key1, key2, key3)
            res_rename3 = e
        frm = "GENERIC_CMD - 'RENAMENX': RENAME - %s," \
              " NOT_EXIST_NEW_KEY - %s, EXIST_KEY_NEW - %s, NOT_EXIST_KEY - %s\n"
        logger.debug(frm, res1, res_rename1, res_rename2, res_rename3)

    async def rd_restore_cmd(self):
        """
        Create a key associated with a value that is obtained by
          deserializing the provided serialized value (obtained via DUMP).
          If ttl is 0 the key is created without any expire, otherwise
          the specified expire time (in milliseconds) is set.
          RESTORE will return a "Target key name is busy" error when key
          already exists unless you use the REPLACE modifier (Redis 3.0 or greater).
          RESTORE checks the RDB version and data checksum. If they don't
          match an error is returned.

        :return: None
        """
        key1 = 'key_1'
        value1 = 'TEST1'
        value_restore = "\n\x17\x17\x00\x00\x00\x12\x00\x00\x00\x03\x00\
                        x00\xc0\x01\x00\x04\xc0\x02\x00\x04\xc0\x03\x00\
                        xff\x04\x00u#<\xc0;.\xe9\xdd"
        ttl = 0
        res_restore, res_type, res = None, None, None
        try:
            with await self.rd1 as conn:
                await conn.set(key1, value1)
                await conn.delete(key1)
                res_restore = await conn.restore(key1, ttl, value_restore)
                res_type = await conn.type(key1)
                res = await conn.LRANGE(key1, 0, -1)
        except aioredis.errors.ReplyError as e:
            res_restore = e
            await conn.delete(key1)
        frm = "GENERIC_CMD - 'RESTORE': RESTORE_RES - %s, KEY_TYPE - %s, RESTORE_VALUE - %s\n"
        logger.debug(frm, res_restore, res_type, res)

    async def rd_ttl_cmd(self):
        """
        Returns the remaining time to live of a key that
          has a timeout. This introspection capability
          allows a Redis client to check how many seconds
          a given key will continue to be part of the dataset.
          Return value in case of error changed:
            - '-2' - if the key does not exist.
            - '-1' - if the key exists but has no associated expire.

        :return: None
        """
        key1, key2, key3 = 'key_1', 'key_2', 'key_3'
        value1, value2 = 'test_ttl', 'test_ttl_not_ex'
        ttl = 10
        with await self.rd1 as conn:
            await conn.setex(key1, ttl, value1)
            await conn.set(key2, value2)
            await asyncio.sleep(2)
            res1 = await conn.ttl(key1)
            res2 = await conn.ttl(key2)
            res3 = await conn.ttl(key3)
            await conn.delete(key1, key2, key3)
        frm = "GENERIC_CMD - 'TTL': KEY- {0}, TTL_OK - {1}," \
              " TTL_NOT_EXPIRE - {2}, TTL_NOT_EXIST - {3}, \n"
        logger.debug(frm.format([key1, key2, key3], res1, res2, res3))


def main():
    # load config from yaml file
    conf = load_config(os.path.join(BASE_DIR, "config_files/dev.yml"))
    # create event loop
    loop = asyncio.get_event_loop()
    # rd1 = await RedisClient.connect(conf=conf['redis'], loop=loop)
    # rd2 = await RedisClient.connect(conf=conf['redis'], loop=loop)
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
