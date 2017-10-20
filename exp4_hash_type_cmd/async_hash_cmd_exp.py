# -*- coding: utf-8 -*-
"""
    Simple example of Redis Hash commands using async lib - aioredis
"""
import asyncio
import os

from itertools import chain

from redis_client import rd_client_factory
from settings import BASE_DIR, logger
from utils import load_config


class RedisHashCommands:
    def __init__(self, rd1, rd2, conf=None):
        self.rd1 = rd1
        self.rd2 = rd2
        self.rd_conf = conf

    async def run_list_cmd(self):
        await self.rd_hset_cmd()
        await self.rd_hdel_cmd()
        await self.rd_hexists_cmd()
        await self.rd_hget_cmd()
        await self.rd_hgetall_cmd()
        await self.rd_hincrby_cmd()
        await self.rd_hincrbyfloat_cmd()
        await self.rd_hkeys_cmd()
        await self.rd_hlen_cmd()
        await self.rd_hmget_cmd()
        await self.rd_hmset_dict_cmd()
        await self.rd_hsetnx_cmd()
        await self.rd_hvals_cmd()

    async def rd_hset_cmd(self):
        """
        Sets field in the hash stored at key to value. If key does not exist,
          a new key holding a hash is created. If field already exists in
          the hash, it is overwritten.

        :return: None
        """
        key1 = 'key1'
        field1, field2 = 'f1', 'f2'
        value1, value2 = 'TEST1', 'TEST2'
        with await self.rd1 as conn:
            await conn.hset(key1, field1, value1)
            await conn.hset(key1, field2, value2)
            res1 = await conn.hgetall(key1)
            await conn.hset(key1, field2, value1)
            res2 = await conn.hgetall(key1)
            await conn.delete(key1)
        frm = "HASH_CMD - 'HSET': KEY- {0}, INSERT_DATA- {1}, UPDATE_DATA - {2}\n"
        logger.debug(frm.format(key1, res1, res2))

    async def rd_hdel_cmd(self):
        """
        Removes the specified fields from the hash stored at key.
          Specified fields that do not exist within this hash are
          ignored. If key does not exist, it is treated as an
          empty hash and this command returns 0.

        :return: None
        """
        key1 = 'key1'
        fields = ('f1', 'f2', 'f3')
        values = ('TEST1', 'TEST2', 'TEST3')
        pairs = list(chain(*zip(fields, values)))
        with await self.rd1 as conn:
            await conn.hmset(key1, *pairs)
            res_1 = await conn.hdel(key1, fields[0])
            res1 = await conn.hgetall(key1)
            res_2 = await conn.hdel(key1, *fields[1:])
            res2 = await conn.hgetall(key1)
            res_3 = await conn.hdel(key1, *fields[1:])
            res3 = await conn.hgetall(key1)
            await conn.delete(key1)
        frm = "HASH_CMD - 'HDEL': KEY- {0}, DEL_PART - {1}, DEL_ALL - {2}, DEL_EMPTY - {3}\n"
        logger.debug(frm.format(key1, (res1, res_1), (res2, res_2), (res3, res_3)))

    async def rd_hexists_cmd(self):
        """
        Returns if field is an existing field in the hash stored at key.
          Return value:
            - True - if the hash contains field.
            - False - if the hash does not contain field, or key does not exist.

        :return: None
        """
        key1, field, value = 'key1', ['f1', 'f2'], 'TEST1'
        with await self.rd1 as conn:
            await conn.hset(key1, field[0], value)
            res1 = await conn.hexists(key1, field[0])
            res2 = await conn.hexists(key1, field[1])
            await conn.delete(key1)
        frm = "HASH_CMD - 'HEXISTS': KEY- {0}, EXIST_FIELD - {1}, NOT_EXIST_FIELD - {2}\n"
        logger.debug(frm.format(key1, res1, res2))

    async def rd_hget_cmd(self):
        """
        Returns the value associated with field in the hash stored at key.
          Return value:
            - the value associated with field,
            - None when field is not present in the hash or key does not exist.

        :return: None
        """
        key1, field, value = 'key1', ['f1', 'f2'], 'TEST1'
        with await self.rd1 as conn:
            await conn.hset(key1, field[0], value)
            res1 = await conn.hget(key1, field[0])
            res2 = await conn.hget(key1, field[1])
            await conn.delete(key1)
        frm = "HASH_CMD - 'HGET': KEY- {0}, EXIST_FIELD - {1}, NOT_EXIST_FIELD - {2}\n"
        logger.debug(frm.format(key1, res1, res2))

    async def rd_hgetall_cmd(self):
        """
        Returns all fields and values of the hash stored at key.
          In the returned value, every field name is followed by
          its value, so the length of the reply is twice the
          size of the hash.
          Return value:
            - Array reply: list of fields and their values stored in the hash
            - An empty list when key does not exist.

        :return: None
        """
        key1, key2 = 'key1', 'key2'
        fields = ('f1', 'f2')
        values = ('TEST1', 'TEST2')
        pairs = list(chain(*zip(fields, values)))
        with await self.rd1 as conn:
            await conn.hmset(key1, *pairs)
            res1 = await conn.hgetall(key1)
            res2 = await conn.hgetall(key2)
            await conn.delete(key1)
        frm = "HASH_CMD - 'HGETALL': KEY- {0}, EXIST_FIELDS - {1}, NOT_EXIST_FIELDS - {2}\n"
        logger.debug(frm.format(key1, res1, res2))

    async def rd_hincrby_cmd(self):
        """
        Increments the number stored at field in the hash stored at
          key by increment. If key does not exist, a new key holding
          a hash is created. If field does not exist the value is set
          to 0 before the operation is performed.
          The range of values supported by HINCRBY is limited to 64
          bit signed integers.

        :return: None
        """
        key1 = 'key1'
        field1, field2 = 'f1', 'f2'
        value1 = '5'
        with await self.rd1 as conn:
            await conn.hset(key1, field1, value1)
            res1 = await conn.hincrby(key1, field1, 1)
            res2 = await conn.hincrby(key1, field1, -6)
            res3 = await conn.hincrby(key1, field2, 6)
            await conn.delete(key1)
        frm = "HASH_CMD - 'HINCRBY': K- {0}, V- {1}, " \
              "INCR(1) - {2}, INCR(-6) - {3}, INCR_NX(6)- {4}\n"
        logger.debug(frm.format(key1, value1, res1, res2, res3))

    async def rd_hincrbyfloat_cmd(self):
        """
        Increment the specified field of a hash stored at key,
          and representing a floating point number, by the
          specified increment. If the increment value is negative,
          the result is to have the hash field value decremented
          instead of incremented. If the field does not exist, it
          is set to 0 before performing the operation. An error
          is returned if one of the following conditions occur:
          The field contains a value of the wrong type (not a string).
          The current field content or the specified increment
          are not parsable as a double precision floating point number.

        :return: None
        """
        key1 = 'key1'
        field1, field2 = 'f1', 'f2'
        value1 = '5.0'
        with await self.rd1 as conn:
            await conn.hset(key1, field1, value1)
            res1 = await conn.hincrbyfloat(key1, field1, 1.5)
            res2 = await conn.hincrbyfloat(key1, field1, -6)
            res3 = await conn.hincrbyfloat(key1, field2, 6)
            await conn.delete(key1)
        frm = "HASH_CMD - 'HINCRBYFLOAT': K- {0}, V- {1}, " \
              "INCR(1.5) - {2}, INCR(-6) - {3}, INCR_NX(6) - {4}\n"
        logger.debug(frm.format(key1, value1, res1, res2, res3))

    async def rd_hkeys_cmd(self):
        """
        Returns all field names in the hash stored at key.
          Array reply:
          list of fields in the hash, or an empty list when
          key does not exist.

        :return: None
        """
        key1, key2 = 'key1', 'key2'
        fields = ('f1', 'f2', 'f3')
        values = ('TEST1', 'TEST2', 'TEST3')
        pairs = list(chain(*zip(fields, values)))
        with await self.rd1 as conn:
            await conn.hmset(key1, *pairs)
            res1 = await conn.hkeys(key1)
            res2 = await conn.hkeys(key2)
            await conn.delete(key1)
        frm = "HASH_CMD - 'HKEYS': KEY- {0}, EXIST_KEY - {1}, NOT_EXIST_KEY - {2}\n"
        logger.debug(frm.format(key1, res1, res2))

    async def rd_hlen_cmd(self):
        """
        Returns the number of fields contained in the hash stored at key.
          Integer reply:
          - number of fields in the hash
          - 0 when key does not exist.

        :return: None
        """
        key1, key2 = 'key1', 'key2'
        fields = ('f1', 'f2', 'f3')
        values = ('TEST1', 'TEST2', 'TEST3')
        pairs = list(chain(*zip(fields, values)))
        with await self.rd1 as conn:
            await conn.hmset(key1, *pairs)
            res1 = await conn.hlen(key1)
            res2 = await conn.hlen(key2)
            await conn.delete(key1)
        frm = "HASH_CMD - 'HLEN': KEY- {0}, EXIST_KEY_LEN - {1}, NOT_EXIST_KEY_LEN - {2}\n"
        logger.debug(frm.format(key1, res1, res2))

    async def rd_hmget_cmd(self):
        """
        Returns the values associated with the specified
          fields in the hash stored at key. For every
          field that does not exist in the hash, a nil
          value is returned. Because a non-existing keys
          are treated as empty hashes, running HMGET
          against a non-existing key will return a
          list of nil values.
          Return value:
            list of values associated with the given fields,
            in the same order as they are requested.

        :return: None
        """
        key1 = 'key1'
        fields = ('f1', 'f2', 'f3')
        values = ('TEST1', 'TEST2', 'TEST3')
        pairs = list(chain(*zip(fields, values)))
        with await self.rd1 as conn:
            await conn.hmset(key1, *pairs)
            res1 = await conn.hmget(key1, *fields)
            await conn.delete(key1)
        frm = "HASH_CMD - 'HMGET': KEY- {0}, HMGET_VALUE - {1}\n"
        logger.debug(frm.format(key1, res1))

    async def rd_hmset_dict_cmd(self):
        """
        Returns the values associated with the specified
          fields in the hash stored at key. For every
          field that does not exist in the hash, a nil
          value is returned. Because a non-existing keys
          are treated as empty hashes, running HMGET
          against a non-existing key will return a
          list of nil values.
          Return value:
            list of values associated with the given fields,
            in the same order as they are requested.

        :return: None
        """
        key1 = 'key1'
        fields = ('f1', 'f2', 'f3')
        values = ('TEST1', 'TEST2', 'TEST3')
        pairs = dict(zip(fields, values))
        with await self.rd1 as conn:
            await conn.hmset_dict(key1, pairs)
            res1 = await conn.hmget(key1, *fields)
            await conn.delete(key1)
        frm = "HASH_CMD - 'HMSET_DICT': KEY- {0}, HMSET_VALUE_RES - {1}\n"
        logger.debug(frm.format(key1, res1))

    async def rd_hsetnx_cmd(self):
        """
        Sets field in the hash stored at key to value,
          only if field does not yet exist. If key does
          not exist, a new key holding a hash is created.
          If field already exists, this operation has no effect.
          Return value:
          - 1 if field is a new field in the hash and value was set.
          - 0 if field already exists in the hash and no operation
            was performed.

        :return: None
        """
        key1 = 'key1'
        fields = ('f1', 'f2', 'f3')
        values = ('TEST1', 'TEST2', 'TEST3')
        pairs = dict(zip(fields, values))
        with await self.rd1 as conn:
            await conn.hmset_dict(key1, pairs)
            res1 = await conn.hsetnx(key1, fields[0], values[0])
            res2 = await conn.hsetnx(key1, 'f4', 'TEST4')
            await conn.delete(key1)
        frm = "HASH_CMD - 'HSETNX': KEY- {0}, EXIST_FIELD - {1}, NOT_EXIST_FIELD - {2}\n"
        logger.debug(frm.format(key1, res1, res2))

    async def rd_hvals_cmd(self):
        """
        Returns all values in the hash stored at key.
          Return value:
          - list of values in the hash
          - an empty list when key does not exist.

        :return: None
        """
        key1, key2 = 'key1', 'key2'
        fields = ('f1', 'f2', 'f3')
        values = ('TEST1', 'TEST2', 'TEST3')
        pairs = dict(zip(fields, values))
        with await self.rd1 as conn:
            await conn.hmset_dict(key1, pairs)
            res1 = await conn.hvals(key1)
            res2 = await conn.hvals(key2)
            await conn.delete(key1)
        frm = "HASH_CMD - 'HVALS': KEY- {0}, EXIST_KEY - {1}, NOT_EXIST_KEY - {2}\n"
        logger.debug(frm.format(key1, res1, res2))


def main():
    # load config from yaml file
    conf = load_config(os.path.join(BASE_DIR, "config_files/dev.yml"))
    # create event loop
    loop = asyncio.get_event_loop()
    # rd1 = await RedisClient.connect(conf=conf['redis'], loop=loop)
    # rd2 = await RedisClient.connect(conf=conf['redis'], loop=loop)
    rd1_conn = loop.run_until_complete(rd_client_factory(loop=loop, conf=conf['redis1']))
    rd2_conn = loop.run_until_complete(rd_client_factory(loop=loop, conf=conf['redis2']))
    rgc = RedisHashCommands(rd1_conn.rd, rd2_conn.rd, conf=conf['redis2'])
    try:
        loop.run_until_complete(rgc.run_list_cmd())
    except KeyboardInterrupt as e:
        logger.error("Caught keyboard interrupt {0}\nCanceling tasks...".format(e))
    finally:
        loop.run_until_complete(rd1_conn.close_connection())
        loop.run_until_complete(rd2_conn.close_connection())
        loop.close()

if __name__ == '__main__':
    main()
