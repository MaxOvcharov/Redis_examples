# -*- coding: utf-8 -*-
"""
    Simple example of Redis Generic commands using async lib - aioredis
"""
import asyncio
import aioredis
import datetime as dt
import os
import random
import string

from random import choice

from redis_client import rd_client_factory
from settings import BASE_DIR, logger
from utils import load_config


class RedisListCommands:
    def __init__(self, rd1, rd2, conf=None):
        self.rd1 = rd1
        self.rd2 = rd2
        self.rd_conf = conf

    async def run_list_cmd(self):
        await self.rd_rpush_cmd()
        await self.rd_rpushx_cmd()
        await self.rd_blpop_cmd()
        await self.rd_brpop_cmd()

    async def rd_rpush_cmd(self):
        """
        Insert all the specified values at the tail of the list stored
          at key. If key does not exist, it is created as empty list
          before performing the push operation. When key holds a value
          that is not a list, an error is returned.
          It is possible to push multiple elements using a single command
          call just specifying multiple arguments at the end of the command.
          Elements are inserted one after the other to the tail of the list,
          from the leftmost element to the rightmost element. So for instance
          the command RPUSH mylist a b c will result into a list containing
          a as first element, b as second element and c as third element.

        :return: None
        """
        key1 = 'key_list1'
        values = ['TEST1', 'TEST2', 'TEST3']
        with await self.rd1 as conn:
            push_index = await conn.rpush(key1, *values)
            res = await conn.lrange(key1, 0, -1)
            await conn.delete(key1)
        frm = "LIST_CMD - 'RPUSH': KEY- {0}, INDEX_NUM- {1}, RES - {2}\n"
        logger.debug(frm.format(key1, push_index, res))

    async def rd_rpushx_cmd(self):
        """
        Inserts value at the tail of the list stored at key, only if key
          already exists and holds a list. In contrary to RPUSH, no
          operation will be performed when key does not yet exist.

        :return: None
        """
        key1, key2 = 'key1', 'key2'
        values_push = 'TEST1'
        values_pushx = 'TEST2'
        with await self.rd1 as conn:
            await conn.rpush(key1, values_push)
            pushx_index1 = await conn.rpushx(key1, values_pushx)
            pushx_index2 = await conn.rpushx(key2, values_pushx)
            res1 = await conn.lrange(key1, 0, -1)
            res2 = await conn.lrange(key2, 0, -1)
            await conn.delete(key1, key2)
        frm = "LIST_CMD - 'RPUSHX': KEYS- {0}, INDEX_EXIST- {1}, " \
              "INDEX_NOT_EXIST- {2}, RES - {3}\n"
        logger.debug(frm.format([key1, key2], pushx_index1, pushx_index2, [res1, res2]))

    async def rd_blpop_cmd(self):
        """
        BLPOP is a blocking list pop primitive. It is the blocking version
          of LPOP because it blocks the connection when there are no
          elements to pop from any of the given lists. An element is popped
          from the head of the first list that is non-empty, with the given
          keys being checked in the order that they are given.

        :return: None
        """
        key1, key2 = 'key1', 'key2'
        values_rpush = ('TEST1', 'TEST2)')
        with await self.rd1 as conn:
            await conn.rpush(key1, *values_rpush)
            res1_1 = await conn.blpop(key1, timeout=1)
            res2_1 = await conn.blpop(key2, timeout=1)
            res1_2 = await conn.blpop(key1, timeout=1)
            res2_2 = await conn.blpop(key2, timeout=1)
            await conn.delete(key1, key2)
        frm = "LIST_CMD - 'BLPOP': KEYS- {0}, RES - {1}\n"
        logger.debug(frm.format([key1, key2], (res1_1, res2_1, res1_2, res2_2)))

    async def rd_brpop_cmd(self):
        """
        BRPOP is a blocking list pop primitive. It is the blocking version of
          RPOP because it blocks the connection when there are no elements to
          pop from any of the given lists. An element is popped from the tail
          of the first list that is non-empty, with the given keys being
          checked in the order that they are given.

        :return: None
        """
        key1, key2 = 'key1', 'key2'
        values_rpush = ('TEST1', 'TEST2)')
        with await self.rd1 as conn:
            await conn.rpush(key1, *values_rpush)
            res1_1 = await conn.brpop(key1, timeout=1)
            res2_1 = await conn.brpop(key2, timeout=1)
            res1_2 = await conn.brpop(key1, timeout=1)
            res2_2 = await conn.brpop(key2, timeout=1)
            await conn.delete(key1, key2)
        frm = "LIST_CMD - 'BRPOP': KEYS- {0}, RES - {1}\n"
        logger.debug(frm.format([key1, key2], (res1_1, res2_1, res1_2, res2_2)))


def main():
    # load config from yaml file
    conf = load_config(os.path.join(BASE_DIR, "config_files/dev.yml"))
    # create event loop
    loop = asyncio.get_event_loop()
    # rd1 = await RedisClient.connect(conf=conf['redis'], loop=loop)
    # rd2 = await RedisClient.connect(conf=conf['redis'], loop=loop)
    rd1_conn = loop.run_until_complete(rd_client_factory(loop=loop, conf=conf['redis1']))
    rd2_conn = loop.run_until_complete(rd_client_factory(loop=loop, conf=conf['redis2']))
    rgc = RedisListCommands(rd1_conn.rd, rd2_conn.rd, conf=conf['redis2'])
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
