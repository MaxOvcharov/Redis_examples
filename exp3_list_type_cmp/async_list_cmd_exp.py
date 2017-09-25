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
        frm = "GENERIC_CMD - 'RPUSH': KEY- {0}, INDEX_NUM- {1}, RES - {2}\n"
        logger.debug(frm.format(key1, push_index, res))


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
