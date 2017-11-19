# -*- coding: utf-8 -*-
"""
    Simple example of Redis Server commands using async lib - aioredis
    For commands details see: http://redis.io/commands/#server
"""
import asyncio
import os

from redis_client import rd_client_factory
from settings import BASE_DIR, logger
from utils import load_config


class RedisServerCommands:
    def __init__(self, rd1, rd2, conf=None):
        self.rd1 = rd1
        self.rd2 = rd2
        self.rd_conf = conf

    async def run_server_cmd(self):
        await self.server_bgrewriteaof_cmd()

    async def server_bgrewriteaof_cmd(self):
        """
        Instruct Redis to start an Append Only File
          rewrite process. The rewrite will create a
          small optimized version of the current
          Append Only File.
          If BGREWRITEAOF fails, no data gets lost
          as the old AOF will be untouched.
          The rewrite will be only triggered by Redis
          if there is not already a background process
          doing persistence.

        :return: None
        """
        key1, key2 = 'key_list1', 'key_list2'
        values1, values2 = ['TEST1', 'TEST2', 'TEST3'], ['test1', 'test2']
        with await self.rd1 as conn:
            await conn.rpush(key1, *values1)
            await conn.rpush(key2, *values2)
        with await self.rd1 as conn:
            aof_dir = await conn.config_get(parameter='dir')
            res1 = await conn.bgrewriteaof()
            await asyncio.sleep(2)
            res2 = os.listdir(aof_dir['dir'])
        frm = "SERVER_CMD - 'BGREWRITEAOF': RES - {0}, AOF_DIR - {1}, AOF_EXIST - {2}\n"
        logger.debug(frm.format(res1, aof_dir, res2))


def main():
    # load config from yaml file
    conf = load_config(os.path.join(BASE_DIR, "config_files/dev.yml"))
    # create event loop
    loop = asyncio.get_event_loop()
    # rd1 = await RedisClient.connect(conf=conf['redis'], loop=loop)
    # rd2 = await RedisClient.connect(conf=conf['redis'], loop=loop)
    rd1_conn = loop.run_until_complete(rd_client_factory(loop=loop, conf=conf['redis1']))
    rd2_conn = loop.run_until_complete(rd_client_factory(loop=loop, conf=conf['redis2']))
    rgc = RedisServerCommands(rd1_conn.rd, rd2_conn.rd, conf=conf['redis2'])
    try:
        loop.run_until_complete(rgc.run_server_cmd())
    except KeyboardInterrupt as e:
        logger.error("Caught keyboard interrupt {0}\nCanceling tasks...".format(e))
    finally:
        loop.run_until_complete(rd1_conn.close_connection())
        loop.run_until_complete(rd2_conn.close_connection())
        loop.close()


if __name__ == '__main__':
    main()
