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
        await self.server_bgsave_cmd()
        await self.server_client_list_cmd()

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

    async def server_bgsave_cmd(self):
        """
        Save the DB in background. The OK code is immediately
          returned. Redis forks, the parent continues to serve
          the clients, the child saves the DB on disk then exits.
          A client may be able to check if the operation
          succeeded using the LASTSAVE command.

        :return: None
        """
        with await self.rd1 as conn:
            ls_before = await conn.lastsave()
            res1 = await conn.bgsave()
            await asyncio.sleep(2)
            ls_after = await conn.lastsave()
        frm = "SERVER_CMD - 'BGSAVE': BGSAVE_RES - {0}, LAST_SAVE_AFTER - {1}, " \
              "LAST_SAVE_BEFORE - {2}\n"
        logger.debug(frm.format(res1, ls_before, ls_after))

    async def server_client_list_cmd(self):
        """
        The CLIENT LIST command returns information and
          statistics about the client connections server in
          a mostly human readable format.
          Here is the meaning of the fields:
            - id: an unique 64-bit client ID (introduced in Redis 2.8.12).
            - addr: address/port of the client
            - fd: file descriptor corresponding to the socket
            - age: total duration of the connection in seconds
            - idle: idle time of the connection in seconds
            - flags: client flags (see below)
            - db: current database ID
            - sub: number of channel subscriptions
            - psub: number of pattern matching subscriptions
            - multi: number of commands in a MULTI/EXEC context
            - qbuf: query buffer length (0 means no query pending)
            - qbuf-free: free space of the query buffer (0 means the buffer is full)
            - obl: output buffer length
            - oll: output list length (replies are queued in this list when the buffer is full)
            - omem: output buffer memory usage
            - events: file descriptor events (see below)
            -cmd: last command played

        :return: None
        """
        with await self.rd1 as conn:
            res1 = await conn.client_list()
        frm = "SERVER_CMD - 'CLIENT_LIST': RES - {0}\n"
        logger.debug(frm.format(res1))


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
