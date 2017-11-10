# -*- coding: utf-8 -*-
"""
    Simple example of Redis Transactions commands using async lib - aioredis
    For commands details see: http://redis.io/commands/#transactions
"""
import asyncio
import os

from redis_client import rd_client_factory
from settings import BASE_DIR, logger
from utils import load_config


class RedisTransactionCommands:
    def __init__(self, rd1, rd2, conf=None):
        self.rd1 = rd1
        self.rd2 = rd2
        self.rd_conf = conf

    async def run_transaction_cmd(self):
        await self.rd_multi_exec_cmd()
        await self.rd_pipeline_cmd()
        await self.rd_watch_cmd()
        await self.rd_unwatch_cmd()

    async def rd_multi_exec_cmd(self):
        """
        Marks the start of a transaction block.
          Subsequent commands will be queued for atomic
          execution using EXEC.

        :return: None
        """
        key1, key2 = 'key1', 'key2'
        value1, value2 = '10', '2'
        with await self.rd1 as conn:
            await conn.set(key1, value1)
            await conn.set(key2, value2)
            tr = conn.multi_exec()
            fut1 = tr.incr(key1)
            fut2 = tr.incr(key2)
            res1 = await tr.execute()
            res2 = await asyncio.gather(fut1, fut2)
            await conn.delete(key1, key2)
        frm = "TRANSACTION_CMD - 'MULTI_EXEC': KEY - {0}, BEFORE - {1}," \
              " AFTER_MULTI_EXEC - {2}, AFTER_SIMPLE_EXEC - {3}\n"
        logger.debug(frm.format((key1, key2), (value1, value2), res1, res2))

    async def rd_pipeline_cmd(self):
        """
        Returns :class:`Pipeline` object to execute bulk of commands.
          It is provided for convenience.
          Commands can be pipelined without it.

        :return: None
        """
        key1, key2 = 'key1', 'key2'
        value1, value2 = '10', '2'
        with await self.rd1 as conn:
            await conn.set(key1, value1)
            await conn.set(key2, value2)
            pipe = conn.pipeline()
            fut1 = pipe.incr(key1)
            fut2 = pipe.incr(key2)
            res1 = await pipe.execute()
            res2 = await asyncio.gather(fut1, fut2)
            await conn.delete(key1, key2)
        frm = "TRANSACTION_CMD - 'PIPELINE': KEY - {0}, BEFORE - {1}," \
              " AFTER_PIPELINE - {2}, AFTER_SIMPLE_EXEC - {3}\n"
        logger.debug(frm.format((key1, key2), (value1, value2), res1, res2))

    async def rd_watch_cmd(self):
        """
        Watch the given keys to determine execution
          of the MULTI/EXEC block.

        :return: None
        """
        key1, key2 = 'key1', 'key2'
        value1, value2 = '10', '2'
        with await self.rd1 as conn:
            await conn.set(key1, value1)
            await conn.set(key2, value2)
            await conn.watch(key1, key2)
            tr = conn.multi_exec()
            fut1 = tr.incr(key1)
            fut2 = tr.incr(key2)
            res1 = await tr.execute()
            res2 = await asyncio.gather(fut1, fut2)
            await conn.delete(key1, key2)
        frm = "TRANSACTION_CMD - 'WATCH': KEY - {0}, BEFORE - {1}," \
              " AFTER_MULTI_EXEC - {2}, AFTER_SIMPLE_EXEC - {3}\n"
        logger.debug(frm.format((key1, key2), (value1, value2), res1, res2))

    async def rd_unwatch_cmd(self):
        """
        Forget about all watched keys.

        :return: None
        """
        key1, key2 = 'key1', 'key2'
        value1, value2 = '10', '2'
        with await self.rd1 as conn:
            await conn.set(key1, value1)
            await conn.set(key2, value2)
            await conn.watch(key1, key2)
            tr = conn.multi_exec()
            fut1 = tr.incr(key1)
            await conn.unwatch()
            fut2 = tr.incr(key2)
            res1 = await tr.execute()
            res2 = await asyncio.gather(fut1, fut2)
            await conn.delete(key1, key2)
        frm = "TRANSACTION_CMD - 'WATCH': KEY - {0}, BEFORE - {1}," \
              " AFTER_MULTI_EXEC - {2}, AFTER_SIMPLE_EXEC - {3}\n"
        logger.debug(frm.format((key1, key2), (value1, value2), res1, res2))


def main():
    # load config from yaml file
    conf = load_config(os.path.join(BASE_DIR, "config_files/dev.yml"))
    # create event loop
    loop = asyncio.get_event_loop()
    # rd1 = await RedisClient.connect(conf=conf['redis'], loop=loop)
    # rd2 = await RedisClient.connect(conf=conf['redis'], loop=loop)
    rd1_conn = loop.run_until_complete(rd_client_factory(loop=loop, conf=conf['redis1']))
    rd2_conn = loop.run_until_complete(rd_client_factory(loop=loop, conf=conf['redis2']))
    rgc = RedisTransactionCommands(rd1_conn.rd, rd2_conn.rd, conf=conf['redis2'])
    try:
        loop.run_until_complete(rgc.run_transaction_cmd())
    except KeyboardInterrupt as e:
        logger.error("Caught keyboard interrupt {0}\nCanceling tasks...".format(e))
    finally:
        loop.run_until_complete(rd1_conn.close_connection())
        loop.run_until_complete(rd2_conn.close_connection())
        loop.close()

if __name__ == '__main__':
    main()
