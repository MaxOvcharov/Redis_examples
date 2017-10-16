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
