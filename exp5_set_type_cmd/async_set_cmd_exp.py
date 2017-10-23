# -*- coding: utf-8 -*-
"""
    Simple example of Redis Set commands using async lib - aioredis
"""
import asyncio
import os

from redis_client import rd_client_factory
from settings import BASE_DIR, logger
from utils import load_config


class RedisSetCommands:
    def __init__(self, rd1, rd2, conf=None):
        self.rd1 = rd1
        self.rd2 = rd2
        self.rd_conf = conf

    async def run_list_cmd(self):
        await self.rd_sadd_cmd()

    async def rd_sadd_cmd(self):
        """
        Add the specified members to the set stored at key.
          Specified members that are already a member of
          this set are ignored. If key does not exist, a
          new set is created before adding the specified members.
          An error is returned when the value stored at key is not a set.
          Return value:
          - the number of elements that were added to the set,
          not including all the elements already present into the set.

        :return: None
        """
        key1, key2 = 'key1', 'key2'
        values1, values2 = ['TEST1', 'TEST2', 'TEST1'], 'TEST1'
        with await self.rd1 as conn:
            res1 = await conn.sadd(key1, *values1)
            res2 = await conn.sadd(key2, values2)
            await conn.delete(key1, key2)
        frm = "HASH_CMD - 'SADD': KEY- {0}, SOME_VALUE - {1}, ONE_VALUE - {2}\n"
        logger.debug(frm.format((key1, key2), res1, res2))


def main():
    # load config from yaml file
    conf = load_config(os.path.join(BASE_DIR, "config_files/dev.yml"))
    # create event loop
    loop = asyncio.get_event_loop()
    # rd1 = await RedisClient.connect(conf=conf['redis'], loop=loop)
    # rd2 = await RedisClient.connect(conf=conf['redis'], loop=loop)
    rd1_conn = loop.run_until_complete(rd_client_factory(loop=loop, conf=conf['redis1']))
    rd2_conn = loop.run_until_complete(rd_client_factory(loop=loop, conf=conf['redis2']))
    rgc = RedisSetCommands(rd1_conn.rd, rd2_conn.rd, conf=conf['redis2'])
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
