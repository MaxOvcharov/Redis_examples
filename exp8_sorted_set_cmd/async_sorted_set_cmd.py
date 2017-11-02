# -*- coding: utf-8 -*-
"""
    Simple example of Redis SortedSet commands using async lib - aioredis
"""
import asyncio
import os

from itertools import chain
from random import choice

from redis_client import rd_client_factory
from settings import BASE_DIR, logger
from utils import load_config


class RedisSortedSetCommands:
    def __init__(self, rd1, rd2, conf=None):
        self.rd1 = rd1
        self.rd2 = rd2
        self.rd_conf = conf

    async def run_sorted_set_cmd(self):
        await self.rd_zadd_cmd()
        await self.rd_zcard_cmd()
        await self.rd_zcount_cmd()

    async def rd_zadd_cmd(self):
        """
        Adds all the specified members with the specified
          scores to the sorted set stored at key. It is
          possible to specify multiple score / member pairs.
          If a specified member is already a member of the
          sorted set, the score is updated and the element
          reinserted at the right position to ensure the
          correct ordering.
          Return value:
          - The number of elements added to the sorted sets,
            not including elements already existing for which
            the score was updated.
          - If the INCR option is specified, the return value
            will be Bulk string reply: the new score of member
            (a double precision floating point number),
            represented as string.

        :return: None
        """
        key1 = 'key1'
        values = ('TEST1', 'TEST1', 'TEST2', 'TEST3')
        scores = (1, 2, 2, 1)
        pairs = list(chain(*zip(scores, values)))
        with await self.rd1 as conn:
            await conn.zadd(key1, *pairs)
            res1 = await conn.zrange(key1, 0, -1, withscores=True)
            await conn.delete(key1)
        frm = "SORTED_SET_CMD - 'ZADD': KEY- {0}, RES_VALUE - {1}\n"
        logger.debug(frm.format(key1, res1))

    async def rd_zcard_cmd(self):
        """
        Returns the sorted set cardinality (number of
          elements) of the sorted set stored at key.
          Return value:
          - the cardinality (number of elements) of the
            sorted set, or 0 if key does not exist.

        :return: None
        """
        key1, key2 = 'key1', 'key2'
        values = ('TEST1', 'TEST1', 'TEST2', 'TEST3')
        scores = (1, 2, 2, 1)
        pairs = list(chain(*zip(scores, values)))
        with await self.rd1 as conn:
            await conn.zadd(key1, *pairs)
            res1 = await conn.zcard(key1)
            res2 = await conn.zcard(key2)
            await conn.delete(key1)
        frm = "SORTED_SET_CMD - 'ZCARD': KEY- {0}, ZCARD_EXIST_SET - {1}," \
              " ZCARD_NOT_EXIST_SET - {2}\n"
        logger.debug(frm.format(key1, res1, res2))

    async def rd_zcount_cmd(self):
        """
        Returns the number of elements in the sorted
          set at key with a score between min and max.
          The command has a complexity of just O(log(N))
          because it uses elements ranks (see ZRANK) to
          get an idea of the range. Because of this there
          is no need to do a work proportional to the size
          of the range.
          Return value:
          - Integer reply: the number of elements in the
            specified score range.
        :return: None
        """
        key1, key2 = 'key1', 'key2'
        values = ('TEST1', 'TEST2', 'TEST3', 'TEST4', 'TEST5')
        scores = (1, 2, 2, 1, 2)
        pairs = list(chain(*zip(scores, values)))
        with await self.rd1 as conn:
            await conn.zadd(key1, *pairs)
            res1 = await conn.zcount(key1, 2, 2)
            res2 = await conn.zcount(key2)
            await conn.delete(key1)
        frm = "SORTED_SET_CMD - 'ZCOUNT': KEY- {0}, ZCOUNT_EXIST_SET - {1}," \
              " ZCOUNT_NOT_EXIST_SET - {2}\n"
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
    rgc = RedisSortedSetCommands(rd1_conn.rd, rd2_conn.rd, conf=conf['redis2'])
    try:
        loop.run_until_complete(rgc.run_sorted_set_cmd())
    except KeyboardInterrupt as e:
        logger.error("Caught keyboard interrupt {0}\nCanceling tasks...".format(e))
    finally:
        loop.run_until_complete(rd1_conn.close_connection())
        loop.run_until_complete(rd2_conn.close_connection())
        loop.close()

if __name__ == '__main__':
    main()
