# -*- coding: utf-8 -*-
"""
    Simple example of Redis HyperLogLog commands using async lib - aioredis
"""
import asyncio
import os
import string
from random import choice


from redis_client import rd_client_factory
from settings import BASE_DIR, logger
from utils import load_config


class RedisHyperLogLogCommands:
    def __init__(self, rd1, rd2, conf=None):
        self.rd1 = rd1
        self.rd2 = rd2
        self.rd_conf = conf

    async def run_list_cmd(self):
        await self.rd_pfadd_cmd()

    async def rd_pfadd_cmd(self):
        """
        Adds all the element arguments to the HyperLogLog
          data structure stored at the variable name
          specified as first argument.
          As a side effect of this command the HyperLogLog
          internals may be updated to reflect a different
          estimation of the number of unique items added
          so far (the cardinality of the set).
          If the approximated cardinality estimated by the
          HyperLogLog changed after executing the command,
          PFADD returns 1, otherwise 0 is returned. The
          command automatically creates an empty HyperLogLog
          structure (that is, a Redis String of a specified
          length and with a given encoding) if the specified
          key does not exist.
          Return valu:
          - 1 if at least 1 HyperLogLog internal register was altered.
          - 0 otherwise.


        :return: None
        """
        key1, key2 = 'key1', 'key2'
        value_tmp = 'TEST_%s'
        values1 = [value_tmp % choice(string.ascii_letters) for _ in range(1, 10 ^ 3)]
        values2 = [value_tmp % choice([1, 2, 3]) for _ in range(1, 10 ^ 3)]
        with await self.rd1 as conn:
            res1 = await conn.pfadd(key1, *values1)
            res2 = await conn.pfadd(key2, *values2)
            res3 = await conn.pfcount(key1)
            res4 = await conn.pfcount(key2)

            await conn.delete(key1, key2)
        frm = "HASH_CMD - 'PFADD': KEYS- {0}, INSERT_HLL - {1}, COUNT_VAL1 - {2}, COUNT_VAL2 - {3}\n"
        logger.debug(frm.format((key1, key2), (res1, res2), res3, res4))


def main():
    # load config from yaml file
    conf = load_config(os.path.join(BASE_DIR, "config_files/dev.yml"))
    # create event loop
    loop = asyncio.get_event_loop()
    # rd1 = await RedisClient.connect(conf=conf['redis'], loop=loop)
    # rd2 = await RedisClient.connect(conf=conf['redis'], loop=loop)
    rd1_conn = loop.run_until_complete(rd_client_factory(loop=loop, conf=conf['redis1']))
    rd2_conn = loop.run_until_complete(rd_client_factory(loop=loop, conf=conf['redis2']))
    rgc = RedisHyperLogLogCommands(rd1_conn.rd, rd2_conn.rd, conf=conf['redis2'])
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
