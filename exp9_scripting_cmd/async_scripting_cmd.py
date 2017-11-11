# -*- coding: utf-8 -*-
"""
    Simple example of Redis Scripting commands using async lib - aioredis
    For commands details see: http://redis.io/commands#scripting
"""
import asyncio
import os

from itertools import chain
from random import choice, randint

from redis_client import rd_client_factory
from settings import BASE_DIR, logger
from utils import load_config


class RedisScriptingCommands:
    def __init__(self, rd1, rd2, conf=None):
        self.rd1 = rd1
        self.rd2 = rd2
        self.rd_conf = conf

    async def run_scripting_cmd(self):
        await self.rd_eval_cmd()

    async def rd_eval_cmd(self):
        """
        EVAL and EVALSHA are used to evaluate scripts
          using the Lua interpreter built into Redis
          starting from version 2.6.0.
          - The first argument of EVAL is a Lua 5.1 script.
          The script does not need to define a Lua function
          (and should not). It is just a Lua program that
          will run in the context of the Redis server.
          - The second argument of EVAL is the number of
          arguments that follows the script (starting from
          the third argument) that represent Redis key names.
          The arguments can be accessed by Lua using the KEYS
          global variable in the form of a one-based array
          (so KEYS[1], KEYS[2], ...).
          - All the additional arguments should not represent
          key names and can be accessed by Lua using the ARGV
          global variable, very similarly to what happens with
          keys (so ARGV[1], ARGV[2], ...).

        :return: None
        """
        script_cmd = "return {1,2,{3,'Hello World!'}}"
        with await self.rd1 as conn:
            res1 = await conn.eval(script_cmd, args=[0])
        frm = "SORTED_SCRIPTING_CMD - 'EVAL': SCRIPT_RES_VALUE - {0}\n"
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
    rgc = RedisScriptingCommands(rd1_conn.rd, rd2_conn.rd, conf=conf['redis2'])
    try:
        loop.run_until_complete(rgc.run_scripting_cmd())
    except KeyboardInterrupt as e:
        logger.error("Caught keyboard interrupt {0}\nCanceling tasks...".format(e))
    finally:
        loop.run_until_complete(rd1_conn.close_connection())
        loop.run_until_complete(rd2_conn.close_connection())
        loop.close()


if __name__ == '__main__':
    main()
