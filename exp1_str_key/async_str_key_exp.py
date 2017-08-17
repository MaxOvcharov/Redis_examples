# -*- coding: utf-8 -*-
"""
    Simple example of commands with STRING KEY using async lib - aioredis
"""
import asyncio
import os

from redis_client import rd_client_factory
from settings import BASE_DIR, logger
from utils import load_config


class RedisStrCommands:
    def __init__(self, rd):
        self.rd = rd

    async def run_rd_str_commands(self):
        await self.rd_set_cmd()
        await self.rd_append_cmd()

    async def rd_set_cmd(self):
        """
        Set key to hold the string value. If key already holds a value, it is
        overwritten, regardless of its type. Any previous time to live associated
        with the key is discarded on successful SET operation.

        :return: None
        """
        key = 'str_set_cmd'
        value = 'test_str_set_cmd'
        with await self.rd as conn:
            await conn.set(key, value)
            res = await conn.get(key)
            conn.delete(key)
        frm = "STR_CMD - 'SET -> GET': KEY - {0}, VALUE - {1}\n"
        logger.debug(frm.format(key, res))

    async def rd_append_cmd(self):
        """
        If key already exists and is a string, this command appends the value at
          the end of the string. If key does not exist it is created and set as
          an empty string, so APPEND will be similar to SET in this special case.

        :return: None
        """
        key = 'str_append_cmd'
        value1 = 'test_str_append(new)_cmd___'
        value2 = 'test_str_append_cmd'
        with await self.rd as conn:
            await conn.append(key, value1)
            await conn.append(key, value2)
            res = await conn.get(key)
            conn.delete(key)
        frm = "STR_CMD - 'APPEND(NEW) -> APPEND -> GET': KEY - {0}, VALUE - {1}\n"
        logger.debug(frm.format(key, res))


def main():
    # load config from yaml file
    conf = load_config(os.path.join(BASE_DIR, "config_files/dev.yml"))
    # create event loop
    loop = asyncio.get_event_loop()
    rd_conn = loop.run_until_complete(rd_client_factory(loop=loop, conf=conf['redis']))
    rsc = RedisStrCommands(rd_conn.rd)
    try:
        loop.run_until_complete(rsc.run_rd_str_commands())
    except KeyboardInterrupt as e:
        logger.error(f"Caught keyboard interrupt {0}\nCanceling tasks...".format(e))
    finally:
        loop.run_until_complete(rd_conn.close_connection())
        loop.close()

if __name__ == '__main__':
    main()
