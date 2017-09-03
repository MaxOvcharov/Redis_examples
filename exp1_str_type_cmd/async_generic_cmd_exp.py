# -*- coding: utf-8 -*-
"""
    Simple example of Redis Generic commands using async lib - aioredis
"""
import asyncio
import os

from redis_client import rd_client_factory
from settings import BASE_DIR, logger
from utils import load_config


class RedisGenericCommands:
    def __init__(self, rd):
        self.rd = rd

    async def run_generic_cmd(self):
        await self.rd_del_cmd()
        await self.rd_dump_cmd()

    async def rd_del_cmd(self):
        """
        Removes the specified keys. A key is ignored if it does not exist.
          Return value:
            The number of keys that were removed.

        :return: None
        """
        key1, key2, key3 = 'key_1', 'key_2', 'key_3'
        value1, value2 = 'TEST1', 'TEST2'
        with await self.rd as conn:
            await conn.mset(key1, value1, key2, value2)
            res = await conn.delete(key1, key2, key3)
        frm = "GENERIC_CMD - 'DELETE': KEY 1,2,3- {0}, DEL_NUM - {1}\n"
        logger.debug(frm.format([key1, key2, key3], res))

    async def rd_dump_cmd(self):
        """
        Serialize the value stored at key in a Redis-specific format and
          return it to the user. The returned value can be synthesized
          back into a Redis key using the RESTORE command.

        :return: None
        """
        key1 = 'key_1'
        value1 = 'TEST1'
        with await self.rd as conn:
            await conn.set(key1, value1)
            res1 = await conn.dump(key1)
            await conn.delete(key1)
            await conn.restore(key1, 0, res1)
            res2 = await conn.get(key1)
            await conn.delete(key1)
        frm = "GENERIC_CMD - 'DUMP': KEY- {0}, SERIALIZE - {1}, DESERIALIZE - {2}\n"
        logger.debug(frm.format(key1, res1, res2))


def main():
    # load config from yaml file
    conf = load_config(os.path.join(BASE_DIR, "config_files/dev.yml"))
    # create event loop
    loop = asyncio.get_event_loop()
    rd_conn = loop.run_until_complete(rd_client_factory(loop=loop, conf=conf['redis']))
    rgc = RedisGenericCommands(rd_conn.rd)
    try:
        loop.run_until_complete(rgc.run_generic_cmd())
    except KeyboardInterrupt as e:
        logger.error("Caught keyboard interrupt {0}\nCanceling tasks...".format(e))
    finally:
        loop.run_until_complete(rd_conn.close_connection())
        loop.close()

if __name__ == '__main__':
    main()
