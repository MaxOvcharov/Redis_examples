# -*- coding: utf-8 -*-
"""
    Simple example of commands with STRING KEY using async lib - aioredis
"""
import asyncio
import os

from redis_client import rd_client_factory
from settings import BASE_DIR, logger
from utils import load_config


async def rd_set_cmd(rd):
    with await rd as conn:
        await conn.set('str_set_cmd', 'test_str_set_cmd')
        val = await conn.get('str_set_cmd')
    logger.debug("STR_CMD - 'SET': {0}".format(val))


def main():
    # load config from yaml file
    conf = load_config(os.path.join(BASE_DIR, "config_files/dev.yml"))
    # create event loop
    loop = asyncio.get_event_loop()
    rd_conn = loop.run_until_complete(rd_client_factory(loop=loop, conf=conf['redis']))
    try:
        loop.run_until_complete(rd_set_cmd(rd_conn.rd))
    except KeyboardInterrupt as e:
        logger.error(f"Caught keyboard interrupt {0}\nCanceling tasks...".format(e))
    finally:
        loop.run_until_complete(rd_conn.close_connection())
        loop.close()

if __name__ == '__main__':
    main()
