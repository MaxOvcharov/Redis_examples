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
    await rd.set('my-key', 'value')
    val = await rd.get('my-key')
    logger.debug(val)


def main():
    # load config from yaml file
    conf = load_config(os.path.join(BASE_DIR, "config_files/dev.yml"))
    loop = asyncio.get_event_loop()
    rd_conn = loop.run_until_complete(rd_client_factory(loop=loop, conf=conf['redis']))
    tasks = asyncio.gather(rd_set_cmd(rd_conn.rd))
    loop.run_until_complete(tasks)
    try:
        loop.run_forever()
    except KeyboardInterrupt as e:
        logger.error("Caught keyboard interrupt - {0}.\n Canceling tasks...".format(e))
        tasks.cancel()
        loop.run_forever()
        tasks.exception()
    finally:
        loop.close()

if __name__ == '__main__':
    main()
