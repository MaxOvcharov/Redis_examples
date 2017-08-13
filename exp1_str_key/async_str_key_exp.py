# -*- coding: utf-8 -*-
"""
    Simple example of commands with STRING KEY using async lib - aioredis
"""
import asyncio
from redis_client import rd_client_factory

async def rd_set_cmd(rd):
    await rd.set('my-key', 'value')
    val = await rd.get('my-key')
    print(val)


def main():
    loop = asyncio.get_event_loop()
    rd = loop.run_until_complete(rd_client_factory(loop=loop, conf=conf))
    tasks = asyncio.gather(rd_set_cmd(rd))
    loop.run_until_complete(tasks)
    try:
        loop.run_forever()
    except KeyboardInterrupt as e:
        print("Caught keyboard interrupt. Canceling tasks...")
        tasks.cancel()
        loop.run_forever()
        tasks.exception()
    finally:
        loop.close()

if __name__ == '__main__':
    main()
