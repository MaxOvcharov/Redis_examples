# -*- coding: utf-8 -*-
"""
    Simple example of Redis Geo commands using async lib - aioredis
    For commands details see: http://redis.io/commands#geo
"""
import asyncio
import os

from redis_client import rd_client_factory
from settings import BASE_DIR, logger
from utils import load_config


class RedisGeoCommands:
    def __init__(self, rd1, rd2, conf=None):
        self.rd1 = rd1
        self.rd2 = rd2
        self.rd_conf = conf

    async def run_geo_cmd(self):
        await self.rd_geoadd_cmd()

    async def rd_geoadd_cmd(self):
        """
        Adds the specified geospatial items
          (latitude, longitude, name) to the specified key.
          Data is stored into the key as a sorted set, in
          a way that makes it possible to later retrieve
          items using a query by radius with the
          GEORADIUS or GEORADIUSBYMEMBER commands.

        :return: None
        """
        key1 = 'Sicily'
        long1, lat1, member1 = 13.361389, 38.115556, "Palermo"
        long2, lat2, member2 = 15.087269, 37.502669, "Catania"
        with await self.rd1 as conn:
            res1 = await conn.geoadd(key1, long1, lat1, member1)
            res2 = await conn.geoadd(key1, long2, lat2, member2)
            await conn.delete(key1)
        frm = "LIST_CMD - 'GEOADD': KEY- {0}, GEO_RES({1})- {2}, GEO_RES({3}) - {4}\n"
        logger.debug(frm.format(key1, member1, res1, member2, res2))


def main():
    # load config from yaml file
    conf = load_config(os.path.join(BASE_DIR, "config_files/dev.yml"))
    # create event loop
    loop = asyncio.get_event_loop()
    # rd1 = await RedisClient.connect(conf=conf['redis'], loop=loop)
    # rd2 = await RedisClient.connect(conf=conf['redis'], loop=loop)
    rd1_conn = loop.run_until_complete(rd_client_factory(loop=loop, conf=conf['redis1']))
    rd2_conn = loop.run_until_complete(rd_client_factory(loop=loop, conf=conf['redis2']))
    rgc = RedisGeoCommands(rd1_conn.rd, rd2_conn.rd, conf=conf['redis2'])
    try:
        loop.run_until_complete(rgc.run_geo_cmd())
    except KeyboardInterrupt as e:
        logger.error("Caught keyboard interrupt {0}\nCanceling tasks...".format(e))
    finally:
        loop.run_until_complete(rd1_conn.close_connection())
        loop.run_until_complete(rd2_conn.close_connection())
        loop.close()


if __name__ == '__main__':
    main()