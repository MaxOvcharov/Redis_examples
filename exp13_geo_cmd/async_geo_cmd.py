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
        await self.rd_geodist_cmd()
        await self.rd_geohash_cmd()
        await self.rd_geopos_cmd()
        await self.rd_georadius_cmd()

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
        frm = "LIST_CMD - 'GEOADD': KEY- {0}, GEO_RES({1})- {2}, GEO_RES({3})- {4}\n"
        logger.debug(frm.format(key1, member1, res1, member2, res2))

    async def rd_geodist_cmd(self):
        """
        Return the distance between two members in the
          geospatial index represented by the sorted set.
          The unit must be one of the following,
          and defaults to meters:
          - m for meters.
          - km for kilometers.
          - mi for miles.
          - ft for feet.
          The distance is computed assuming that the Earth
          is a perfect sphere, so errors up to 0.5% are
          possible in edge cases.

        :return: None
        """
        key1 = 'Sicily'
        long1, lat1, member1 = 13.361389, 38.115556, "Palermo"
        long2, lat2, member2 = 15.087269, 37.502669, "Catania"
        unit = 'km'
        with await self.rd1 as conn:
            await conn.geoadd(key1, long1, lat1, member1)
            await conn.geoadd(key1, long2, lat2, member2)
            res1 = await conn.geodist(key1, member1, member2, unit=unit)
            await conn.delete(key1)
        frm = "LIST_CMD - 'GEODIST': KEY- {0}, GEO_DIST({1} <-> {2})- {3} {4}\n"
        logger.debug(frm.format(key1, member1, member2, res1, unit))

    async def rd_geohash_cmd(self):
        """
        The command returns an array where each element is
          the Geohash corresponding to each member name
          passed as argument to the command.

        :return: None
        """
        key1 = 'Sicily'
        long1, lat1, member1 = 13.361389, 38.115556, "Palermo"
        long2, lat2, member2 = 15.087269, 37.502669, "Catania"
        with await self.rd1 as conn:
            await conn.geoadd(key1, long1, lat1, member1)
            await conn.geoadd(key1, long2, lat2, member2)
            res1 = await conn.geohash(key1, member1, member2)
            await conn.delete(key1)
        frm = "LIST_CMD - 'GEOHASH': KEY- {0}, GEO_HASH({1}) - {2}\n"
        logger.debug(frm.format(key1, (member1, member2), res1))

    async def rd_geopos_cmd(self):
        """
        The command returns an array where each element is a
          two elements array representing longitude and
          latitude (x,y) of each member name passed as argument to the command.
          Non existing elements are reported as NULL elements
          of the array.

        :return: None
        """
        key1 = 'Sicily'
        long1, lat1, member1 = 13.361389, 38.115556, "Palermo"
        long2, lat2, member2 = 15.087269, 37.502669, "Catania"
        with await self.rd1 as conn:
            await conn.geoadd(key1, long1, lat1, member1)
            await conn.geoadd(key1, long2, lat2, member2)
            res1 = await conn.geopos(key1, member1, member2)
            await conn.delete(key1)
        frm = "LIST_CMD - 'GEOPOS': KEY- {0}, GEO_POS1({1}) - {2}, GEO_POS2({3}) - {4}\n"
        logger.debug(frm.format(key1, member1, res1[0], member2, res1[0]))

    async def rd_georadius_cmd(self):
        """
        Return the members of a sorted set populated
          with geospatial information using GEOADD,
          which are within the borders of the area
          specified with the center location and the
          maximum distance from the center (the radius).
          The command optionally returns additional
          information using the following options:
          - WITHDIST: Also return the distance of the
            returned items from the specified center.
            The distance is returned in the same unit as
            the unit specified as the radius argument
            of the command.
          - WITHCOORD: Also return the longitude,latitude
            coordinates of the matching items.
          - WITHHASH: Also return the raw geohash-encoded
            sorted set score of the item, in the form of a
            52 bit unsigned integer. This is only useful for
            low level hacks or debugging and is otherwise of
            little interest for the general user.

        :return: None
        """
        key1 = 'Sicily'
        long1, lat1, member1 = 13.361389, 38.115556, "Palermo"
        long2, lat2, member2 = 15.087269, 37.502669, "Catania"
        longitude, latitude, radius, unit = 15, 37, 200, 'km'
        with await self.rd1 as conn:
            await conn.geoadd(key1, long1, lat1, member1)
            await conn.geoadd(key1, long2, lat2, member2)
            res1 = await conn.georadius(key1, longitude, latitude, radius, unit,
                                        with_dist=True, with_hash=True,
                                        with_coord=True, sort='ASC')
            await conn.delete(key1)
        frm = "LIST_CMD - 'GEORADIUS': KEY- {0}, GEO_RADIUS - {1}\n"
        logger.debug(frm.format(key1, res1))


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
