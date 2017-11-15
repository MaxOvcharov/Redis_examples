# -*- coding: utf-8 -*-
"""
    Simple example of Redis PubSub commands using async lib - aioredis
    For commands details see: http://redis.io/commands/#pubsub
"""
import asyncio
import os
import time

from redis_client import rd_client_factory
from settings import BASE_DIR, logger
from utils import load_config


class RedisSubWorker:
    def __init__(self, rd, channel, conf=None):
        self.rd = rd
        self.rd_conf = conf
        self.channel = channel

    @classmethod
    async def connect(cls, *args, **kwargs):
        """
        This method creates Redis client.

        :return obj self: RedisClient instance
        """
        self = cls(*args, **kwargs)
        await self._init_connect()
        return self

    async def _init_connect(self):
        """

        :return:
        """
        with await self.rd as conn:
            ch = await conn.subscribe(self.channel)
        print(ch)
        while await ch[0].wait_message():
            msg = await ch[0].get(encoding='utf-8')
            frm = "PUBSUB_CMD - SUB_RESULT - {0}\n"
            logger.debug(frm.format(msg))
            msg = await ch[0].get(encoding='utf-8')
            print(msg)

class RedisPubSubCommands:
    def __init__(self, rd1, rd2, conf=None):
        self.rd1 = rd1
        self.rd2 = rd2
        self.rd_conf = conf

    async def run_pubsub_cmd(self):
        await self.pubsub_publish_cmd()
        await self.pubsub2_publish_cmd()

    async def pubsub_publish_cmd(self):
        """
        Posts a message to the given channel.
          Return value:
          - the number of clients that received the message.

        :return: None
        """
        msg, channel = "Hello World!", 'TEST'
        with await self.rd2 as conn2:
            res1 = await conn2.publish(channel, msg)
        frm = "PUBSUB_CMD - 'PUBLISH':PUB_RES - {0}\n"
        logger.debug(frm.format(res1))

    async def pubsub2_publish_cmd(self):
        """
        Posts a message to the given channel.
          Return value:
          - the number of clients that received the message.

        :return: None
        """
        msg, channel = "Hello World!", 'TEST'
        with await self.rd2 as conn2:
            res1 = await conn2.publish(channel, msg)
        frm = "PUBSUB_CMD - 'PUBLISH':PUB_RES - {0}\n"
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
    rgc = RedisPubSubCommands(rd1_conn.rd, rd2_conn.rd, conf=conf['redis2'])
    tsk1 = asyncio.ensure_future(RedisSubWorker.connect(rd1_conn.rd, 'TEST', conf=conf['redis1']))
    try:
        loop.run_until_complete(rgc.run_pubsub_cmd())
        loop.run_until_complete(asyncio.gather(tsk1))
        # loop.run_in_executor(None, asyncio.gather(tsk1))
    except KeyboardInterrupt as e:
        logger.error("Caught keyboard interrupt {0}\nCanceling tasks...".format(e))
    finally:
        loop.run_until_complete(rd1_conn.close_connection())
        loop.run_until_complete(rd2_conn.close_connection())
        loop.close()


if __name__ == '__main__':
    main()
