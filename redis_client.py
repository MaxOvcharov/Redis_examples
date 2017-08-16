import aioredis
from settings import logger


class RedisClient:
    """ Redis connection pool client """

    def __init__(self, loop, conf):
        self.loop = loop
        self.conf = conf
        self.rd = None

    @classmethod
    async def connect(cls, **options):
        self = cls(**options)
        await self.init_connection()
        return self

    async def init_connection(self):
        self.rd = await aioredis.create_pool(
            (self.conf['host'], self.conf['port']),
            db=self.conf['db'],
            password=self.conf['password'],
            maxsize=self.conf['maxsize'],
            minsize=self.conf['minsize'],
            loop=self.loop
        )

    async def close_connection(self):
        self.rd.close()
        await self.rd.wait_closed()
        logger.debug('Redis closing pool connection...')

async def rd_client_factory(loop, conf, client=RedisClient):
    """ Abstract Redis client factory """
    rd = await client.connect(loop=loop, conf=conf)
    return rd
