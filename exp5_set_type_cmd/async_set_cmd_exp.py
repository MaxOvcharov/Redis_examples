# -*- coding: utf-8 -*-
"""
    Simple example of Redis Set commands using async lib - aioredis
"""
import asyncio
import os

from redis_client import rd_client_factory
from settings import BASE_DIR, logger
from utils import load_config


class RedisSetCommands:
    def __init__(self, rd1, rd2, conf=None):
        self.rd1 = rd1
        self.rd2 = rd2
        self.rd_conf = conf

    async def run_list_cmd(self):
        await self.rd_sadd_cmd()
        await self.rd_scard_cmd()
        await self.rd_sdiff_cmd()
        await self.rd_sdiffstore_cmd()
        await self.rd_sinter_cmd()
        await self.rd_sinterstore_cmd()
        await self.rd_sismember_cmd()
        await self.rd_smembers_cmd()
        await self.rd_smove_cmd()
        await self.rd_spop_cmd()
        await self.rd_srandmember_cmd()
        await self.rd_srem_cmd()

    async def rd_sadd_cmd(self):
        """
        Add the specified members to the set stored at key.
          Specified members that are already a member of
          this set are ignored. If key does not exist, a
          new set is created before adding the specified members.
          An error is returned when the value stored at key is not a set.
          Return value:
          - the number of elements that were added to the set,
          not including all the elements already present into the set.

        :return: None
        """
        key1, key2 = 'key1', 'key2'
        values1, values2 = ('TEST1', 'TEST2', 'TEST1'), 'TEST1'
        with await self.rd1 as conn:
            res1 = await conn.sadd(key1, *values1)
            res2 = await conn.sadd(key2, values2)
            await conn.delete(key1, key2)
        frm = "HASH_CMD - 'SADD': KEYS- {0}, SOME_VALUE - {1}, ONE_VALUE - {2}\n"
        logger.debug(frm.format((key1, key2), res1, res2))

    async def rd_scard_cmd(self):
        """
        Returns the set cardinality (number of elements) of
          the set stored at key.
          Return value:
          - the cardinality (number of elements) of the set
          - 0 if key does not exist.

        :return: None
        """
        key1, key2 = 'key1', 'key2'
        values1, values2 = ('TEST1', 'TEST2', 'TEST3'), 'TEST1'
        with await self.rd1 as conn:
            await conn.sadd(key1, *values1)
            await conn.sadd(key2, values2)
            res1 = await conn.scard(key1)
            res2 = await conn.scard(key2)
            await conn.delete(key1, key2)
        frm = "HASH_CMD - 'SCARD': KEYS- {0}, SET_MANY - {1}, SET_ONE - {2}\n"
        logger.debug(frm.format((key1, key2), res1, res2))

    async def rd_sdiff_cmd(self):
        """
        Returns the members of the set resulting from
          the difference between the first set and all
          the successive sets.
        Return value:
        - list with members of the resulting set.

        :return: None
        """
        key1, key2, key3 = 'key1', 'key2', 'key3'
        values1, values2, values3 = ('TEST1', 'TEST2', 'TEST3'), ('TEST1', 'TEST2'), ('TEST2', )
        diff_key = (key2, key3)
        with await self.rd1 as conn:
            await conn.sadd(key1, *values1)
            await conn.sadd(key2, *values2)
            await conn.sadd(key3, *values3)
            res1 = await conn.sdiff(key1, *diff_key)
            await conn.delete(key1, key2, key3)
        frm = "HASH_CMD - 'SDIFF': KEYS- {0}, DIFF_SET_VALUES - {1}\n"
        logger.debug(frm.format((key1, key2, key3), res1))

    async def rd_sdiffstore_cmd(self):
        """
        This command is equal to SDIFF, but instead of
          returning the resulting set, it is stored in destination.
          If destination already exists, it is overwritten.
          Return value:
          - the number of elements in the resulting set.

        :return: None
        """
        key1, key2, key3, dest_key = 'key1', 'key2', 'key3', 'key4'
        values1, values2, values3 = ('TEST1', 'TEST2', 'TEST3'), ('TEST1', 'TEST2'), ('TEST2', )
        diff_key = (key2, key3)
        with await self.rd1 as conn:
            await conn.sadd(key1, *values1)
            await conn.sadd(key2, *values2)
            await conn.sadd(key3, *values3)
            await conn.sdiffstore(dest_key, key1, *diff_key)
            res1 = await conn.smembers(dest_key)
            await conn.delete(key1, key2, key3, dest_key)
        frm = "HASH_CMD - 'SDIFFSTORE': KEYS- {0}, DIFFSTORE_VALUES - {1}\n"
        logger.debug(frm.format((key1, key2, key3), res1))

    async def rd_sinter_cmd(self):
        """
        Returns the members of the set resulting from
          the intersection of all the given sets.
          Keys that do not exist are considered to be
          empty sets. With one of the keys being an
          empty set, the resulting set is also empty
          (since set intersection with an empty set
          always results in an empty set).
          Return value:
          - list with members of the resulting set.

        :return: None
        """
        key1, key2, key3, dest_key = 'key1', 'key2', 'key3', 'key4'
        values1, values2, values3 = ('TEST1', 'TEST2', 'TEST3'), ('TEST1', 'TEST2'), ('TEST2', )
        diff_key = (key2, key3)
        with await self.rd1 as conn:
            await conn.sadd(key1, *values1)
            await conn.sadd(key2, *values2)
            await conn.sadd(key3, *values3)
            res1 = await conn.sinter(key1, *diff_key)
            await conn.delete(key1, key2, key3, dest_key)
        frm = "HASH_CMD - 'SINTER': KEYS- {0}, INTERSECTION_VALUES - {1}\n"
        logger.debug(frm.format((key1, key2, key3), res1))

    async def rd_sinterstore_cmd(self):
        """
        This command is equal to SINTER, but instead of returning
          the resulting set, it is stored in destination.
          If destination already exists, it is overwritten.
          Return value:
          - the number of elements in the resulting set.

        :return: None
        """
        key1, key2, key3, dest_key = 'key1', 'key2', 'key3', 'key4'
        values1, values2, values3 = ('TEST1', 'TEST2', 'TEST3'), ('TEST1', 'TEST2'), ('TEST2', )
        diff_key = (key2, key3)
        with await self.rd1 as conn:
            await conn.sadd(key1, *values1)
            await conn.sadd(key2, *values2)
            await conn.sadd(key3, *values3)
            await conn.sinterstore(dest_key, key1, *diff_key)
            res1 = await conn.smembers(dest_key)
            await conn.delete(key1, key2, key3, dest_key)
        frm = "HASH_CMD - 'SINTERSTORE': KEYS- {0}, INTERSECTION_VALUES - {1}\n"
        logger.debug(frm.format((key1, key2, key3), res1))

    async def rd_sismember_cmd(self):
        """
        Returns if member is a member of the set stored at key.
          Return value:
          - 1 if the element is a member of the set.
          - 0 if the element is not a member of the set, or if key
            does not exist.

        :return: None
        """
        key1, key2 = 'key1', 'key2'
        values1, values2 = ('TEST1', 'TEST2', 'TEST3'), ('TEST1', 'TEST3')
        set_member = 'TEST2'
        with await self.rd1 as conn:
            await conn.sadd(key1, *values1)
            await conn.sadd(key2, *values2)
            res1 = await conn.sismember(key1, set_member)
            res2 = await conn.sismember(key2, set_member)
            await conn.delete(key1, key2)
        frm = "HASH_CMD - 'SISMEMBER': KEYS- {0}, EXIST_MEMBER - {1}, NOT_EXIST_MEMBER - {2}\n"
        logger.debug(frm.format((key1, key2), res1, res2))

    async def rd_smembers_cmd(self):
        """
        Returns all the members of the set value stored at key.
          This has the same effect as running SINTER with one argument key.
          Return value:
          - all elements of the set.

        :return: None
        """
        key1 = 'key1'
        values1 = ['TEST1', 'TEST2', 'TEST3']
        with await self.rd1 as conn:
            await conn.sadd(key1, *values1)
            res1 = await conn.smembers(key1)
            await conn.delete(key1)
        frm = "HASH_CMD - 'SMEMBERS': KEY- {0}, INTPUT - {1}, RES - {2}\n"
        logger.debug(frm.format(key1, values1, res1))

    async def rd_smove_cmd(self):
        """
        Move member from the set at source to the set at
          destination. This operation is atomic. In every
          given moment the element will appear to be a
          member of source or destination for other clients.
          If the source set does not exist or does not contain
          the specified element, no operation is performed and
          0 is returned.
          Otherwise, the element is removed from the source set
          and added to the destination set. When the specified
          element already exists in the destination set, it is
          only removed from the source set.
          Return value:
          - 1 if the element is moved.
          - 0 if the element is not a member of source and
          no operation was performed.

        :return: None
        """
        key1, des_key1, des_key2 = 'key1', 'des_key1', 'des_key2'
        values1 = ['TEST1', 'TEST2', 'TEST3']
        moved_val1, moved_val2 = b'test1', b'TEST1'
        with await self.rd1 as conn:
            await conn.sadd(key1, *values1)
            res1 = await conn.smove(key1, des_key1, moved_val1)
            res2 = await conn.smove(key1, des_key2, moved_val2)
            await conn.delete(key1, des_key1, des_key2)
        frm = "HASH_CMD - 'SMOVE': KEYS- {0}, MOVE_OK - {1}, MOVE_FAIL - {2}\n"
        logger.debug(frm.format((key1, des_key1, des_key2), res1, res2))

    async def rd_spop_cmd(self):
        """
        Removes and returns one or more random elements
          from the set value store at key.
          This operation is similar to SRANDMEMBER, that
          returns one or more random elements from a set
          but does not remove it.
          The count argument is available since version 3.2.
          Return value:
          - the removed element
          - nil when key does not exist.

        :return: None
        """
        key1 = 'key1'
        values1 = ['TEST1', 'TEST2', 'TEST3']
        with await self.rd1 as conn:
            await conn.sadd(key1, *values1)
            res1 = await conn.spop(key1)
            res2 = await conn.spop(key1)
            await conn.delete(key1)
        frm = "HASH_CMD - 'SPOP': KEY- {0}, RANDOM_VAL1 - {1}, RANDOM_VAL2 - {2}\n"
        logger.debug(frm.format(key1, res1, res2))

    async def rd_srandmember_cmd(self):
        """
        When called with just the key argument, return a
          random element from the set value stored at key.
          Return value:
          - without the additional count argument the
            command returns a Bulk Reply with the randomly
            selected element, or nil when key does not exist.
          - when the additional count argument is passed the
            command returns an array of elements, or an empty
            array when key does not exist.

        :return: None
        """
        key1 = 'key1'
        values1 = ['TEST1', 'TEST2', 'TEST3', 'TEST4']
        with await self.rd1 as conn:
            await conn.sadd(key1, *values1)
            res1 = await conn.srandmember(key1)
            res2 = await conn.srandmember(key1, 2)
            await conn.delete(key1)
        frm = "HASH_CMD - 'SRANDMEMBER': KEY- {0}, RANDOM_ONE - {1}, RANDOM_ARRAY - {2}\n"
        logger.debug(frm.format(key1, res1, res2))

    async def rd_srem_cmd(self):
        """
        Remove the specified members from the set stored at key.
          Specified members that are not a member of this set
          are ignored. If key does not exist, it is treated as
          an empty set and this command returns 0.
          An error is returned when the value stored at
          key is not a set.
          Return value:
          - the number of members that were removed from the set,
            not including non existing members.

        :return: None
        """
        key1 = 'key1'
        values1 = ['TEST1', 'TEST2', 'TEST3', 'TEST4']
        with await self.rd1 as conn:
            await conn.sadd(key1, *values1)
            res1 = await conn.srem(key1, *values1[0:2])
            res2 = await conn.srem(key1, 'TEST5')
            res3 = await conn.smembers(key1)
            await conn.delete(key1)
        frm = "HASH_CMD - 'SREM': KEY- {0}, REMOVE_OK - {1}, REMOVE_FAIL - {2}, RES - {3}\n"
        logger.debug(frm.format(key1, res1, res2, res3))


def main():
    # load config from yaml file
    conf = load_config(os.path.join(BASE_DIR, "config_files/dev.yml"))
    # create event loop
    loop = asyncio.get_event_loop()
    # rd1 = await RedisClient.connect(conf=conf['redis'], loop=loop)
    # rd2 = await RedisClient.connect(conf=conf['redis'], loop=loop)
    rd1_conn = loop.run_until_complete(rd_client_factory(loop=loop, conf=conf['redis1']))
    rd2_conn = loop.run_until_complete(rd_client_factory(loop=loop, conf=conf['redis2']))
    rgc = RedisSetCommands(rd1_conn.rd, rd2_conn.rd, conf=conf['redis2'])
    try:
        loop.run_until_complete(rgc.run_list_cmd())
    except KeyboardInterrupt as e:
        logger.error("Caught keyboard interrupt {0}\nCanceling tasks...".format(e))
    finally:
        loop.run_until_complete(rd1_conn.close_connection())
        loop.run_until_complete(rd2_conn.close_connection())
        loop.close()

if __name__ == '__main__':
    main()
