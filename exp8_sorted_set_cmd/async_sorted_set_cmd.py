# -*- coding: utf-8 -*-
"""
    Simple example of Redis SortedSet commands using async lib - aioredis
    For commands details see: http://redis.io/commands/#sorted_set
"""
import asyncio
import os

from itertools import chain
from random import choice, randint

from redis_client import rd_client_factory
from settings import BASE_DIR, logger
from utils import load_config


class RedisSortedSetCommands:
    def __init__(self, rd1, rd2, conf=None):
        self.rd1 = rd1
        self.rd2 = rd2
        self.rd_conf = conf

    async def run_sorted_set_cmd(self):
        await self.rd_zadd_cmd()
        await self.rd_zcard_cmd()
        await self.rd_zcount_cmd()
        await self.rd_zincrby_cmd()
        await self.rd_zinterstore_cmd()
        await self.rd_zlexcount_cmd()
        await self.rd_zrange_cmd()
        await self.rd_zrangebylex_cmd()
        await self.rd_zrangebyscore_cmd()
        await self.rd_zrank_cmd()
        await self.rd_zrem_cmd()
        await self.rd_zremrangebylex_cmd()
        await self.rd_zremrangebyrank_cmd()
        await self.rd_zremrangebyscore_cmd()
        await self.rd_zrevrange_cmd()
        await self.rd_zrevrangebyscore_cmd()
        await self.rd_zrevrangebylex_cmd()
        await self.rd_zrevrank_cmd()
        await self.rd_zscore_cmd()
        await self.rd_zunionstore_cmd()
        await self.rd_zscan_cmd()
        await self.rd_izscan_cmd()

    async def rd_zadd_cmd(self):
        """
        Adds all the specified members with the specified
          scores to the sorted set stored at key. It is
          possible to specify multiple score / member pairs.
          If a specified member is already a member of the
          sorted set, the score is updated and the element
          reinserted at the right position to ensure the
          correct ordering.
          Return value:
          - The number of elements added to the sorted sets,
            not including elements already existing for which
            the score was updated.
          - If the INCR option is specified, the return value
            will be Bulk string reply: the new score of member
            (a double precision floating point number),
            represented as string.

        :return: None
        """
        key1 = 'key1'
        values = ('TEST1', 'TEST1', 'TEST2', 'TEST3')
        scores = (1, 2, 2, 1)
        pairs = list(chain(*zip(scores, values)))
        with await self.rd1 as conn:
            await conn.zadd(key1, *pairs)
            res1 = await conn.zrange(key1, 0, -1, withscores=True)
            await conn.delete(key1)
        frm = "SORTED_SET_CMD - 'ZADD': KEY- {0}, RES_VALUE - {1}\n"
        logger.debug(frm.format(key1, res1))

    async def rd_zcard_cmd(self):
        """
        Returns the sorted set cardinality (number of
          elements) of the sorted set stored at key.
          Return value:
          - the cardinality (number of elements) of the
            sorted set, or 0 if key does not exist.

        :return: None
        """
        key1, key2 = 'key1', 'key2'
        values = ('TEST1', 'TEST1', 'TEST2', 'TEST3')
        scores = (1, 2, 2, 1)
        pairs = list(chain(*zip(scores, values)))
        with await self.rd1 as conn:
            await conn.zadd(key1, *pairs)
            res1 = await conn.zcard(key1)
            res2 = await conn.zcard(key2)
            await conn.delete(key1)
        frm = "SORTED_SET_CMD - 'ZCARD': KEY- {0}, ZCARD_EXIST_SET - {1}," \
              " ZCARD_NOT_EXIST_SET - {2}\n"
        logger.debug(frm.format(key1, res1, res2))

    async def rd_zcount_cmd(self):
        """
        Returns the number of elements in the sorted
          set at key with a score between min and max.
          The command has a complexity of just O(log(N))
          because it uses elements ranks (see ZRANK) to
          get an idea of the range. Because of this there
          is no need to do a work proportional to the size
          of the range.

          Return value:
          - Integer reply: the number of elements in the
            specified score range.
        :return: None
        """
        key1, key2 = 'key1', 'key2'
        values = ('TEST1', 'TEST2', 'TEST3', 'TEST4', 'TEST5')
        scores = (1, 2, 2, 1, 2)
        pairs = list(chain(*zip(scores, values)))
        with await self.rd1 as conn:
            await conn.zadd(key1, *pairs)
            res1 = await conn.zcount(key1, 2, 2)
            res2 = await conn.zcount(key2)
            await conn.delete(key1)
        frm = "SORTED_SET_CMD - 'ZCOUNT': KEY- {0}, ZCOUNT_EXIST_SET - {1}," \
              " ZCOUNT_NOT_EXIST_SET - {2}\n"
        logger.debug(frm.format(key1, res1, res2))

    async def rd_zincrby_cmd(self):
        """
        Increments the score of member in the
          sorted set stored at key by increment.
          If member does not exist in the sorted set,
          it is added with increment as its score
          (as if its previous score was 0.0). If key
          does not exist, a new sorted set with the
          specified member as its sole member is created.
          An error is returned when key exists but
          does not hold a sorted set.
          The score value should be the string
          representation of a numeric value, and accepts
          double precision floating point numbers. It is
          possible to provide a negative value to
          decrement the score.
          Return value:
          - the new score of member (a double precision
            floating point number), represented as string.

        :return: None
        """
        key1, key2 = 'key1', 'key2'
        values = ('TEST1', 'TEST2')
        scores = (1, 2)
        pairs = list(chain(*zip(scores, values)))
        with await self.rd1 as conn:
            await conn.zadd(key1, *pairs)
            res1 = await conn.zincrby(key1, 2, values[0])
            res2 = await conn.zincrby(key1, -1, values[1])
            res3 = await conn.zincrby(key1, 10, "TEST4")
            res4 = await conn.zrange(key1, 0, -1, withscores=True)
            await conn.delete(key1)
        frm = "SORTED_SET_CMD - 'ZINCRBY': KEY- {0}, INCR - {1}, " \
              "DECR - {2}, NOT_EXIST - {3}, RES_SET - {4}\n"
        logger.debug(frm.format(key1, res1, res2, res3, res4))

    async def rd_zinterstore_cmd(self):
        """
        Computes the intersection of numkeys sorted
          sets given by the specified keys, and stores
          the result in destination. It is mandatory
          to provide the number of input keys (numkeys)
          before passing the input keys and the other
          (optional) arguments.
          By default, the resulting score of an element
          is the sum of its scores in the sorted sets
          where it exists. Because intersection requires
          an element to be a member of every given sorted
          set, this results in the score of every element
          in the resulting sorted set to be equal to the
          number of input sorted sets.
          For a description of the WEIGHTS and AGGREGATE
          options, see ZUNIONSTORE. If destination already
          exists, it is overwritten.
          Return value:
          - Integer reply: the number of elements in
            the resulting sorted set at destination.

        :return: None
        """
        key1, key2, key3 = 'key1', 'key2', 'key3'
        values1, values2 = ('TEST1', 'TEST2'), ('TEST1', 'TEST2', 'TEST3')
        scores1, scores2 = (1, 2), (1, 2, 3)
        pairs1 = list(chain(*zip(scores1, values1)))
        pairs2 = list(chain(*zip(scores2, values2)))
        with await self.rd1 as conn:
            await conn.zadd(key1, *pairs1)
            await conn.zadd(key2, *pairs2)
            res1 = await conn.zinterstore(key3, (key1, 2), (key2, 3),
                                          with_weights=True, aggregate='ZSET_AGGREGATE_SUM')
            res2 = await conn.zrange(key3, 0, -1, withscores=True)
            await conn.delete(key1, key2, key3)
        frm = "SORTED_SET_CMD - 'ZINTERSTORE': KEYS- {0}, " \
              "RES_INTERSTORE - {1}, DEST_KEY_VAL - {2}\n"
        logger.debug(frm.format((key1, key2, key3), res1, res2))

    async def rd_zlexcount_cmd(self):
        """
        When all the elements in a sorted set are inserted
          with the same score, in order to force lexicographical
          ordering, this command returns the number of elements
          in the sorted set at key with a value between min and max.
          The min and max arguments have the same meaning as
          described for ZRANGEBYLEX.
          Return value:
          - the number of elements in the specified score range.

        :return: None
        """
        key1 = 'key1'
        values1 = ('TEST1', 'TEST2', 'TEST3', 'TEST4', 'TEST5')
        scores1 = (1, 1, 1, 1, 1)
        pairs1 = list(chain(*zip(scores1, values1)))
        with await self.rd1 as conn:
            await conn.zadd(key1, *pairs1)
            res1 = await conn.zlexcount(key1, min=b'-', max=b'+')
            res2 = await conn.zlexcount(key1, min=b'TEST3', max=b'TEST5',
                                        include_min=True, include_max=True)
            await conn.delete(key1)
        frm = "SORTED_SET_CMD - 'ZLENCOUNT': KEY- {0}, " \
              "RES_ALL - {1}, RES_INCLUDE - {2}\n"
        logger.debug(frm.format(key1, res1, res2))

    async def rd_zrange_cmd(self):
        """
        Returns the specified range of elements in the
          sorted set stored at key. The elements are
          considered to be ordered from the lowest to
          the highest score. Lexicographical order is
          used for elements with equal score.
          Return value:
          - list of elements in the specified range
            (optionally with their scores, in case
            the WITHSCORES option is given).

        :return: None
        """
        key1, key2 = 'key1', 'key2'
        values = ('TEST1', 'TEST1', 'TEST2', 'TEST3')
        scores = (1, 2, 2, 1)
        pairs = list(chain(*zip(scores, values)))
        with await self.rd1 as conn:
            await conn.zadd(key1, *pairs)
            res1 = await conn.zrange(key1, 0, -1, withscores=True)
            res2 = await conn.zrange(key2, 0, -1, withscores=True)
            await conn.delete(key1)
        frm = "SORTED_SET_CMD - 'ZRANGE': KEY- {0}, RES_EXIST_LEN - {1}, RES_NOT_EXIST_LEN - {2}\n"
        logger.debug(frm.format(key1, res1, res2))

    async def rd_zrangebylex_cmd(self):
        """
       When all the elements in a sorted set are
         inserted with the same score, in order to
         force lexicographical ordering, this command
         returns all the elements in the sorted set
         at key with a value between min and max.
         If the elements in the sorted set have different
         scores, the returned elements are unspecified.
          Return value:
          - list of elements in the specified score range.

        :return: None
        """
        key1, key2 = 'key1', 'key2'
        values = ('TEST1', 'TEST2', 'TEST3', 'TEST4', 'TEST5', 'TEST6')
        scores = (1, 1, 1, 1, 1, 1)
        pairs = list(chain(*zip(scores, values)))
        with await self.rd1 as conn:
            await conn.zadd(key1, *pairs)
            res1 = await conn.zrangebylex(key1, b'TEST2', b'TEST5', include_min=True, include_max=True)
            res2 = await conn.zrangebylex(key1, b'TEST2', b'TEST5', include_min=False, include_max=False)
            await conn.delete(key1)
        frm = "SORTED_SET_CMD - 'ZRANGEBYLEX': KEY- {0}, " \
              "RES_INCLUDE - {1}, RES_NOT_INCLUDE - {2}\n"
        logger.debug(frm.format(key1, res1, res2))

    async def rd_zrangebyscore_cmd(self):
        """
        Returns all the elements in the sorted set at key
          with a score between min and max (including
          elements with score equal to min or max).
          The elements are considered to be ordered from
          low to high scores.
          Return value:
          - list of elements in the specified score range
          (optionally with their scores).

        :return: None
        """
        key1, key2 = 'key1', 'key2'
        values = ('TEST1', 'TEST2', 'TEST3', 'TEST4', 'TEST5', 'TEST6')
        scores = (1.3, 1.2, 1.1, 1.5, 2.0, 2.5)
        pairs = list(chain(*zip(scores, values)))
        with await self.rd1 as conn:
            await conn.zadd(key1, *pairs)
            res1 = await conn.zrangebyscore(key1, min=1.0, max=1.1, withscores=True)
            res2 = await conn.zrangebyscore(key1, min=1.0, max=2.0, withscores=True, offset=1, count=2)
            await conn.delete(key1)
        frm = "SORTED_SET_CMD - 'ZRANGEBYSCORE': KEY- {0}, " \
              "RES1 - {1}, RES_NOT_OFFSET - {2}\n"
        logger.debug(frm.format(key1, res1, res2))

    async def rd_zrank_cmd(self):
        """
        Returns the rank of member in the sorted set
          stored at key, with the scores ordered from
          low to high. The rank (or index) is 0-based,
          which means that the member with the lowest
          score has rank 0.
          Return value:
          - If member exists in the sorted set, Integer
            reply: the rank of member.
          - If member does not exist in the sorted set
            or key does not exist, Bulk string reply: nil.

        :return: None
        """
        key1, key2 = 'key1', 'key2'
        values = ('TEST1', 'TEST2', 'TEST3', 'TEST4', 'TEST5', 'TEST6')
        scores = (1.3, 1.2, 1.1, 1.5, 2.0, 2.5)
        pairs = list(chain(*zip(scores, values)))
        with await self.rd1 as conn:
            await conn.zadd(key1, *pairs)
            res1 = await conn.zrank(key1, values[-1])
            res2 = await conn.zrank(key2, values[-1])
            await conn.delete(key1)
        frm = "SORTED_SET_CMD - 'ZRANK': KEYS- {0}, " \
              "RES_EXIST - {1}, RES_NOT_EXIST - {2}\n"
        logger.debug(frm.format((key1, key2), res1, res2))

    async def rd_zrem_cmd(self):
        """
        Removes the specified members from the sorted
          set stored at key. Non existing members are ignored.
          An error is returned when key exists and does not
          hold a sorted set.
          Return value:
          - The number of members removed from the sorted set,
            not including non existing members.

        :return: None
        """
        key1 = 'key1'
        values = ('TEST1', 'TEST2', 'TEST3', 'TEST4', 'TEST5', 'TEST6')
        scores = (1.3, 1.2, 1.1, 1.5, 2.0, 2.5)
        pairs = list(chain(*zip(scores, values)))
        with await self.rd1 as conn:
            await conn.zadd(key1, *pairs)
            res1 = await conn.zrem(key1, *values[:4])
            res2 = await conn.zrange(key1, 0, -1, withscores=True)
            res3 = await conn.zrem(key1, 'TEST7')
            res4 = await conn.zrange(key1, 0, -1, withscores=True)
            await conn.delete(key1)
        frm = "SORTED_SET_CMD - 'ZREM': KEY- {0}, " \
              "REM_OK({1}) - {2}, RES_SKIP({3}) - {4}\n"
        logger.debug(frm.format(key1, res1, res2, res3, res4))

    async def rd_zremrangebylex_cmd(self):
        """
        Removes the specified members from the sorted
          set stored at key. Non existing members are ignored.
          An error is returned when key exists and does not
          hold a sorted set.
          Return value:
          - The number of members removed from the sorted set,
            not including non existing members.

        :return: None
        """
        key1 = 'key1'
        values = ('a', 'b', 'c', 'd', 'f')
        scores = (0, 0, 0, 0, 0)
        pairs = list(chain(*zip(scores, values)))
        with await self.rd1 as conn:
            await conn.zadd(key1, *pairs)
            res1 = await conn.zrange(key1, 0, -1)
            res2 = await conn.zremrangebylex(key1, min=b'a', max=b'd',
                                             include_min=True, include_max=True)
            res3 = await conn.zrange(key1, 0, -1)
            await conn.delete(key1)
        frm = "SORTED_SET_CMD - 'ZREMRANGEBYLEX': KEY- {0}, " \
              "BEFORE - {1}, REM_NUM - {2}, AFTER - {3}\n"
        logger.debug(frm.format(key1, res1, res2, res3))

    async def rd_zremrangebyrank_cmd(self):
        """
        Removes all elements in the sorted set stored
          at key with rank between start and stop.
          Both start and stop are 0 -based indexes
          with 0 being the element with the lowest score.
          These indexes can be negative numbers, where
          they indicate offsets starting at the element
          with the highest score. For example: -1 is the
          element with the highest score, -2 the element
          with the second highest score and so forth.
          Return value:
          - the number of elements removed.

        :return: None
        """
        key1 = 'key1'
        values = ('a', 'b', 'c', 'd', 'f')
        scores = (1, 2, 3, 4, 5)
        pairs = list(chain(*zip(scores, values)))
        with await self.rd1 as conn:
            await conn.zadd(key1, *pairs)
            res1 = await conn.zrange(key1, 0, -1)
            res2 = await conn.zremrangebyrank(key1, 0, 3)
            res3 = await conn.zrange(key1, 0, -1)
            await conn.delete(key1)
        frm = "SORTED_SET_CMD - 'ZREMRANGEBYRANK': KEY- {0}, " \
              "BEFORE - {1}, REM_NUM - {2}, AFTER - {3}\n"
        logger.debug(frm.format(key1, res1, res2, res3))

    async def rd_zremrangebyscore_cmd(self):
        """
        Removes all elements in the sorted set stored
          at key with a score between min and max (inclusive).
          Return value:
          - the number of elements removed.

        :return: None
        """
        key1 = 'key1'
        values = ('a', 'b', 'c', 'd', 'f')
        scores = (1, 2, 3, 4, 5)
        pairs = list(chain(*zip(scores, values)))
        with await self.rd1 as conn:
            await conn.zadd(key1, *pairs)
            res1 = await conn.zrange(key1, 0, -1)
            res2 = await conn.zremrangebyscore(key1, min=0, max=4)
            res3 = await conn.zrange(key1, 0, -1)
            await conn.delete(key1)
        frm = "SORTED_SET_CMD - 'ZREMRANGEBYSCORE': KEY- {0}, " \
              "BEFORE - {1}, REM_NUM - {2}, AFTER - {3}\n"
        logger.debug(frm.format(key1, res1, res2, res3))

    async def rd_zrevrange_cmd(self):
        """
        Returns the specified range of elements in the
          sorted set stored at key. The elements are
          considered to be ordered from the highest to
          the lowest score. Descending lexicographical
          order is used for elements with equal score.
          Apart from the reversed ordering, ZREVRANGE
          is similar to ZRANGE.
          Return value:
          - list of elements in the specified range
            (optionally with their scores).

        :return: None
        """
        key1 = 'key1'
        values = ('a', 'b', 'c', 'd', 'f')
        scores = (1, 2, 3, 4, 5)
        pairs = list(chain(*zip(scores, values)))
        with await self.rd1 as conn:
            await conn.zadd(key1, *pairs)
            res1 = await conn.zrevrange(key1, 0, -1, withscores=True)
            await conn.delete(key1)
        frm = "SORTED_SET_CMD - 'ZREVRANGE': KEY- {0}, REVRANGE - {1}\n"
        logger.debug(frm.format(key1, res1))

    async def rd_zrevrangebyscore_cmd(self):
        """
        Returns all the elements in the sorted set at
          key with a score between max and min (including
          elements with score equal to max or min). In
          contrary to the default ordering of sorted sets,
          for this command the elements are considered to
          be ordered from high to low scores.
          The elements having the same score are returned
          in reverse lexicographical order.
          Return value:
          - list of elements in the specified score range
            (optionally with their scores).

        :return: None
        """
        key1 = 'key1'
        values = ('a', 'b', 'c', 'd', 'f')
        scores = (1, 2, 3, 4, 5)
        pairs = list(chain(*zip(scores, values)))
        with await self.rd1 as conn:
            await conn.zadd(key1, *pairs)
            res1 = await conn.zrevrangebyscore(key1, withscores=True)
            await conn.delete(key1)
        frm = "SORTED_SET_CMD - 'ZREVRANGEBYSCORE': KEY- {0}, REVRANGEBYSCORE - {1}\n"
        logger.debug(frm.format(key1, res1))

    async def rd_zrevrangebylex_cmd(self):
        """
        When all the elements in a sorted set are inserted
          with the same score, in order to force lexicographical
          ordering, this command returns all the elements
          in the sorted set at key with a value between max
          and min.
          Return value:
          - list of elements in the specified score range.

        :return: None
        """
        key1 = 'key1'
        values = ('a', 'b', 'c', 'd', 'f')
        scores = (1, 2, 2, 3, 3)
        pairs = list(chain(*zip(scores, values)))
        with await self.rd1 as conn:
            await conn.zadd(key1, *pairs)
            res1 = await conn.zrevrangebylex(key1, min=b'a', max=b'd',
                                             include_min=True, include_max=True)
            await conn.delete(key1)
        frm = "SORTED_SET_CMD - 'ZREVRANGEBYLEX': KEY- {0}, REVRANGEBYLEX - {1}\n"
        logger.debug(frm.format(key1, res1))

    async def rd_zrevrank_cmd(self):
        """
        Returns the rank of member in the sorted
          set stored at key, with the scores ordered
          from high to low. The rank (or index) is
          0-based, which means that the member with
          the highest score has rank 0.
          Return value:
          - If member exists in the sorted set -
            the rank of member.
          - If member does not exist in the sorted
            set or key does not exist, - nil.

        :return: None
        """
        key1 = 'key1'
        values = ('a', 'b', 'c', 'd', 'f')
        scores = (1, 2, 3, 4, 5, 6)
        pairs = list(chain(*zip(scores, values)))
        with await self.rd1 as conn:
            await conn.zadd(key1, *pairs)
            res1 = await conn.zrevrank(key1, 'b')
            res2 = await conn.zrevrank(key1, 'g')
            await conn.delete(key1)
        frm = "SORTED_SET_CMD - 'ZREVRANK': KEY- {0}, " \
              "EXIST_VALUE - {1}, NOT_EXIST_VALUE - {2}\n"
        logger.debug(frm.format(key1, res1, res2))

    async def rd_zscore_cmd(self):
        """
        Returns the score of member in the
          sorted set at key. If member does
          not exist in the sorted set, or key
          does not exist, nil is returned.
          Return value:
          - the score of member (a double
            precision floating point number),
            represented as string.

        :return: None
        """
        key1, key2 = 'key1', 'key2'
        values = ('a', 'b', 'c', 'd', 'f')
        scores = (1, 2, 3, 4, 5, 6)
        pairs = list(chain(*zip(scores, values)))
        with await self.rd1 as conn:
            await conn.zadd(key1, *pairs)
            res1 = await conn.zscore(key1, 'b')
            res2 = await conn.zscore(key1, 'g')
            res3 = await conn.zscore(key2, 'b')
            await conn.delete(key1)
        frm = "SORTED_SET_CMD - 'ZSCORE': KEYS - {0}, EXIST_VALUE - {1}," \
              " NOT_EXIST_VALUE - {2}, NOT_EXIST_KEY - {3}\n"
        logger.debug(frm.format((key1, key2), res1, res2, res3))

    async def rd_zunionstore_cmd(self):
        """
        Returns the score of member in the
          sorted set at key. If member does
          not exist in the sorted set, or key
          does not exist, nil is returned.
          Return value:
          - the score of member (a double
            precision floating point number),
            represented as string.

        :return: None
        """
        key1, key2, dest_key = 'key1', 'key2', 'key3'
        values = ('a', 'b', 'c', 'd', 'f')
        scores1, scores2 = (1, 2, 3, 4, 5, 6), (6, 5, 4, 3, 2, 1)
        pairs1 = list(chain(*zip(scores1, values)))
        pairs2 = list(chain(*zip(scores2, values)))
        with await self.rd1 as conn:
            await conn.zadd(key1, *pairs1)
            await conn.zadd(key2, *pairs2)
            await conn.zunionstore(dest_key, (key1, 1), (key2, 2), with_weights=True,
                                   aggregate='ZSET_AGGREGATE_MAX')
            res1 = await conn.zrange(dest_key, 0, -1, withscores=True)
            await conn.delete(key1)
        frm = "SORTED_SET_CMD - 'ZUNIONSTORE': KEYS - {0}, UNION_RES - {1}\n"
        logger.debug(frm.format((key1, key2), res1))

    async def rd_zscan_cmd(self):
        """
        SCAN is a cursor based iterator. This means that at
          every call of the command, the server returns an
          updated cursor that the user needs to use as the
          cursor argument in the next call. An iteration
          starts when the cursor is set to 0, and terminates
          when the cursor returned by the server is 0.

        :return: None
        """
        key1 = 'key1'
        values_tmp = ('TEST%s', 'test%s', 't%s')
        values = (choice(values_tmp) % i for i in range(1, 5))
        scores = (randint(1, 10) for _ in range(1, 5))
        pairs = list(chain(*zip(scores, values)))
        matched_keys = []
        match, cur = b'test*', b'0'
        with await self.rd1 as conn:
            await conn.zadd(key1, *pairs)
            while cur:
                cur, keys = await conn.zscan(key1, cur, match=match)
                matched_keys.extend(keys)
            await conn.flushdb()
        frm = "SET_CMD - 'ZSCAN': KEY_TMP- {0}, MATCH_STR - {1}, MATCHED_KEYS - {2}\n"
        logger.debug(frm.format(key1, match, len(matched_keys)))

    async def rd_izscan_cmd(self):
        """
        Incrementally iterate the keys space using async for

        :return: None
        """
        key1 = 'key1'
        values_tmp = ('TEST%s', 'test%s', 't%s')
        values = (choice(values_tmp) % i for i in range(1, 5))
        scores = (randint(1, 10) for _ in range(1, 5))
        pairs = list(chain(*zip(scores, values)))
        matched_keys = []
        match, cur = b'test*', b'0'
        with await self.rd1 as conn:
            await conn.zadd(key1, *pairs)
            async for key in conn.izscan(key1, match=match):
                matched_keys.extend(key)
            await conn.flushdb()
        frm = "SET_CMD - 'IZSCAN': KEY_TMP- {0}, MATCH_STR - {1}, MATCHED_KEYS - {2}\n"
        logger.debug(frm.format(key1, match, len(matched_keys)))


def main():
    # load config from yaml file
    conf = load_config(os.path.join(BASE_DIR, "config_files/dev.yml"))
    # create event loop
    loop = asyncio.get_event_loop()
    # rd1 = await RedisClient.connect(conf=conf['redis'], loop=loop)
    # rd2 = await RedisClient.connect(conf=conf['redis'], loop=loop)
    rd1_conn = loop.run_until_complete(rd_client_factory(loop=loop, conf=conf['redis1']))
    rd2_conn = loop.run_until_complete(rd_client_factory(loop=loop, conf=conf['redis2']))
    rgc = RedisSortedSetCommands(rd1_conn.rd, rd2_conn.rd, conf=conf['redis2'])
    try:
        loop.run_until_complete(rgc.run_sorted_set_cmd())
    except KeyboardInterrupt as e:
        logger.error("Caught keyboard interrupt {0}\nCanceling tasks...".format(e))
    finally:
        loop.run_until_complete(rd1_conn.close_connection())
        loop.run_until_complete(rd2_conn.close_connection())
        loop.close()


if __name__ == '__main__':
    main()
