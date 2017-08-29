# -*- coding: utf-8 -*-
"""
    Simple example of commands with STRING KEY using async lib - aioredis
"""
import asyncio
import os

from redis_client import rd_client_factory
from settings import BASE_DIR, logger
from utils import load_config


class RedisStrCommands:
    def __init__(self, rd):
        self.rd = rd

    async def run_rd_str_commands(self):
        await self.rd_set_cmd()
        await self.rd_append_cmd()
        await self.rd_bitcount_cmd()
        await self.rd_bitop_and_cmd()
        await self.rd_bitop_or_cmd()
        await self.rd_bitop_xor_cmd()
        await self.rd_bitop_not_cmd()
        await self.rd_bitpos_cmd()
        await self.rd_decr_cmd()
        await self.rd_incr_cmd()
        await self.rd_decrby_cmd()
        await self.rd_incrby_cmd()
        await self.rd_setbit_cmd()

    async def rd_set_cmd(self):
        """
        Set key to hold the string value. If key already holds a value, it is
        overwritten, regardless of its type. Any previous time to live associated
        with the key is discarded on successful SET operation.

        :return: None
        """
        key = 'str_set_cmd'
        value = 'test_str_set_cmd'
        with await self.rd as conn:
            await conn.set(key, value)
            res = await conn.get(key)
            conn.delete(key)
        frm = "STR_CMD - 'SET -> GET': KEY - {0}, VALUE - {1}\n"
        logger.debug(frm.format(key, res))

    async def rd_append_cmd(self):
        """
        If key already exists and is a string, this command appends the value at
          the end of the string. If key does not exist it is created and set as
          an empty string, so APPEND will be similar to SET in this special case.

        :return: None
        """
        key = 'str_append_cmd'
        value1 = 'test_str_append(new)_cmd___'
        value2 = 'test_str_append_cmd'
        with await self.rd as conn:
            await conn.append(key, value1)
            await conn.append(key, value2)
            res = await conn.get(key)
            conn.delete(key)
        frm = "STR_CMD - 'APPEND(NEW) -> APPEND -> GET': KEY - {0}, VALUE - {1}\n"
        logger.debug(frm.format(key, res))

    async def rd_bitcount_cmd(self):
        """
        Count the number of set bits (population counting) in a string.
        By default all the bytes contained in the string are examined.
        It is possible to specify the counting operation only in an
        interval passing the additional arguments start and end.

        :return: None
        """
        key = 'str_bitcount_cmd'
        value = 'foobar'
        with await self.rd as conn:
            await conn.set(key, value)
            res1 = await conn.bitcount(key)
            res2 = await conn.bitcount(key, 0, 0)
            res3 = await conn.bitcount(key, 0, 15)
            conn.delete(key)
        frm = "STR_CMD - 'BITCOUNT': KEY - {0}, VALUE - {1}\n"
        logger.debug(frm.format(key, [res1, res2, res3]))

    async def rd_bitop_and_cmd(self):
        """
        Perform a bitwise operation between multiple keys
        (containing string values) and store the result in the destination key.

        BITOP AND destkey srckey1 srckey2 srckey3 ... srckeyN

        The result of the operation is always stored at destkey.
        EXAMPLE:
        a_byte = bytearray('foobar', 'utf-8')
        b_byte = bytearray('abcdef', 'utf-8')
        res = bytearray(a_byte[i] & b_byte[i] for i in range(len(b_byte)))

        bytearray(b'`bc`ab') - RESULT

        :return: None
        """
        destkey = 'str_bitop_and_cmd'
        key1 = 'key_1'
        key2 = 'key_2'
        value1 = 'foobar'
        value2 = 'abcdef'
        with await self.rd as conn:
            await conn.set(key1, value1)
            await conn.set(key2, value2)
            res1 = await conn.bitop_and(destkey, key1, key2)
            res2 = await conn.get(destkey)
            conn.delete(destkey, key1, key2)
        frm = "STR_CMD - 'BITOP_AND': KEY - {0}, VALUE - {1}\n"
        logger.debug(frm.format(destkey, [res1, res2]))

    async def rd_bitop_or_cmd(self):
        """
        Perform a bitwise operation between multiple keys
        (containing string values) and store the result in the destination key.

        BITOP OR destkey srckey1 srckey2 srckey3 ... srckeyN

        The result of the operation is always stored at destkey.
        EXAMPLE:
        a_byte = bytearray('foobar', 'utf-8')
        b_byte = bytearray('abcdef', 'utf-8')
        res = bytearray(a_byte[i] | b_byte[i] for i in range(len(b_byte)))

        bytearray(b'goofev') - RESULT

        :return: None
        """
        destkey = 'str_bitop_or_cmd'
        key1 = 'key_1'
        key2 = 'key_2'
        value1 = 'foobar'
        value2 = 'abcdef'
        with await self.rd as conn:
            await conn.set(key1, value1)
            await conn.set(key2, value2)
            res1 = await conn.bitop_or(destkey, key1, key2)
            res2 = await conn.get(destkey)
            conn.delete(destkey, key1, key2)
        frm = "STR_CMD - 'BITOP_OR': KEY - {0}, VALUE - {1}\n"
        logger.debug(frm.format(destkey, [res1, res2]))

    async def rd_bitop_xor_cmd(self):
        """
        Perform a bitwise operation between multiple keys
        (containing string values) and store the result in the destination key.

        BITOP XOR destkey srckey1 srckey2 srckey3 ... srckeyN

        The result of the operation is always stored at destkey.
        EXAMPLE:
        a_byte = bytearray('foobar', 'utf-8')
        b_byte = bytearray('abcdef', 'utf-8')
        res = bytearray(a_byte[i] ^ b_byte[i] for i in range(len(b_byte)))

        bytearray(b'\x07\r\x0c\x06\x04\x14') - RESULT

        :return: None
        """
        destkey = 'str_bitop_xor_cmd'
        key1 = 'key_1'
        key2 = 'key_2'
        value1 = 'foobar'
        value2 = 'abcdef'
        with await self.rd as conn:
            await conn.set(key1, value1)
            await conn.set(key2, value2)
            res1 = await conn.bitop_xor(destkey, key1, key2)
            res2 = await conn.get(destkey)
            conn.delete(destkey, key1, key2)
        frm = "STR_CMD - 'BITOP_XOR': KEY - {0}, VALUE - {1}\n"
        logger.debug(frm.format(destkey, [res1, res2]))

    async def rd_bitop_not_cmd(self):
        """
        Perform a bitwise operation between multiple keys
        (containing string values) and store the result in the destination key.

        BITOP NOT destkey srckey

        The result of the operation is always stored at destkey.
        EXAMPLE:
        a_byte = bytearray('foobar', 'utf-8')
        b_byte = bytearray('abcdef', 'utf-8')
        res = bytearray(~a_byte[i] + 256  for i in range(len(a_byte)))

        bytearray(b'\x99\x90\x90\x9d\x9e\x8d') - RESULT

        :return: None
        """
        destkey = 'str_bitop_xor_cmd'
        key1 = 'key_1'
        value1 = 'foobar'
        with await self.rd as conn:
            await conn.set(key1, value1)
            res1 = await conn.bitop_not(destkey, key1)
            res2 = await conn.get(destkey)
            conn.delete(destkey, key1)
        frm = "STR_CMD - 'BITOP_NOT': KEY - {0}, VALUE - {1}\n"
        logger.debug(frm.format(destkey, [res1, res2]))

    async def rd_bitpos_cmd(self):
        """
        Return the position of the first bit set to 1 or 0 in a string.

        BITPOS key [start] [end]

        The command returns the position of the first bit set to 1 or 0
          according to the request. If we look for set bits (the bit
          argument is 1) and the string is empty or composed of just zero
          bytes, -1 is returned.

        :return: None
        """
        key = 'key'
        value = "\x00\xff\xf0"
        with await self.rd as conn:
            await conn.set(key, value)
            res = await conn.bitpos(key, 1, 0)
            conn.delete(key)
        frm = "STR_CMD - 'BITPOS': KEY - {0}, VALUE - {1}\n"
        logger.debug(frm.format(key, res))

    async def rd_decr_cmd(self):
        """
        Decrements the number stored at key by one. If the key does not
          exist, it is set to 0 before performing the operation. An error
          is returned if the key contains a value of the wrong type or
          contains a string that can not be represented as integer. This
          operation is limited to 64 bit signed integers.

        :return: None
        """
        key = 'key'
        value = "10"
        with await self.rd as conn:
            await conn.set(key, value)
            res = await conn.decr(key)
            conn.delete(key)
        frm = "STR_CMD - 'DECR': KEY - {0}, BEFORE - {1}, AFTER - {2}\n"
        logger.debug(frm.format(key, value, res))

    async def rd_decrby_cmd(self):
        """
        Decrements the number stored at key by decrement. If the key does
          not exist, it is set to 0 before performing the operation. An
          error is returned if the key contains a value of the wrong type
          or contains a string that can not be represented as integer.
          This operation is limited to 64 bit signed integers.

        :return: None
        """
        key = 'key'
        value = "10"
        with await self.rd as conn:
            await conn.set(key, value)
            res = await conn.decrby(key, 3)
            conn.delete(key)
        frm = "STR_CMD - 'DECRBY': KEY - {0}, BEFORE - {1}, AFTER - {2}\n"
        logger.debug(frm.format(key, value, res))

    async def rd_incr_cmd(self):
        """
        Increments the number stored at key by one. If the key does not
          exist, it is set to 0 before performing the operation. An error
          is returned if the key contains a value of the wrong type or
          contains a string that can not be represented as integer. This
          operation is limited to 64 bit signed integers.

        :return: None
        """
        key = 'key'
        value = "10"
        with await self.rd as conn:
            await conn.set(key, value)
            res = await conn.incr(key)
            conn.delete(key)
        frm = "STR_CMD - 'INCR': KEY - {0}, BEFORE - {1}, AFTER - {2}\n"
        logger.debug(frm.format(key, value, res))

    async def rd_incrby_cmd(self):
        """
        Increments the number stored at key by increment. If the key does
          not exist, it is set to 0 before performing the operation. An
          error is returned if the key contains a value of the wrong type
          or contains a string that can not be represented as integer.
          This operation is limited to 64 bit signed integers.

        :return: None
        """
        key = 'key'
        value = "0"
        with await self.rd as conn:
            await conn.set(key, value)
            res = await conn.incrby(key, 4)
            conn.delete(key)
        frm = "STR_CMD - 'INCRBY': KEY - {0}, BEFORE - {1}, AFTER - {2}\n"
        logger.debug(frm.format(key, value, res))

    async def rd_setbit_cmd(self):
        """
        Sets or clears the bit at offset in the string value stored at key.
          The bit is either set or cleared depending on value, which can
          be either 0 or 1. When key does not exist, a new string value is
          created. The string is grown to make sure it can hold a bit at
          offset. The offset argument is required to be greater than or
          equal to 0, and smaller than 2^32 (this limits bitmaps to 512MB).
          When the string at key is grown, added bits are set to 0.

        :return: None
        """
        key = 'key'
        offset = 7
        bit_val1 = 1
        bit_val2 = 0
        with await self.rd as conn:
            res1 = await conn.setbit(key, offset, bit_val1)
            res1_val = await conn.get(key)
            res2 = await conn.setbit(key, offset, bit_val2)
            res2_val = await conn.get(key)
            conn.delete(key)
        frm = "STR_CMD - 'SETBIT': KEY - {0}, BEFORE - {1}, AFTER - {2}\n"
        logger.debug(frm.format(key, [res1, res1_val], [res2, res2_val]))


def main():
    # load config from yaml file
    conf = load_config(os.path.join(BASE_DIR, "config_files/dev.yml"))
    # create event loop
    loop = asyncio.get_event_loop()
    rd_conn = loop.run_until_complete(rd_client_factory(loop=loop, conf=conf['redis']))
    rsc = RedisStrCommands(rd_conn.rd)
    try:
        loop.run_until_complete(rsc.run_rd_str_commands())
    except KeyboardInterrupt as e:
        logger.error(f"Caught keyboard interrupt {0}\nCanceling tasks...".format(e))
    finally:
        loop.run_until_complete(rd_conn.close_connection())
        loop.close()

if __name__ == '__main__':
    main()
