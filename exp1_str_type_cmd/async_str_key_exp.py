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
        await self.rd_append_cmd()
        await self.rd_bitcount_cmd()
        await self.rd_bitop_and_cmd()
        await self.rd_bitop_or_cmd()
        await self.rd_bitop_xor_cmd()
        await self.rd_bitop_not_cmd()
        await self.rd_bitpos_cmd()
        await self.rd_decr_cmd()
        await self.rd_decrby_cmd()
        await self.rd_incr_cmd()
        await self.rd_incrby_cmd()
        await self.rd_incrbyfloat_cmd()
        await self.rd_set_cmd()
        await self.rd_setbit_cmd()
        await self.rd_setex_cmd()
        await self.rd_setnx_cmd()
        await self.rd_setrange_cmd()
        await self.rd_mset_cmd()
        await self.rd_getbit_cmd()
        await self.rd_getrange_cmd()
        await self.rd_getset_cmd()
        await self.rd_mget_cmd()
        await self.rd_strlen_cmd()

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

    async def rd_incrbyfloat_cmd(self):
        """
        Increment the string representing a floating point number stored at
          key by the specified increment. By using a negative increment value,
          the result is that the value stored at the key is decremented
          (by the obvious properties of addition). If the key does not exist,
          it is set to 0 before performing the operation. An error is returned
          if one of the following conditions occur:
            The key contains a value of the wrong type (not a string).
            The current key content or the specified increment are not
            parsable as a double precision floating point number.

        :return: None
        """
        key = 'key'
        start_float_num = 10.50
        with await self.rd as conn:
            await conn.set(key, start_float_num)
            res1 = await conn.incrbyfloat(key, 0.1)
            res2 = await conn.incrbyfloat(key, -5.0)
            conn.delete(key)
        frm = "STR_CMD - 'INCRBYFLOAT': KEY - {0}, INCR_FLOAT - {1}, DECR_FLOAT - {2}\n"
        logger.debug(frm.format(key, res1, res2))

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

    async def rd_setex_cmd(self):
        """
        Set key to hold the string value and set key to timeout
          after a given number of seconds. This command is
          equivalent to executing the following commands:
            SET mykey value
            EXPIRE mykey seconds

        :return: None
        """
        key = 'key'
        value = 'test_str_setex_cmd'
        time_of_ex = 10
        with await self.rd as conn:
            await conn.setex(key, time_of_ex, value)
            await asyncio.sleep(2)
            ttl = await conn.ttl(key)
            res = await conn.get(key)
            conn.delete(key)
        frm = "STR_CMD - 'SETEX': KEY - {0}, TIME_OF_EX - {1}, VALUE - {2}\n"
        logger.debug(frm.format(key, ttl, res))

    async def rd_setnx_cmd(self):
        """
        Set key to hold string value if key does not exist.
          In that case, it is equal to SET. When key already
          holds a value, no operation is performed.
          SETNX is short for "SET if Not eXists".
            Return value:
              1 if the key was set
              0 if the key was not set

        :return: None
        """
        key = 'key'
        value = 'test_str_setnx_cmd'
        with await self.rd as conn:
            res1 = await conn.setnx(key, value)
            res2 = await conn.setnx(key, value)
            conn.delete(key)
        frm = "STR_CMD - 'SETNX': KEY - {0}, FIRST_SET - {1}, SECOND_SET - {2}\n"
        logger.debug(frm.format(key, res1, res2))

    async def rd_setrange_cmd(self):
        """
        Overwrites part of the string stored at key, starting at
          the specified offset, for the entire length of value.
          If the offset is larger than the current length of the
          string at key, the string is padded with zero-bytes to
          make offset fit. Non-existing keys are considered as
          empty strings, so this command will make sure it holds a
          string large enough to be able to set value at offset.

        :return: None
        """
        key = 'key'
        value = 'Hello World'
        new_value = 'Redis'
        offset = 6
        with await self.rd as conn:
            await conn.set(key, value)
            res1 = await conn.get(key)
            await conn.setrange(key, offset, new_value)
            res2 = await conn.get(key)
            conn.delete(key)
        frm = "STR_CMD - 'SETRANGE': KEY - {0}, BEFORE - {1}, AFTER - {2}\n"
        logger.debug(frm.format(key, res1, res2))

    async def rd_mset_cmd(self):
        """
        Sets the given keys to their respective values.
          MSET replaces existing values with new values,
          just as regular SET.
        See MSETNX if you don't want to overwrite existing values.
        MSET is atomic, so all given keys are set at once.
          It is not possible for clients to see that some of
          the keys were updated while others are unchanged.

        :return: None
        """
        key1 = 'key1'
        key2 = 'key2'
        set_val1 = 'TEST1'
        set_val2 = 'TEST2'
        with await self.rd as conn:
            await conn.mset(key1, set_val1, key2, set_val2)
            res = await conn.mget(key1, key2)
            conn.delete(key1, key2)
        frm = "STR_CMD - 'MSET': KEY1, 2 - {0}, MSET_RES - {1}\n"
        logger.debug(frm.format([key1, key2], res))

    async def rd_getbit_cmd(self):
        """
        Returns the bit value at offset in the string value stored at key.
          When offset is beyond the string length, the string is assumed
          to be a contiguous space with 0 bits. When key does not exist it
          is assumed to be an empty string, so offset is always out of
          range and the value is also assumed to be a contiguous space
          with 0 bits.

        :return: None
        """
        key = 'key'
        offset1 = 7
        offset2 = 0
        bit_val1 = 1
        with await self.rd as conn:
            await conn.setbit(key, offset1, bit_val1)
            res1 = await conn.getbit(key, offset2)
            res2 = await conn.getbit(key, offset1)
            conn.delete(key)
        frm = "STR_CMD - 'GETBIT': KEY - {0}, NOT_SETBIT - {1}, SETBIT - {2}\n"
        logger.debug(frm.format(key, res1, res2))

    async def rd_getrange_cmd(self):
        """
        Returns the substring of the string value stored at key, determined
          by the offsets start and end (both are inclusive). Negative
          offsets can be used in order to provide an offset starting from
          the end of the string. So -1 means the last character, -2 the
          penultimate and so forth. The function handles out of range requests
          by limiting the resulting range to the actual length of the string.

        :return: None
        """
        key = 'key'
        start1 = 0
        end1 = 3
        start2 = -6
        end2 = -1
        val1 = "This is a string"
        with await self.rd as conn:
            await conn.set(key, val1)
            res1 = await conn.getrange(key, start1, end1)
            res2 = await conn.getrange(key, start2, end2)
            conn.delete(key)
        frm = "STR_CMD - 'GETRANGE': KEY - {0}, DIRECT - {1}, REVERS - {2}\n"
        logger.debug(frm.format(key, res1, res2))

    async def rd_getset_cmd(self):
        """
        Atomically sets key to value and returns the old value stored
          at key. Returns an error when key exists but does not hold
          a string value.
          'Design pattern':
            GETSET can be used together with INCR for counting with
            atomic reset. For example: a process may call INCR against
            the key mycounter every time some event occurs, but from
            time to time we need to get the value of the counter and
            reset it to zero atomically. This can be done using
            GETSET mycounter "0"

        :return: None
        """
        key = 'key'
        set_val = 0
        with await self.rd as conn:
            await conn.incr(key)
            res1 = await conn.getset(key, set_val)
            res2 = await conn.get(key)
            conn.delete(key)
        frm = "STR_CMD - 'GETSET': KEY - {0}, INCR_DATA - {1}, AFTER_GETSET_0 - {2}\n"
        logger.debug(frm.format(key, res1, res2))

    async def rd_mget_cmd(self):
        """
        Returns the values of all specified keys. For every key that
          does not hold a string value or does not exist, the special
          value nil is returned. Because of this, the operation never fails.

        :return: None
        """
        key1 = 'key1'
        key2 = 'key2'
        set_val1 = 'TEST1'
        set_val2 = 'TEST2'
        with await self.rd as conn:
            await conn.set(key1, set_val1)
            await conn.set(key2, set_val2)
            res = await conn.mget(key1, key2)
            conn.delete(key1, key2)
        frm = "STR_CMD - 'MGET': KEY1 - {0}, KEY2 - {1}, MGET_RES - {2}\n"
        logger.debug(frm.format(key1, key2, res))

    async def rd_strlen_cmd(self):
        """
        Returns the length of the string value stored at key.
          An error is returned when key holds a non-string value.
          Return value:
            Integer reply: the length of the string at key,
            or 0 when key does not exist.

        :return: None
        """
        key = 'key'
        set_val = 'test_str_strlen_cmd'
        with await self.rd as conn:
            await conn.set(key, set_val)
            res = await conn.strlen(key)
            conn.delete(key)
        frm = "STR_CMD - 'STRLEN': KEY - {0}, RES(test_str_strlen_cmd) - {1}\n"
        logger.debug(frm.format(key, res))


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
        logger.error("Caught keyboard interrupt {0}\nCanceling tasks...".format(e))
    finally:
        loop.run_until_complete(rd_conn.close_connection())
        loop.close()

if __name__ == '__main__':
    main()
