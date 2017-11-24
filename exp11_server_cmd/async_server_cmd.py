# -*- coding: utf-8 -*-
"""
    Simple example of Redis Server commands using async lib - aioredis
    For commands details see: http://redis.io/commands/#server
"""
import asyncio
import os

from redis_client import rd_client_factory
from settings import BASE_DIR, logger
from utils import load_config


class RedisServerCommands:
    def __init__(self, rd1, rd2, conf=None):
        self.rd1 = rd1
        self.rd2 = rd2
        self.rd_conf = conf

    async def run_server_cmd(self):
        await self.server_bgrewriteaof_cmd()
        await self.server_bgsave_cmd()
        await self.server_client_list_cmd()
        await self.server_client_getname_cmd()
        await self.server_client_pause_cmd()
        await self.server_client_setname_cmd()
        await self.server_config_get_cmd()
        await self.server_config_rewrite_cmd()
        await self.server_config_set_cmd()
        await self.server_config_resetstat_cmd()
        await self.server_dbsize_cmd()
        await self.server_debug_object_cmd()
        await self.server_flushall_cmd()
        await self.server_flushdb_cmd()
        await self.server_info_cmd()
        await self.server_lastsave_cmd()

    async def server_bgrewriteaof_cmd(self):
        """
        Instruct Redis to start an Append Only File
          rewrite process. The rewrite will create a
          small optimized version of the current
          Append Only File.
          If BGREWRITEAOF fails, no data gets lost
          as the old AOF will be untouched.
          The rewrite will be only triggered by Redis
          if there is not already a background process
          doing persistence.

        :return: None
        """
        key1, key2 = 'key_list1', 'key_list2'
        values1, values2 = ['TEST1', 'TEST2', 'TEST3'], ['test1', 'test2']
        with await self.rd1 as conn:
            await conn.rpush(key1, *values1)
            await conn.rpush(key2, *values2)
        with await self.rd1 as conn:
            aof_dir = await conn.config_get(parameter='dir')
            res1 = await conn.bgrewriteaof()
            await asyncio.sleep(2)
            res2 = os.listdir(aof_dir['dir'])
            await conn.delete(key1, key2)
        frm = "SERVER_CMD - 'BGREWRITEAOF': RES - {0}, AOF_DIR - {1}, AOF_EXIST - {2}\n"
        logger.debug(frm.format(res1, aof_dir, res2))

    async def server_bgsave_cmd(self):
        """
        Save the DB in background. The OK code is immediately
          returned. Redis forks, the parent continues to serve
          the clients, the child saves the DB on disk then exits.
          A client may be able to check if the operation
          succeeded using the LASTSAVE command.

        :return: None
        """
        with await self.rd1 as conn:
            ls_before = await conn.lastsave()
            res1 = await conn.bgsave()
            await asyncio.sleep(2)
            ls_after = await conn.lastsave()
        frm = "SERVER_CMD - 'BGSAVE': BGSAVE_RES - {0}, LAST_SAVE_AFTER - {1}, " \
              "LAST_SAVE_BEFORE - {2}\n"
        logger.debug(frm.format(res1, ls_before, ls_after))

    async def server_client_list_cmd(self):
        """
        The CLIENT LIST command returns information and
          statistics about the client connections server in
          a mostly human readable format.
          Here is the meaning of the fields:
            - id: an unique 64-bit client ID (introduced in Redis 2.8.12).
            - addr: address/port of the client
            - fd: file descriptor corresponding to the socket
            - age: total duration of the connection in seconds
            - idle: idle time of the connection in seconds
            - flags: client flags (see below)
            - db: current database ID
            - sub: number of channel subscriptions
            - psub: number of pattern matching subscriptions
            - multi: number of commands in a MULTI/EXEC context
            - qbuf: query buffer length (0 means no query pending)
            - qbuf-free: free space of the query buffer (0 means the buffer is full)
            - obl: output buffer length
            - oll: output list length (replies are queued in this list when the buffer is full)
            - omem: output buffer memory usage
            - events: file descriptor events (see below)
            -cmd: last command played

        :return: None
        """
        with await self.rd1 as conn:
            await conn.client_setname('test_name')
            res1 = await conn.client_list()
        frm = "SERVER_CMD - 'CLIENT_LIST': LIST_LEN - {0}, RES - {1}\n"
        logger.debug(frm.format(len(res1), res1))

    async def server_client_getname_cmd(self):
        """
        The CLIENT GETNAME returns the name of the current
          connection as set by CLIENT SETNAME. Since every
          new connection starts without an associated name,
          if no name was assigned a null bulk reply is returned.

        :return: None
        """
        with await self.rd1 as conn:
            await conn.client_setname('test_name')
            res1 = await conn.client_getname()
        frm = "SERVER_CMD - 'CLIENT_GETNAME': RES - {0}\n"
        logger.debug(frm.format(res1))

    async def server_client_pause_cmd(self):
        """
        CLIENT PAUSE is a connections control command able
          to suspend all the Redis clients for the specified
          amount of time (in milliseconds).
          The command performs the following actions:
           - It stops processing all the pending commands from
             normal and pub/sub clients. However interactions
             with slaves will continue normally.
           - However it returns OK to the caller ASAP, so the
             CLIENT PAUSE command execution is not paused by itself.
           - When the specified amount of time has elapsed, all
             the clients are unblocked: this will trigger the
             processing of all the commands accumulated in
             the query buffer of every client during the pause.

        :return: None
        """
        with await self.rd1 as conn:
            res1 = await conn.client_pause(2)
        frm = "SERVER_CMD - 'CLIENT_PAUSE': RES - {0}\n"
        logger.debug(frm.format(res1))

    async def server_client_setname_cmd(self):
        """
        The CLIENT SETNAME command assigns a name to the
          current connection. The assigned name is displayed
          in the output of CLIENT LIST so that it is possible
          to identify the client that performed a given connection.
          For instance when Redis is used in order to implement
          a queue, producers and consumers of messages may want
          to set the name of the connection according to their role.

        :return: None
        """
        with await self.rd1 as conn:
            res1 = await conn.client_setname('test_name')
            res2 = await conn.client_getname()
        frm = "SERVER_CMD - 'CLIENT_SETNAME': SET_RES - {0}, GET_RES - {1}\n"
        logger.debug(frm.format(res1, res2))

    async def server_config_get_cmd(self):
        """
        The CONFIG GET command is used to read the configuration
          parameters of a running Redis server. You can obtain a
          list of all the supported configuration parameters
          by typing 'CONFIG GET *'

        :return: None
        """
        with await self.rd1 as conn:
            res1 = await conn.config_get(parameter='dir')
        frm = "SERVER_CMD - 'CONFIG_GET': CONF_PARAM - {0}\n"
        logger.debug(frm.format(res1))

    async def server_config_rewrite_cmd(self):
        """
        The CONFIG REWRITE command rewrites the redis.conf
          file the server was started with, applying the
          minimal changes needed to make it reflect the
          configuration currently used by the server,
          which may be different compared to the original
          one because of the use of the CONFIG SET command.

        :return: None
        """
        with await self.rd1 as conn:
            res1 = await conn.config_rewrite()
        frm = "SERVER_CMD - 'CONFIG_REWRITE': REWRITE_CONF_RES - {0}\n"
        logger.debug(frm.format(res1))

    async def server_config_set_cmd(self):
        """
        The CONFIG SET command is used in order to
          reconfigure the server at run time without
          the need to restart Redis. You can change
          both trivial parameters or switch from one
          to another persistence option using this command.

        :return: None
        """
        with await self.rd1 as conn:
            res_before = await conn.config_get(parameter='appendonly')
            value = 'no' if res_before['appendonly'] == 'yes' else 'yes'
            res1 = await conn.config_set('appendonly', value)
            res_after = await conn.config_get(parameter='appendonly')
        frm = "SERVER_CMD - 'CONFIG_SET': CONF_PARAM_BEFORE - {0}, RES - {1}, " \
              "CONF_PARAM_AFTER - {2}\n"
        logger.debug(frm.format(res_before, res1, res_after))

    async def server_config_resetstat_cmd(self):
        """
        Resets the statistics reported by Redis
          using the INFO command.
          These are the counters that are reset:
          - Keyspace hits;
          - Keyspace misses;
          - Number of commands processed;
          - Number of connections received;
          - Number of expired keys;
          - Number of rejected connections;
          - Latest fork(2) time;
          - The aof_delayed_fsync counter.

        :return: None
        """
        key = 'key'
        value = 'test_str_setex_cmd'
        time_of_ex = 1
        with await self.rd1 as conn:
            await conn.setex(key, time_of_ex, value)
            info_before = await conn.info()
            await asyncio.sleep(2)
            res1 = await conn.config_resetstat()
            info_after = await conn.info()
            await conn.delete(key)
        frm = "SERVER_CMD - 'CONFIG_RESETSTAT': INFO_BEFORE - {0}, RES - {1}, " \
              "INFO_AFTER - {2}\n"
        logger.debug(frm.format(info_before['keyspace'], res1, info_after['keyspace']))

    async def server_dbsize_cmd(self):
        """
        Return the number of keys in the currently-selected database.

        :return: None
        """
        key = 'key'
        value = 'test_str_setex_cmd'
        time_of_ex = 1
        with await self.rd1 as conn:
            await conn.setex(key, time_of_ex, value)
            res1 = await conn.dbsize()
            await conn.delete(key)
        frm = "SERVER_CMD - 'DBSIZE': RES - {0}\n"
        logger.debug(frm.format(res1))

    async def server_debug_object_cmd(self):
        """
        Get debugging information about a key.

        :return: None
        """
        key = 'key'
        value = 'test_str_setex_cmd'
        time_of_ex = 1
        with await self.rd1 as conn:
            await conn.setex(key, time_of_ex, value)
            res1 = await conn.debug_object(key)
            await conn.delete(key)
        frm = "SERVER_CMD - 'DEBUG_OBJECT': RES - {0}\n"
        logger.debug(frm.format(res1))

    async def server_flushall_cmd(self):
        """
        Delete all the keys of all the existing
          databases, not just the currently
          selected one. This command never fails.
          The time-complexity for this operation
          is O(N), N being the number of keys in
          all existing databases.

        :return: None
        """
        key = 'key'
        value = 'test_str_setex_cmd'
        time_of_ex = 10
        with await self.rd1 as conn:
            await conn.setex(key, time_of_ex, value)
            res1_before = await conn.keys('*')
        with await self.rd2 as conn:
            await conn.setex(key, time_of_ex, value)
            res2_before = await conn.keys('*')
            await conn.flushall()
            res2_after = await conn.keys('*')
        with await self.rd1 as conn:
            res1_after = await conn.keys('*')
        frm = "SERVER_CMD - 'FLUSHALL': RES_DB1_BEFORE - {0},  " \
              "RES_DB1_AFTER - {1}, RES_DB2_BEFORE - {2}, RES_DB2_AFTER - {3}\n"
        logger.debug(frm.format(res1_before, res1_after, res2_before, res2_after))

    async def server_flushdb_cmd(self):
        """
        Delete all the keys of the currently
         selected DB. This command never fails.
         The time-complexity for this operation
         is O(N), N being the number of keys
         in the database.

        :return: None
        """
        key1, key2 = 'key1', 'key2'
        value1, value2 = 'TEST1', 'TEST2'
        time_of_ex = 10
        with await self.rd1 as conn:
            await conn.setex(key1, time_of_ex, value1)
            await conn.setex(key2, time_of_ex, value2)
            res1_before = await conn.keys('*')
            await conn.flushdb()
            res1_after = await conn.keys('*')
        frm = "SERVER_CMD - 'FLUSHDB': RES_DB1_BEFORE - {0}, RES_DB1_AFTER - {1}\n"
        logger.debug(frm.format(res1_before, res1_after))

    async def server_info_cmd(self):
        """
        The INFO command returns information and
          statistics about the server in a format
          that is simple to parse by computers
          and easy to read by humans.
          The optional parameter can be used to
          select a specific section of information:
          - server: General information about the Redis server
          - clients: Client connections section
          - memory: Memory consumption related information
          - persistence: RDB and AOF related information
          - stats: General statistics
          - replication: Master/slave replication information
          - cpu: CPU consumption statistics
          - commandstats: Redis command statistics
          - cluster: Redis Cluster section
          - keyspace: Database related statistics

        :return: None
        """

        with await self.rd1 as conn:
            res1 = await conn.info(section='memory')
        frm = "SERVER_CMD - 'INFO': RES_INFO - {0}\n"
        logger.debug(frm.format(list(res1['memory'].keys())))

    async def server_lastsave_cmd(self):
        """
        Return the UNIX TIME of the last DB save executed
          with success. A client may check if a BGSAVE
          command succeeded reading the LASTSAVE value,
          then issuing a BGSAVE command and checking at
          regular intervals every N seconds if LASTSAVE changed.

        :return: None
        """

        with await self.rd1 as conn:
            ls_before = await conn.lastsave()
            await asyncio.sleep(2)
            res = await conn.bgsave()
            await asyncio.sleep(2)
            ls_after = await conn.lastsave()
        frm = "SERVER_CMD - 'LASTSAVE': BEFORE - {0}, AFTER - {1}, BGSAVE - {2}\n"
        logger.debug(frm.format(ls_before, ls_after, res))


def main():
    # load config from yaml file
    conf = load_config(os.path.join(BASE_DIR, "config_files/dev.yml"))
    # create event loop
    loop = asyncio.get_event_loop()
    # rd1 = await RedisClient.connect(conf=conf['redis'], loop=loop)
    # rd2 = await RedisClient.connect(conf=conf['redis'], loop=loop)
    rd1_conn = loop.run_until_complete(rd_client_factory(loop=loop, conf=conf['redis1']))
    rd2_conn = loop.run_until_complete(rd_client_factory(loop=loop, conf=conf['redis2']))
    rgc = RedisServerCommands(rd1_conn.rd, rd2_conn.rd, conf=conf['redis2'])
    try:
        loop.run_until_complete(rgc.run_server_cmd())
    except KeyboardInterrupt as e:
        logger.error("Caught keyboard interrupt {0}\nCanceling tasks...".format(e))
    finally:
        loop.run_until_complete(rd1_conn.close_connection())
        loop.run_until_complete(rd2_conn.close_connection())
        loop.close()


if __name__ == '__main__':
    main()
