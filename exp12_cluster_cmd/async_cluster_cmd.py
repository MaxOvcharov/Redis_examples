# -*- coding: utf-8 -*-
"""
    Simple example of Redis Cluster commands using async lib - aioredis
    For commands details see: http://redis.io/commands#cluster
"""
import asyncio
import os

from redis_client import rd_client_factory
from settings import BASE_DIR, logger
from utils import load_config


class RedisClusterCommands:
    def __init__(self, rd1, rd2, conf=None):
        self.rd1 = rd1
        self.rd2 = rd2
        self.rd_conf = conf

    async def run_cluster_cmd(self):
        await self.cluster_cluster_add_slots_cmd()
        await self.cluster_cluster_count_failure_reports_cmd()
        await self.cluster_cluster_count_key_in_slots_cmd()
        await self.cluster_cluster_del_slots_cmd()
        await self.cluster_cluster_forget_cmd()
        await self.cluster_cluster_get_keys_in_slots_cmd()
        await self.cluster_cluster_keyslot_cmd()
        await self.cluster_cluster_meet_cmd()

    async def cluster_cluster_add_slots_cmd(self):
        """
        This command is useful in order to modify a node's view
          of the cluster configuration. Specifically it assigns a
          set of hash slots to the node receiving the command.
          If the command is successful, the node will map the
          specified hash slots to itself, and will start
          broadcasting the new configuration.
          However note that:
          - The command only works if all the specified slots are,
            from the point of view of the node receiving the command,
            currently not assigned. A node will refuse to take
            ownership for slots that already belong to some other
            node (including itself).
          - The command fails if the same slot is specified
            multiple times.
          - As a side effect of the command execution, if a slot
            among the ones specified as argument is set as importing,
            this state gets cleared once the node assigns the
            (previously unbound) slot to itself.

        :return: None
        """
        try:
            with await self.rd1 as conn:
                res1 = await conn.cluster_add_slots(*(1, 2, 3))
        except Exception as e:
            res1 = 'HANDLE ERROR: %s' % e
        frm = "CLUSTER_CMD - 'CLUSTER ADDSLOTS': RES - {0}\n"
        logger.debug(frm.format(res1))

    async def cluster_cluster_count_failure_reports_cmd(self):
        """
        The command returns the number of failure reports for
          the specified node. Failure reports are the way
          Redis Cluster uses in order to promote a PFAIL state,
          that means a node is not reachable, to a FAIL state,
          that means that the majority of masters in the cluster
          agreed within a window of time that the node is not reachable.

        :return: None
        """
        try:
            with await self.rd1 as conn:
                res1 = await conn.cluster_count_failure_reports(node_id=1)
        except Exception as e:
            res1 = 'HANDLE ERROR: %s' % e
        frm = "CLUSTER_CMD - 'CLUSTER_COUNT_FAILURE_REPORTS': RES - {0}\n"
        logger.debug(frm.format(res1))

    async def cluster_cluster_count_key_in_slots_cmd(self):
        """
        Returns the number of keys in the specified Redis Cluster
          hash slot. The command only queries the local data set,
          so contacting a node that is not serving the specified
          hash slot will always result in a count of zero being returned.

        :return: None
        """
        try:
            with await self.rd1 as conn:
                res1 = await conn.cluster_count_key_in_slots(1)
        except Exception as e:
            res1 = 'HANDLE ERROR: %s' % e
        frm = "CLUSTER_CMD - 'CLUSTER_COUNT_KEY_IN_SLOTS': RES - {0}\n"
        logger.debug(frm.format(res1))

    async def cluster_cluster_del_slots_cmd(self):
        """
        In Redis Cluster, each node keeps track of which
          master is serving a particular hash slot.
          The DELSLOTS command asks a particular Redis
          Cluster node to forget which master is serving
          the hash slots specified as arguments.

        :return: None
        """
        try:
            with await self.rd1 as conn:
                res1 = await conn.cluster_del_slots(1, 2)
        except Exception as e:
            res1 = 'HANDLE ERROR: %s' % e
        frm = "CLUSTER_CMD - 'CLUSTER_DEL_SLOTS': RES - {0}\n"
        logger.debug(frm.format(res1))

    async def cluster_cluster_forget_cmd(self):
        """
        The command is used in order to remove a node,
          specified via its node ID, from the set of
          known nodes of the Redis Cluster node receiving
          the command. In other words the specified node
          is removed from the nodes table of the node
          receiving the command.

        :return: None
        """
        try:
            with await self.rd1 as conn:
                res1 = await conn.cluster_forget(1)
        except Exception as e:
            res1 = 'HANDLE ERROR: %s' % e
        frm = "CLUSTER_CMD - 'CLUSTER_FORGET': RES - {0}\n"
        logger.debug(frm.format(res1))

    async def cluster_cluster_get_keys_in_slots_cmd(self):
        """
        The command returns an array of keys names stored
          in the contacted node and hashing to the specified
          hash slot. The maximum number of keys to return is
          specified via the count argument, so that it is
          possible for the user of this API to batch-processing keys.

        :return: None
        """
        try:
            with await self.rd1 as conn:
                res1 = await conn.cluster_get_keys_in_slots(7000, 3, encoding='utf-8')
        except Exception as e:
            res1 = 'HANDLE ERROR: %s' % e
        frm = "CLUSTER_CMD - 'CLUSTER_GET_KEYS_IN_SLOTS': RES - {0}\n"
        logger.debug(frm.format(res1))

    async def cluster_cluster_keyslot_cmd(self):
        """
        Returns an integer identifying the hash slot
          the specified key hashes to. This command
          is mainly useful for debugging and testing,
          since it exposes via an API the underlying
          Redis implementation of the hashing algorithm.
          Example use cases for this command:
          - Client libraries may use Redis in order to
            test their own hashing algorithm, generating
            random keys and hashing them with both their
            local implementation and using Redis CLUSTER
            KEYSLOT command, then checking if the result
            is the same.
          - Humans may use this command in order to check
            what is the hash slot, and then the associated
            Redis Cluster node, responsible for a given key.

        :return: None
        """
        try:
            key = 'key1'
            with await self.rd1 as conn:
                res1 = await conn.cluster_keyslot(key)
        except Exception as e:
            res1 = 'HANDLE ERROR: %s' % e
        frm = "CLUSTER_CMD - 'CLUSTER_KEYSLOT': RES - {0}\n"
        logger.debug(frm.format(res1))

    async def cluster_cluster_meet_cmd(self):
        """
        CLUSTER MEET is used in order to connect
          different Redis nodes with cluster support
          enabled, into a working cluster.
          The basic idea is that nodes by default don't
          trust each other, and are considered unknown,
          so that it is unlikely that different cluster
          nodes will mix into a single one because of
          system administration errors or network
          addresses modifications.

        :return: None
        """
        ip, port = '127.0.0.2', 9002
        try:
            with await self.rd1 as conn:
                res1 = await conn.cluster_meet(ip, port)
        except Exception as e:
            res1 = 'HANDLE ERROR: %s' % e
        frm = "CLUSTER_CMD - 'CLUSTER_MEET': RES - {0}\n"
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
    rgc = RedisClusterCommands(rd1_conn.rd, rd2_conn.rd, conf=conf['redis2'])
    try:
        loop.run_until_complete(rgc.run_cluster_cmd())
    except KeyboardInterrupt as e:
        logger.error("Caught keyboard interrupt {0}\nCanceling tasks...".format(e))
    finally:
        loop.run_until_complete(rd1_conn.close_connection())
        loop.run_until_complete(rd2_conn.close_connection())
        loop.close()


if __name__ == '__main__':
    main()
