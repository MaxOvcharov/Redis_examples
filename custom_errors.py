# -*- coding: utf-8 -*-


class RedisConnectionLost(Exception):
    """ Error caused with bad Redis connection """
    def __init__(self, *args, **kwargs):
        pass

