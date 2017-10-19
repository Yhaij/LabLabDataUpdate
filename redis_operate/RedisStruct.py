#!/user/bin/python
# -*- coding: utf-8 -*-
from rediscluster import StrictRedisCluster


class RedisHash(object):
    """
    redis中一个hash对应的类
    """
    def __init__(self, conn, tableName):
        self.__redis = conn
        self.__name = tableName

    def get_redisConn(self):
        return self.__redis

    def set_redisConn(self, conn):
        self.__redis = conn

    def get_table_name(self):
        return self.__name

    def set_table_name(self, tableName):
        self.__name = tableName

    def get(self, key):
        """
        根据key 得到 map
        :param key: 
        :return: 
        """
        return self.__redis.hget(key)

    def set(self, key, value, overWrite=True):
        """
        存放key map
        :param key: 
        :param value: 
        :param overWrite: 
        :return: 
        """
        if overWrite:
            self.__redis.hset(self.__name, key, value)
            return key, value
        else:
            if self.exists_key(key):
                return key, self.get(key)

    def exists_key(self, key):
        """
        是否存在key,如果是返回true,否则返回false
        :param key: 
        :return: 
        """
        if self.__redis.hexists(self.__name, key) == 1:
            return True
        else:
            return False


if __name__ == '__main__':
    redis_nodes = [{'host': '10.1.13.111', 'port': 7000}]
    r = StrictRedisCluster(startup_nodes=redis_nodes)
    print r.hget()