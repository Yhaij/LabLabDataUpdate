#!/user/bin/python
# -*- coding: utf-8 -*-
from rediscluster import StrictRedisCluster


def connection_redis_cluster(host='10.1.13.111', port=7000):
    """
    连接redis集群
    :param host: 
    :param port: 
    :return: 
    """
    redis_nodes = [{'host': host, 'port': port}]
    try:
        redisConn = StrictRedisCluster(startup_nodes=redis_nodes)
        print "Connect redis cluster success"
        return redisConn
    except Exception as e:
        print "Connect redis cluster fail"
        print e
        return None


def exists_key(redisConn, key):
    if redisConn.exists(key) == 1:
        return True
    else:
        return False
