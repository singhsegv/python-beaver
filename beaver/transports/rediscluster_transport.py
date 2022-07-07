# -*- coding: utf-8 -*-
from rediscluster import RedisCluster

from beaver.transports.base_transport import BaseTransport
from beaver.transports.exception import TransportException


class RedisclusterTransport(BaseTransport):
    LIST_DATA_TYPE = 'list'
    CHANNEL_DATA_TYPE = 'channel'

    def __init__(self, beaver_config, logger=None):
        super(RedisclusterTransport, self).__init__(
            beaver_config, logger=logger)

        self._cluster_host = beaver_config.get('redis_cluster_host')
        self._cluster_port = beaver_config.get('redis_cluster_port')

        self._cluster = RedisCluster(host=self._cluster_host, port=self._cluster_port, decode_responses=True)

        self._namespace = beaver_config.get('redis_namespace')

        self._data_type = beaver_config.get('redis_data_type')
        if self._data_type not in [self.LIST_DATA_TYPE,
                                   self.CHANNEL_DATA_TYPE]:
            raise TransportException('Unknown Redis data type')

    def _is_reachable(self):
        """Checks if the given redis cluster is reachable"""

        try:
            self._cluster.ping()
            return True
        except Exception:
            self._logger.warn('Cannot reach redis cluster: ' + self._cluster_url)

        return False

    def reconnect(self):
        if self._is_reachable():
            self._cluster = RedisCluster(host=self._cluster_host, port=self._cluster_port, decode_responses=True)

    def invalidate(self):
        """Invalidates the current transport and disconnects all redis cluster connection"""

        super(RedisclusterTransport, self).invalidate()
        self._cluster.connection_pool.disconnect()
        return False

    def callback(self, filename, lines, **kwargs):
        """Sends log lines to redis cluster"""

        self._logger.debug('Redis cluster transport called')

        timestamp = self.get_timestamp(**kwargs)
        if kwargs.get('timestamp', False):
            del kwargs['timestamp']

        namespaces = self._beaver_config.get_field('redis_namespace', filename)
        if not namespaces:
            namespaces = self._namespace
        namespaces = namespaces.split(",")

        self._logger.debug('Got namespaces: '.join(namespaces))

        data_type = self._data_type
        self._logger.debug('Got data type: ' + data_type)

        pipeline = self._cluster.pipeline()

        callback_map = {
            self.LIST_DATA_TYPE: pipeline.rpush,
            self.CHANNEL_DATA_TYPE: pipeline.publish,
        }
        callback_method = callback_map[data_type]

        for line in lines:
            for namespace in namespaces:
                callback_method(
                    namespace.strip(),
                    self.format(filename, line, timestamp, **kwargs)
                )

        try:
            pipeline.execute()
        except Exception as e:
            self._logger.warn(
                'Cannot push lines to redis server: ' + self._cluster_url)
            raise TransportException(e)

    def valid(self):
        """Returns whether or not the transport can send data to redis cluster"""

        return self._is_reachable()

