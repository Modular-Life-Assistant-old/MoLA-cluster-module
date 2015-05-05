from core import Log

from circuits.node import Node
from circuits import Component, Event, handler, Timer
import os
import socket
import json


class Module(Component):
    __network = '0.0.0.0'
    __port = 14211
    __node = None

    def add_peer(self, hostname=None, port=14211, name='', reconnect_delay=5,
                 auto_remote_event={}):
        if not name:
            name = '%s:%d' % (hostname, port)

        return self.__node.add(
            name,
            hostname,
            port,
            auto_remote_event=auto_remote_event,
            reconnect_delay=reconnect_delay
        )

    def load_configuration(self):
        config_path = '%s/configs/cluster.json' % (
            os.path.dirname(os.path.abspath(__file__))
        )

        if os.path.isfile(config_path):
            with open(config_path) as config_file:
                config = json.load(config_file)

                if 'port' in config:
                    self.__port = config['port']

                if 'network' in config:
                    self.__network = config['network']

    def load_peer(self):
        conf_path = '%s/configs/peer/' % \
                    os.path.dirname(os.path.abspath(__file__))
        nb = 0

        if os.path.isdir(conf_path):
            for name in os.listdir(conf_path):
                if not os.path.exists(conf_path + name) \
                        or name[0] == '.':
                    continue

                with open(conf_path + name) as config_file:
                    config = json.load(config_file)

                    if 'name' not in config:
                        config['name'] = name

                    self.add_peer(**config)
                    nb += 1

        Log.debug('%d peer loaded' % nb)

    def save_configuration(self):
        config_path = '%s/configs/cluster.json' % (
            os.path.dirname(os.path.abspath(__file__))
        )

        with open(config_path, 'w+') as config_file:
            config_file.write(json.dumps({
                'network': self.__network,
                'port': self.__port,
            }))

    def started(self, component):
        self.load_configuration()
        options = {}

        try:
            self.__node = Node(
                port=self.__port,
                server_ip=self.__network,
                **options
            ).register(self)

        except socket.error as e:
            Log.error('socket server error: %s' % e)
            self.__node = Node(**options).register(self)

        self.load_peer()
        self.save_configuration()

    @handler('connected_to', channel='node')
    def __client_peer_connect(self, connection_name, hostname, port,
                              client_channel, client_obj):
        Log.info('Connected to %s (%s:%d)' % (connection_name, hostname, port))

    @handler('disconnected_from', channel='node')
    def __client_peer_disconnect(self, connection_name, hostname, port,
                                 client_channel, client_obj):
        Log.info('Disconnected to %s (%s:%d)' % (
            connection_name, hostname, port
        ))

    @handler('connect', channel='node')
    def __server_peer_connect(self, sock, host, port):
        Log.info('Peer connect : %s' % host)

    @handler('disconnect', channel='node')
    def __server_peer_disconnect(self, sock):
        Log.info('Peer disconect : %s' % str(sock))

    @handler('ready', channel='node')
    def __server_ready(self, server, bind):
        Log.info('Start MoLa network : %s:%d' % bind)
