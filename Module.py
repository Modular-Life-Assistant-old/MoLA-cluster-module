import json
from core import Log

from circuits.node import remote, Node
from circuits import Component, Event, handler, Timer
import os
import socket
import subprocess


class cluster_client(Event):
    """ parent class
    """
    def __init__(self, **kwarg):
        super(load_alert, self).__init__(**kwarg)


class connect_to(Event):
    """ client connected
    """


class disconnect_to(Event):
    """ client disconnected
    """


class Module(Component):
    __network = '0.0.0.0'
    __port = 14211
    __node = None
    __clients = {}

    def add_peer(self, hostname=None, port=14211, name='', allow_event={}):
        if not name:
            name = '%s:%d' % (hostname, port)

        client_channel = self.__node.add(name, hostname, port)

        self.__clients[name] = {
            'hostname': hostname,
            'port': port,
            'name': name,
            'channel': client_channel,
            'allow_event': allow_event,
            'client': self.__node.nodes[name],
            'connected': False,
        }

        # bridge event
        for event_name in allow_event:
            for channel in allow_event[event_name]:
                @handler(event_name, channel=channel)
                def event_handle(self, event, *args, **kwargs):
                    yield(yield self.call(remote(event, name)))
                self.addHandler(event_handle)

        @handler('connected', channel=client_channel)
        def connected(self, host, port):
            self.__clients[name]['connected'] = True
            Log.debug('Connected to %s:%d' % (host, port))
            self.fire(connect_to(**self.__clients[name]))
        self.addHandler(connected)

        @handler('disconnected', channel=client_channel)
        def disconnected(self):
            self.__clients[name]['connected'] = False
            Log.debug('disconnected to %s' % name)
            self.fire(disconnect_to(**self.__clients[name]))
        self.addHandler(disconnected)

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
            for hostname in os.listdir(conf_path):
                if not os.path.exists(conf_path + hostname) \
                        or hostname[0] == '.':
                    continue

                with open(conf_path + hostname) as config_file:
                    self.add_peer(**json.load(config_file))
                    nb += 1

        Log.debug('%d peer load' % nb)

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
        # options = self.__get_cert_param()
        options['server_event_firewall'] = self.__server_event_is_allow
        options['client_event_firewall'] = self.__client_event_is_allow

        try:
            self.__node = Node(
                (self.__network, self.__port),
                **options
            ).register(self)

        except socket.error as e:
            Log.error('socket server error: %s' % e)
            self.__node = Node(**options).register(self)

        self.load_peer()
        Timer(60, Event.create('update_peer'), persist=True).register(self)
        self.save_configuration()

    def update_peer(self):
        for name in self.__clients:
            if not self.__clients[name]['connected']:
                self.fire(
                    Event.create(
                        'connect',
                        self.__clients[name]['hostname'],
                        self.__clients[name]['port']
                    ),
                    self.__clients[name]['channel']
                )

    def __client_event_is_allow(self, event, client_name, channel):
        if not client_name in self.__clients:
            Log.error('client "%s" unknown' % client_name)
            return False

        allow_event = self.__clients[client_name]['allow_event']

        for e in event.args:
            if isinstance(e, str):
                continue

            if not e.name in allow_event:
                Log.error('event "%s" not allow for %s client' % (
                    e.name,
                    client_name
                ))
                return False

            if not channel in allow_event[e.name] and \
                    not '*' in allow_event[e.name]:
                Log.error('channel %s (event "%s") not allow for %s client' % (
                    channel,
                    e.name,
                    client_name
                ))
                return False

        return True

    @handler('connect', channel='node')
    def __server_peer_connect(self, sock, host, port):
        Log.debug('Peer connect : %s' % host)

    @handler('disconnect', channel='node')
    def __server_peer_disconnect(self, sock):
        Log.debug('Peer disconect : %s' % str(sock))

    @handler('ready', channel='node')
    def __server_ready(self, server, bind):
        Log.debug('Start MoLa network : %s:%d' % bind)

    def __server_event_is_allow(self, event, sock):
        # TODO

        event.cluster_sock = sock
        return True

    def __get_cert_param(self):
        cert_dir_path = '%s/cert/' % \
                        os.path.dirname(os.path.abspath(__file__))

        certfile_path = '%sserver.crt' % cert_dir_path
        keyfile_path = '%sserver.key' % cert_dir_path

        if not os.path.isdir(cert_dir_path):
            Log.info('Create certificate')
            os.mkdir(cert_dir_path)

            Log.debug(subprocess.Popen(
                'openssl genrsa -out %s 2048 &&'
                'openssl req -new -x509 -key %s -out %s -days 3650 -subj /CN=MoLA' % \
                    (keyfile_path, keyfile_path, certfile_path),
                shell=True,
            ))

        return {
            'secure': True,
            'certfile': certfile_path,
            'keyfile': keyfile_path,
            #'cert_reqs': CERT_NONE,
            #'ssl_version': PROTOCOL_SSLv23,
            #'ca_certs': None,
        }

