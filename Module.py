import json
from core import Log

from circuits.node import remote, Node
from circuits import Component, Event, handler, Timer
import os
import socket
import subprocess


class connect_to(Event):
    """ client connected
    """
    def __init__(self, client_info):
        super(connect_to, self).__init__(client_info)


class disconnect_to(connect_to):
    """ client disconnected
    """


class client_firewall_blocking(Event):
    """ Client's firewall has blocked an event
    """
    def __init__(self, reason, event, client_name, channel):
        super(server_firewall_blocking, self).__init__(reason, str(event), client_name, channel)


class server_firewall_blocking(Event):
    """ Server's firewall has blocked an event
    """
    def __init__(self, reason, event, ip):
        super(server_firewall_blocking, self).__init__(reason, str(event), ip)


class Module(Component):
    __network = '0.0.0.0'
    __port = 14211
    __node = None
    __clients = {}
    __default_server_event_allowned = {}

    def add_peer(self, hostname=None, port=14211, name='', event_allowed={}):
        if not name:
            name = '%s:%d' % (hostname, port)

        client_channel = self.__node.add(name, hostname, port)

        self.__clients[name] = {
            'hostname': hostname,
            'port': port,
            'name': name,
            'channel': client_channel,
            'event_allowed': event_allowed,
            'client': self.__node.nodes[name],
            'connected': False,
        }

        # bridge event
        for event_name in event_allowed:
            for channel in event_allowed[event_name]:
                @handler(event_name, channel=channel)
                def event_handle(self, event, *args, **kwargs):
                    yield(yield self.call(remote(event, name)))
                self.addHandler(event_handle)

        @handler('connected', channel=client_channel)
        def connected(self, host, port):
            self.__clients[name]['connected'] = True
            Log.debug('Connected to %s:%d' % (host, port))
            self.fire(connect_to(self.__clients[name]))
        self.addHandler(connected)

        @handler('disconnected', channel=client_channel)
        def disconnected(self):
            self.__clients[name]['connected'] = False
            Log.debug('disconnected to %s' % name)
            self.fire(disconnect_to(self.__clients[name]))
        self.addHandler(disconnected)

        return self.__clients[name]

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

        # server event allowned
        config_path = '%s/configs/server/default.json' % (
            os.path.dirname(os.path.abspath(__file__))
        )

        if os.path.isfile(config_path):
            with open(config_path) as config_file:
                self.__default_server_event_allowned = json.load(config_file)

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
            reason = 'client "%s" unknown' % client_name
            Log.error(reason)
            self.fire(client_firewall_blocking(
                reason,
                event,
                client_name,
                channel
            ))
            return False

        event_allowed = self.__clients[client_name]['event_allowed']

        for e in event.args:
            if isinstance(e, str):
                continue

            reason = self.__event_is_allow(e, client_name, event_allowed)

            if reason:
                Log.error(reason)
                self.fire(client_firewall_blocking(
                    reason,
                    event,
                    client_name,
                    channel
                ))
                return False

        return True

    def __event_is_allow(self, event, client, event_allowed):
        if not event.name in event_allowed and \
                not '*' in event_allowed:
            return 'event "%s" not allow for %s client' % (
                event.name,
                client
            )

        if not [i for i in event.channels if i in event_allowed[e.name]] and \
                not '*' in event_allowed[e.name]:
            return 'channel %s (event "%s") not allow for %s client' % (
                str(event.channels),
                event.name,
                client
            )

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
        reason = 'unknow (%s, %s)' % (str(event), str(sock)) # event is unallow by default
        try:
            ip = sock.getpeername()[0]
        except:
            Log.warning('peer %s not connected' % str(sock))
            return False

        # default peer (unknown)
        event_allowed = self.__default_server_event_allowned

        reason = self.__event_is_allow(event, ip, event_allowed)

        if reason:
            Log.error(reason)
            self.fire(server_firewall_blocking(reason, event, ip))
            return False

        # add infos in event 
        event.cluster_sock = sock

        # convert byte to str
        args = []
        for arg in event.args:
            args.append(arg.decode('utf-8') if type(arg) == 'byte' else arg)
        event.args = args

        for i in event.kwargs:
            v = event.kwargs[i]
            index = i.decode('utf-8') if type(i) == 'byte' else i
            value = v.decode('utf-8') if type(v) == 'byte' else v

            del(event.kwargs[i])
            event.kwargs[index] = value

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

