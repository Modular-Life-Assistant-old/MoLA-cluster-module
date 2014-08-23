from core import EventManager
from core import Log
from .Network import Network

import asyncio
import json
import os

class Module:
    def __init__(self):
        EventManager.bind('network_connect', self.__event_network_connect)
        self.__transport_list = []

    def load_configuration(self):
        config = {}
        config_path = '%s/configs/network.json' % (
            os.path.dirname(os.path.abspath(__file__))
        )

        if os.path.isfile(config_path):
            with open(config_path) as config_file:
                config = json.load(config_file)

        self.__port = config['port'] if 'port' in config else 14211

    def send_all(self, data):
        data = data.encode()
        for connection in self.__transport_list:
            connection['transport'].write(data)

    def start(self):
        loop = asyncio.get_event_loop()
        self.__server = loop.create_server(
            Network,
            '0.0.0.0',
            self.__port,
        )
        Log.debug('Start network on port %d' % self.__port)
        loop.run_until_complete(self.__server)

    def __event_network_connect(self, data):
        self.__transport_list.append({
            'client': data['client'],
            'transport': data['transport'],
        })

