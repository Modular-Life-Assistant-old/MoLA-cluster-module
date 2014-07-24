from core import EventManager
from core import Log

import asyncio

class Network(asyncio.Protocol):
    def connection_made(self, transport):
        self.__transport = transport
        peer = transport.get_extra_info('peername')

        try:
            self.__peer = "%s:%d" % (peer[0], peer[1])

        except:
            ## eg Unix Domain sockets don't have host/port
            self.__peer = str(peer)

        Log.debug('Network: client connect (%s)' % self.__peer)
        EventManager.trigger(
            'network_connect',
            transport=transport,
            peer=self.__peer,
        )

    def connection_lost(self, exc):
        Log.debug('Network: client disconect (%s)' % self.__peer)
        EventManager.trigger('network_disconect')

    def data_received(self, data):
        Log.debug('Network: receve (%s)' % self.__peer)
        self.__transport.write(data)

