import asyncio
import base64

import websockets
import json
from sortedcontainers import SortedDict
from RestAgent import RestAgent


class Subscription:
    def __init__(self, instance_name, uri, symbol, subs_type, rest_agent):
        self.__topBook = {
            'bid': 0.0,
            'ask': 0.0,
            'last': 0.0,
            'bidSize': 0.0,
            'askSize': 0.0,
            'lastSize': 0,
            'taker': '',
            'timestamp': 0,
            'ts': 0
        }
        self.__instance_name = instance_name
        self.__symbol = symbol
        self.__type = subs_type
        self.__uri = uri
        self.__bidList = SortedDict()
        self.__askList = SortedDict()
        self.__updateBuffer = []
        self.__isStarted = False
        self.__rest_agent = rest_agent
        self.init_seqNum = 0

    async def socket_consumer(self):
        my_subscription = self.__instance_name
        topic = 'orderbook_' + self.__symbol
        full_topic = self.__uri + topic + "/" + my_subscription
        async with websockets.connect(full_topic) as websocket:
            while True:
                msg = await websocket.recv()
                data = json.loads(msg)
                resp = base64.b64decode(data['payload'])
                print(resp)
                ack = json.dumps({'messageId': data['messageId']})
                await websocket.send(ack)
                msg = json.loads(resp)
                self.update(msg)

    def start(self):
        # print(self.__rest_agent.send_request(request='/orderbook/btc_usdc/', param=None, method='GET'))
        self.initSnapshot()
        asyncio.get_event_loop().run_until_complete(self.socket_consumer())

    def initSnapshot(self):
        topic = '/orderbook/' + self.__symbol + '/'
        message = self.__rest_agent.send_request(request=topic, param=None, method='GET')
        self.init_seqNum = int(message['updated_id'])
        for i in message['buy']:
            price = float(i['price'])
            vol = float(i['amount'])
            self.__bidList[price] = vol
        for i in message['sell']:
            price = float(i['price'])
            vol = float(i['amount'])
            self.__askList[price] = vol

    def update(self, message):
        if self.init_seqNum == 0:
            self.__updateBuffer.append(message)
        elif len(self.__updateBuffer) > 0:
            for i in self.__updateBuffer:
                if int(i['updated_id']) > self.init_seqNum:
                    self.updateProceed(i)
            self.__updateBuffer.clear()
        self.updateProceed(message)

        # just output first 5 levels for testing purposes
        slice = self.__askList
        n = 0
        for i in slice:
            n = n + 1
            print('Vol:{}  Px:{}'.format(slice[i], i))
            if n > 5:
                break
        print('***********************')

    def updateProceed(self, msg):
        for i in msg['buy']:
            price = float(i['price'])
            vol = float(i['amount'])
            if price in self.__bidList and vol == 0:
                del self.__bidList[price]
            if vol != 0:
                self.__bidList[price] = vol
        for i in msg['sell']:
            price = float(i['price'])
            vol = float(i['amount'])
            if price in self.__askList and vol == 0:
                del self.__askList[price]
            if vol != 0:
                self.__askList[price] = vol


if __name__ == "__main__":
    rest = RestAgent('https://api.exchange.ripio.com/api/v1',
                     'a963ae2fccf59bbaae607b1a65b3ca2d3305378b2dc59a0659a02b3b675a6513')
    trade_wrapper = Subscription('Ripio', 'wss://api.exchange.ripio.com/ws/v2/consumer/non-persistent/public/default/',
                                 'btc_usdc', 'top', rest)
    trade_wrapper.start()

    input()
