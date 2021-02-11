import time
import requests
from RestAgent import RestAgent


class RipioWrapper:

    def __init__(self, connector_name, instance_name, settings):
        self.__session = requests.Session()
        self.__channel = None
        self.__instance_name = instance_name
        self.connector_name = connector_name
        self.client_list = {}
        self.__isConnected = False
        self.__isStarted = False
        self.session = requests.Session()
        self.base_url = settings['url']
        self.api_secret = settings['key']
        self.rest_agent = RestAgent(self.base_url, self.api_secret)
    def __del__(self):

        self.stop()

    def name(self):
        return self.__instance_name

    def is_connected(self):
        return self.__isConnected

    def is_started(self):
        return self.__isStarted

    def get_all_tickers(self):
        request = '/rate/all/'
        try:
            req = self.send_request(request=request, method='GET')
            return req
        except IndexError as e:
            print(e)

    def get_ticker(self, symbol):
        request = '/rate/{}/'.format(symbol)
        try:
            req = self.send_request(request=request, method='GET')
            return req
        except IndexError as e:
            print(e)

    def get_depth(self, symbol):
        request = '/orderbook/{}/'.format(symbol)
        try:
            req = self.send_request(request=request, method='GET')
            return req
        except IndexError as e:
            print(e)

    def get_pairs(self):
        request = '/pair/'
        try:
            req = self.send_request(request=request, method='GET')
            return req
        except IndexError as e:
            print(e)

    def get_balance(self):
        request = '/balances/exchange_balances/'
        try:
            req = self.send_request(request=request, method='GET', private=True)
            return req
        except IndexError as e:
            print(e)

    def send_order(self, order):
        request = '/order/{}/'.format(order['symbol'])
        new_order = {
            'order_type': 'LIMIT',
            'amount': order['amount'],
            'limit_price': order['limit_price'],
            'side': order['side'],
        }
        try:
            req = self.send_request(request=request, param=new_order, method='POST', private=True)
            return req
        except IndexError as e:
            print(e)

    def cancel_order(self, order):
        request = '/order/{}/{}/cancel/'.format(order['symbol'], order['order_id'])
        try:
            req = self.send_request(request=request, method='POST', private=True)
            return req
        except IndexError as e:
            print(e)

    def get_orders(self, symbol):
        request = '/order/{}/'.format(symbol)
        try:
            req = self.send_request(request=request, method='GET', private=True)
            return req
        except IndexError as e:
            print(e)

    def get_trade_history(self, symbol):
        request = '/tradehistory/{}/'.format(symbol)
        try:
            req = self.send_request(request=request, method='GET', private=True)
            return req
        except IndexError as e:
            print(e)

    def send_request(self, request, param=None, method='POST', private=False):
        return self.rest_agent.send_request(request, param=param, method=method, private=private)


if __name__ == "__main__":
    settings = {
        'key': 'a963ae2fccf59bbaae607b1a65b3ca2d3305378b2dc59a0659a02b3b675a6513',
        'url': 'https://api.exchange.ripio.com/api/v1'
    }
    trade_wrapper = RipioWrapper('Ripio', 'Ripio', settings)
    # trade_wrapper.get_active_order()

    print('Get orders test:')
    resp = trade_wrapper.get_orders('btc_usdc')
    print(resp)

    print('Depth test:')
    resp = trade_wrapper.get_depth('btc_usdc')
    print(resp)

    print('Ticker test:')
    resp = trade_wrapper.get_ticker('btc_usdc')
    print(resp)

    print('All tickers test:')
    resp = trade_wrapper.get_all_tickers()
    print(resp)

    print('Pairs test:')
    resp = trade_wrapper.get_pairs()
    print(resp)

    print('Balance test:')
    resp = trade_wrapper.get_balance()
    print(resp)

    print('Trade history test:')
    resp = trade_wrapper.get_trade_history('btc_usdc')
    print(resp)

    print('New order test:')
    order = {
        'symbol': 'btc_usdc',
        'amount': 0.0012,
        'limit_price': 9000,
        'side': 'BUY'
    }
    resp = trade_wrapper.send_order(order)
    print(resp)

    time.sleep(5)

    print('Cancel order test:')

    order = {
        'symbol': 'btc_usdc',
        'order_id': resp['order_id']
    }
    resp = trade_wrapper.cancel_order(order)
    print(resp)

    while True:
        pass
