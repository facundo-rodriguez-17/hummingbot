import json
import requests

class RestAgent:

    def __init__(self, url, key):
        self.__session = requests.Session()
        self.base_url = url
        self.client_list = {}
        self.session = requests.Session()
        self.api_secret = key

    def send_request(self, request, param=None, method='POST', private=False):
        url = self.base_url + request
        headers = {}
        if private:
            headers = {
                'Content-Type': "application/json",
                'Authorization': 'Bearer {}'.format(self.api_secret)
            }

        try:
            if method == 'GET':
                response = self.session.get(url, headers=headers)
            elif method == 'DELETE':
                response = self.session.delete(url, headers=headers)
            elif method == 'POST' and param is not None:
                response = self.session.post(url, headers=headers, data=json.dumps(param))
            elif method == 'POST' and param is None:
                response = self.session.post(url, headers=headers)
            return response.json()
        except IndexError as e:
            print(e)


if __name__ == "__main__":
    settings = {
        'key': '',
        'secret': ''
    }
    rest_wrapper = RestAgent('https://api.exchange.ripio.com/api/v1', 'a963ae2fccf59bbaae607b1a65b3ca2d3305378b2dc59a0659a02b3b675a6513')
    print('Depth test:')
    resp = rest_wrapper.send_request(request='/orderbook/btc_usdc/', param=None, method='GET')
    print(resp)

    print('Ticker test:')
    resp = rest_wrapper.send_request(request='/rate/btc_usdc', param=None, method='GET')
    print(resp)

    print('Pairs test:')
    resp = rest_wrapper.send_request(request='/pair/', param=None, method='GET')
    print(resp)

    print('Balance test:')
    resp = rest_wrapper.send_request(request='/balances/exchange_balances/', param=None, method='GET', private=True)
    print(resp)

    while True:
        pass