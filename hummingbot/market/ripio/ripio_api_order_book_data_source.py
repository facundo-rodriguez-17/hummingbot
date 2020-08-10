#!/usr/bin/env python
# ---------------
import asyncio
import base64
import aiohttp
import logging
import pandas as pd
from typing import (
    Any,
    AsyncIterable,
    Dict,
    List,
    Optional
)
import re
import time
import ujson
import websockets
from websockets.exceptions import ConnectionClosed

from hummingbot.core.utils import async_ttl_cache
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.core.data_type.order_book_row import OrderBookRow
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.order_book_tracker_entry import OrderBookTrackerEntry
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.logger import HummingbotLogger
from hummingbot.market.ripio.ripio_order_book import RipioOrderBook

TRADING_PAIR_FILTER = re.compile(r"(BTC|ETH|USDC)$")

SNAPSHOT_REST_URL = "https://api.exchange.ripio.com/api/v1/orderbook"
DIFF_STREAM_URL = "wss://api.exchange.ripio.com/ws/v2/consumer/non-persistent/public/default/"
TICKER_PRICE_CHANGE_URL = "https://api.exchange.ripio.com/api/v1/rate/all/"
EXCHANGE_INFO_URL = "https://api.exchange.ripio.com/api/v1/pair/"


class RipioAPIOrderBookDataSource(OrderBookTrackerDataSource):
    MESSAGE_TIMEOUT = 3000.0

    _baobds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._baobds_logger is None:
            cls._baobds_logger = logging.getLogger(__name__)
        return cls._baobds_logger

    def __init__(self, trading_pairs: Optional[List[str]] = None):
        super().__init__()
        self._trading_pairs: Optional[List[str]] = trading_pairs
        self._order_book_create_function = lambda: OrderBook()

    @classmethod
    @async_ttl_cache(ttl=60 * 30, maxsize=1)
    async def get_active_exchange_markets(cls) -> pd.DataFrame:
        """
        Returned data frame should have trading_pair as index and include usd volume, baseAsset and quoteAsset
        """
        async with aiohttp.ClientSession() as client:

            market_response, exchange_response = await safe_gather(
                client.get(TICKER_PRICE_CHANGE_URL),
                client.get(EXCHANGE_INFO_URL)
            )
            market_response: aiohttp.ClientResponse = market_response
            exchange_response: aiohttp.ClientResponse = exchange_response

            if market_response.status != 200:
                raise IOError(f"Error fetching Ripio markets information. "
                              f"HTTP status is {market_response.status}.")
            if exchange_response.status != 200:
                raise IOError(f"Error fetching Ripio exchange information. "
                              f"HTTP status is {exchange_response.status}.")

            market_data = await market_response.json()
            exchange_data = await exchange_response.json()

            trading_pairs: Dict[str, Any] = {item["symbol"]: {k: item[k] for k in ["base", "quote"]}
                                             for item in exchange_data["results"]
                                             if item["enabled"]}
            market_data: List[Dict[str, Any]] = [{**item, **trading_pairs[item["pair"]]}
                                                 for item in market_data
                                                 if item["pair"]]
            for item in market_data:
                item["symbol"] = item.pop("pair")

            # Build the data frame.
            all_markets: pd.DataFrame = pd.DataFrame.from_records(data=market_data, index="symbol")
            btc_price: float = float(all_markets.loc["BTC_USDC"].last_price)
            eth_price: float = float(all_markets.loc["ETH_USDC"].last_price)
            usd_volume: float = [
                (
                    volume * btc_price if trading_pair.endswith("BTC") else
                    volume * eth_price if trading_pair.endswith("ETH") else
                    volume
                )
                for trading_pair, volume in zip(all_markets.index,
                                                all_markets.volume.astype("float"))]
            all_markets.loc[:, "USDVolume"] = usd_volume
            all_markets.loc[:, "volume"] = all_markets.volume
            return all_markets.sort_values("USDVolume", ascending=False)

    async def get_trading_pairs(self) -> List[str]:
        if not self._trading_pairs:
            try:
                active_markets: pd.DataFrame = await self.get_active_exchange_markets()
                self._trading_pairs = active_markets.index.tolist()
            except Exception:
                self._trading_pairs = []
                self.logger().network(
                    f"Error getting active exchange information.",
                    exc_info=True,
                    app_warning_msg=f"Error getting active exchange information. Check network connection."
                )
        return self._trading_pairs

    @staticmethod
    async def get_snapshot(client: aiohttp.ClientSession, trading_pair: str, limit: int = 1000) -> Dict[str, Any]:
        url_path = "{0}/{1}".format(SNAPSHOT_REST_URL, trading_pair)
        async with client.get(url_path) as response:
            response: aiohttp.ClientResponse = response
            if response.status != 200:
                raise IOError(f"Error fetching Ripio market snapshot for {trading_pair}. "
                              f"HTTP status is {response.status}.")
            data: Dict[str, Any] = await response.json()
            bid = [[d["price"], d["amount"]] for d in data["buy"]]
            ask = [[d["price"], d["amount"]] for d in data["sell"]]
            bids = [OrderBookRow(i[0], i[1], data["updated_id"]) for i in bid]
            asks = [OrderBookRow(i[0], i[1], data["updated_id"]) for i in ask]

            return {
                "symbol": trading_pair,
                "bids": bids,
                "asks": asks,
                "lastUpdateId": data["updated_id"]
            }

    async def _get_response(self, ws: websockets.WebSocketClientProtocol) -> AsyncIterable[str]:
        try:
            while True:
                try:
                    msg: str = await asyncio.wait_for(ws.recv(), timeout=self.MESSAGE_TIMEOUT)
                    yield msg
                except asyncio.TimeoutError:
                    raise
        except asyncio.TimeoutError:
            self.logger().warning("WebSocket ping timed out. Going to reconnect...")
            return
        except ConnectionClosed:
            return
        finally:
            await ws.close()

    async def get_tracking_pairs(self) -> Dict[str, OrderBookTrackerEntry]:
        # Get the currently active markets
        async with aiohttp.ClientSession() as client:
            trading_pairs: List[str] = await self.get_trading_pairs()
            retval: Dict[str, OrderBookTrackerEntry] = {}

            number_of_pairs: int = len(trading_pairs)
            for index, trading_pair in enumerate(trading_pairs):
                try:
                    snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair, 1000)
                    snapshot_timestamp: float = time.time()
                    snapshot_msg: OrderBookMessage = RipioOrderBook.snapshot_message_from_exchange(
                        snapshot,
                        snapshot_timestamp,
                        metadata={"trading_pair": trading_pair}
                    )
                    order_book: OrderBook = self.order_book_create_function()
                    order_book.apply_snapshot(snapshot_msg.bids, snapshot_msg.asks, snapshot_msg.update_id)
                    retval[trading_pair] = OrderBookTrackerEntry(trading_pair, snapshot_timestamp, order_book)
                    self.logger().info(f"Initialized order book for {trading_pair}. "
                                       f"{index+1}/{number_of_pairs} completed.")
                    await asyncio.sleep(1.0)
                except Exception:
                    self.logger().error(f"Error getting snapshot for {trading_pair}. ", exc_info=True)
                    await asyncio.sleep(5)
            return retval

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        trading_pairs: List[str] = await self.get_trading_pairs()
        tasks = [
            ev_loop.create_task(self._listen_trades_for_pair(pair, output))
            for pair in trading_pairs
        ]
        await asyncio.gather(*tasks)

    async def _listen_trades_for_pair(self, pair: str, output: asyncio.Queue):
        while True:
            try:
                ws_uri = DIFF_STREAM_URL + 'trades_' + pair.lower() + '/hummingbot_ripio'
                async with websockets.connect(ws_uri) as ws:
                    async for raw_msg in self._get_response(ws):
                        data = ujson.loads(raw_msg)
                        resp = base64.b64decode(data['payload'])
                        ack = ujson.dumps({'messageId': data['messageId']})
                        await ws.send(ack)
                        msg = ujson.loads(resp)
                        if msg:
                            msg_book: OrderBookMessage = RipioOrderBook.trade_message_from_exchange(
                                msg,
                                metadata={"symbol": f"{pair}"}
                            )
                            output.put_nowait(msg_book)
            except asyncio.CancelledError:
                raise
            except Exception as err:
                self.logger().error(f"listen trades for pair {pair}", err)
                self.logger().error(
                    "Unexpected error with WebSocket connection. "
                    f"Retrying after {int(self.MESSAGE_TIMEOUT)} seconds...",
                    exc_info=True)
                await asyncio.sleep(self.MESSAGE_TIMEOUT)

    async def listen_for_order_book_diffs(self,
                                          ev_loop: asyncio.BaseEventLoop,
                                          output: asyncio.Queue):
        trading_pairs: List[str] = await self.get_trading_pairs()
        tasks = [
            self._listen_order_book_for_pair(pair, output)
            for pair in trading_pairs
        ]

        await asyncio.gather(*tasks)

    async def _listen_order_book_for_pair(self, pair: str, output: asyncio.Queue = None):
        while True:
            try:
                ws_uri = DIFF_STREAM_URL + 'orderbook_' + pair.lower() + '/hummingbot_ripio'
                async with websockets.connect(ws_uri) as ws:
                    async for raw_msg in self._get_response(ws):
                        msg_base = ujson.loads(raw_msg)
                        ack = ujson.dumps({'messageId': msg_base['messageId']})
                        await ws.send(ack)
                        resp = base64.b64decode(msg_base['payload'])
                        msg = ujson.loads(resp)
                        order_book_message: OrderBookMessage = RipioOrderBook.diff_message_from_exchange(
                            msg, time.time(), pair)
                        output.put_nowait(order_book_message)
            except asyncio.CancelledError:
                raise
            except Exception as err:
                self.logger().error(err)
                self.logger().network(
                    f"Unexpected error with WebSocket connection.",
                    exc_info=True,
                    app_warning_msg="Unexpected error with WebSocket connection. "
                                    f"Retrying in {int(self.MESSAGE_TIMEOUT)} seconds. "
                                    "Check network connection."
                )
                await asyncio.sleep(self.MESSAGE_TIMEOUT)

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                trading_pairs: List[str] = await self.get_trading_pairs()
                async with aiohttp.ClientSession() as client:
                    for trading_pair in trading_pairs:
                        try:
                            snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair)
                            snapshot_timestamp: float = time.time()
                            snapshot_msg: OrderBookMessage = RipioOrderBook.snapshot_message_from_exchange(
                                snapshot,
                                snapshot_timestamp,
                                metadata={"trading_pair": trading_pair}
                            )
                            output.put_nowait(snapshot_msg)
                            self.logger().debug(f"Saved order book snapshot for {trading_pair}")
                            await asyncio.sleep(5.0)
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            self.logger().error("Unexpected error.", exc_info=True)
                            await asyncio.sleep(5.0)
                    this_hour: pd.Timestamp = pd.Timestamp.utcnow().replace(minute=0, second=0, microsecond=0)
                    next_hour: pd.Timestamp = this_hour + pd.Timedelta(hours=1)
                    delta: float = next_hour.timestamp() - time.time()
                    await asyncio.sleep(delta)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error.", exc_info=True)
                await asyncio.sleep(5.0)
