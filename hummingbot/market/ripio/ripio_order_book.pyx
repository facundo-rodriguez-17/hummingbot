#!/usr/bin/env python

import logging
import time
from typing import (
    Any,
    Dict,
    List,
    Optional,
)
from aiokafka import ConsumerRecord
from sqlalchemy.engine import RowProxy

from hummingbot.logger import HummingbotLogger
from hummingbot.core.event.events import TradeType
from hummingbot.market.ripio.ripio_order_book_message import RipioOrderBookMessage
from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.core.data_type.order_book_message import (
    OrderBookMessage,
    OrderBookMessageType,
)

_btob_logger = None

cdef class RipioOrderBook(OrderBook):
    @classmethod
    def logger(cls) -> HummingbotLogger:
        global _btob_logger
        if _btob_logger is None:
            _btob_logger = logging.getLogger(__name__)
        return _btob_logger

    @classmethod
    def snapshot_message_from_exchange(cls,
                                       msg: Dict[str, any],
                                       timestamp: float,
                                       metadata: Optional[Dict] = None) -> OrderBookMessage:
        if metadata:
            msg.update(metadata)
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": msg["trading_pair"],
            "update_id": msg["lastUpdateId"],
            "bids": msg["bids"],
            "asks": msg["asks"]
        }, timestamp=timestamp)


    @classmethod
    def trade_message_from_exchange(cls,
                                    msg: Dict[str, Any],
                                    timestamp: Optional[float] = None,
                                    metadata: Optional[Dict] = None) -> OrderBookMessage:
        if metadata:
            msg.update(metadata)

        timestamp = msg["created_at"]
        trade_type = TradeType.SELL if msg["side"] == "SELL" else TradeType.BUY
        millis = int(round(time.time() * 1000))
        str_millis = str(millis)
        uniq_id = str_millis[- 10:]
        return OrderBookMessage(
            OrderBookMessageType.TRADE,
            {
                "trading_pair": msg["symbol"],
                "trade_type": float(trade_type.value),
                "trade_id": uniq_id,
                "update_id": timestamp,
                "price": msg["price"],
                "amount": msg["amount"],
            },
            timestamp=timestamp
        )

    @classmethod
    def diff_message_from_exchange(cls,
                                   msg: Dict[str, any],
                                   timestamp: Optional[float] = None,
                                   symbol: str = "",
                                   metadata: Optional[Dict] = None) -> OrderBookMessage:
        if metadata:
            msg.update(metadata)
        bid = [[d["price"], d["amount"]] for d in msg["buy"]]
        ask = [[d["price"], d["amount"]] for d in msg["sell"]]

        return OrderBookMessage(OrderBookMessageType.DIFF, {
            "trading_pair": symbol,
            "update_id": msg["updated_id"],
            "bids": bid,
            "asks": ask
        }, timestamp=timestamp)

    @classmethod
    def from_snapshot(cls, msg: OrderBookMessage) -> "OrderBook":
        retval = RipioOrderBook()
        retval.apply_snapshot(msg.bids, msg.asks, msg.update_id)
        return retval

    @classmethod
    def restore_from_snapshot_and_diffs(self, snapshot: OrderBookMessage, diffs: List[OrderBookMessage]):
        raise NotImplementedError("Ripio restore_from_snapshot_and_diffs NotImplemented")
