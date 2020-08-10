# distutils: language=c++
from decimal import Decimal
from libc.stdint cimport int64_t
import logging
from typing import (
    List,
    Tuple,
    Dict
)

from hummingbot.core.clock cimport Clock
from hummingbot.logger import HummingbotLogger
from hummingbot.core.data_type.limit_order cimport LimitOrder
from hummingbot.core.data_type.limit_order import LimitOrder
from libc.stdint cimport int64_t
from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.market.market_base import MarketBase
from hummingbot.market.market_base cimport MarketBase
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.strategy_base import StrategyBase


NaN = float("nan")
s_decimal_zero = Decimal(0)
ds_logger = None


cdef class HelloWorldStrategy(StrategyBase):
    OPTION_LOG_NULL_ORDER_SIZE = 1 << 0
    OPTION_LOG_REMOVING_ORDER = 1 << 1
    OPTION_LOG_ADJUST_ORDER = 1 << 2
    OPTION_LOG_CREATE_ORDER = 1 << 3
    OPTION_LOG_MAKER_ORDER_FILLED = 1 << 4
    OPTION_LOG_STATUS_REPORT = 1 << 5
    OPTION_LOG_MAKER_ORDER_HEDGED = 1 << 6
    OPTION_LOG_ALL = 0x7fffffffffffffff
    CANCEL_EXPIRY_DURATION = 60.0

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global ds_logger
        if ds_logger is None:
            ds_logger = logging.getLogger(__name__)
        return ds_logger

    def __init__(self,
                 market_infos: List[MarketTradingPairTuple],
                 asset_trading_pair: str,
                 logging_options: int = OPTION_LOG_ALL,
                 status_report_interval: float = 900):

        if len(market_infos) < 1:
            raise ValueError(f"market_infos must not be empty.")

        super().__init__()
        self._market_infos = {
            (market_info.market, market_info.trading_pair): market_info
            for market_info in market_infos
        }

        self._asset_trading_pair = asset_trading_pair
        self._all_markets_ready = False
        self._logging_options = logging_options
        self._status_report_interval = status_report_interval
        self._last_timestamp = 0
        self._last_trade_timestamps = {}

        cdef:
            set all_markets = set([market_info.market for market_info in market_infos])

        self.c_add_markets(list(all_markets))

    cdef c_tick(self, double timestamp):
        """
        Clock tick entry point.

        For the simple trade strategy, this function simply checks for the readiness and connection status of markets, and
        then delegates the processing of each market info to c_process_market().

        :param timestamp: current tick timestamp
        """
        StrategyBase.c_tick(self, timestamp)
        cdef:
            int64_t current_tick = <int64_t>(timestamp // self._status_report_interval)
            int64_t last_tick = <int64_t>(self._last_timestamp // self._status_report_interval)
            bint should_report_warnings = ((current_tick > last_tick) and
                                           (self._logging_options & self.OPTION_LOG_STATUS_REPORT))

        try:                       
            for market_info in self._market_infos:
                data=market_info[0].get_all_balances()
                for key in data:
                    self.logger().info(f"{key} balance: {data[key]}") 
                pair = market_info[1]
                order_book = market_info[0].get_order_book(pair.replace("-", "_"))
                snapshot = order_book.snapshot
                asks = snapshot[0]
                bids = snapshot[1]
                ask_len = len(asks)
                bid_len = len(bids)
                self.logger().info(f"bids count:{bid_len}    ask count:{ask_len}")
                self.logger().info(f"bid:{market_info[0].get_price(pair, False)}    ask:{market_info[0].get_price(pair, True)}")
        except Exception as e:
            #self.logger().info(str(e))
            self.logger().error("Market book is not ready")
        finally:
            self._last_timestamp = timestamp

    @property
    def active_bids(self) -> List[Tuple[MarketBase, LimitOrder]]:
        return self._sb_order_tracker.active_bids

    @property
    def active_asks(self) -> List[Tuple[MarketBase, LimitOrder]]:
        return self._sb_order_tracker.active_asks

    @property
    def active_limit_orders(self) -> List[Tuple[MarketBase, LimitOrder]]:
        return self._sb_order_tracker.active_limit_orders

    @property
    def in_flight_cancels(self) -> Dict[str, float]:
        return self._sb_order_tracker.in_flight_cancels

    @property
    def market_info_to_active_orders(self) -> Dict[MarketTradingPairTuple, List[LimitOrder]]:
        return self._sb_order_tracker.market_pair_to_active_orders

    @property
    def logging_options(self) -> int:
        return self._logging_options

    @logging_options.setter
    def logging_options(self, int64_t logging_options):
        self._logging_options = logging_options

    @property
    def place_orders(self):
        return self._place_orders

    def format_status(self) -> str:
        cdef:
            list lines = []
            list warning_lines = []

        for market_info in self._market_infos.values():
            active_orders = self.market_info_to_active_orders.get(market_info, [])

            warning_lines.extend(self.network_warning([market_info]))

            lines.extend(["", "  Assets:"] + ["    " + str(self._asset_trading_pair) + "    " +
                                              str(market_info.market.get_balance(self._asset_trading_pair))])

            warning_lines.extend(self.balance_warning([market_info]))

        if len(warning_lines) > 0:
            lines.extend(["", "*** WARNINGS ***"] + warning_lines)

        return "\n".join(lines)

    cdef c_start(self, Clock clock, double timestamp):
        StrategyBase.c_start(self, clock, timestamp)
        
