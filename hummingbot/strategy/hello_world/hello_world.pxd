# distutils: language=c++

from hummingbot.strategy.strategy_base cimport StrategyBase
from libc.stdint cimport int64_t

cdef class HelloWorldStrategy(StrategyBase):
    cdef:
        dict _market_infos
        str _asset_trading_pair
        bint _all_markets_ready
        double _status_report_interval
        int64_t _logging_options
        double _last_timestamp
        dict _last_trade_timestamps
        bint _place_order_flag
        list _market_infos_test

    cdef c_place_order(self, object market_info)
    cdef cancel_order(self, object market_pair, str order_id)
    cdef c_did_create_buy_order(self, object order_created_event)
    cdef c_did_create_sell_order(self, object order_created_event)
    cdef c_did_fill_order(self, object order_filled_event)
