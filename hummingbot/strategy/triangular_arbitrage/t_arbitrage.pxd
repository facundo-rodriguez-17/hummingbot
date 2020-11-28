# distutils: language=c++

from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.strategy.strategy_base cimport StrategyBase
from libc.stdint cimport int64_t

cdef class TriangularArbitrageStrategy(StrategyBase):
    cdef:
        object _market_pairs
        str _target_asset
        list _assetList
        double _min_profitability
        double _order_amount
        bint _all_markets_ready
        double _status_report_interval
        double _last_timestamp
        double _next_trade_delay
        int64_t _logging_options
        double _last_trade_timestamps        
        bint _cool_off_logged
        bint _hb_app_notification 
        dict _av_balance_dict
        dict _full_balance_dict  
        bint _order_counter
        double _asset1_balance
        double _asset2_balance
        double _asset3_balance   
    
    cdef bint c_ready_for_new_orders(self)
    cdef c_process_market_pair(self)
    cdef c_process_market_pair_inner(self, object vol_1, str side_1, object vol_2, str side_2, object vol_3, str side_3)
    cdef tuple c_calculate_arbitrage_3_2_1_profitability(self)
    cdef tuple c_calculate_arbitrage_1_2_3_profitability(self)
    cdef update_balance(self, bint generate_profit_log)
    

