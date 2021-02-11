# distutils: language=c++

import traceback
import logging
from decimal import Decimal
import pandas as pd
from typing import (
    List,
    Tuple,
)

from hummingbot.market.market_base cimport MarketBase
from hummingbot.core.event.events import (
    TradeType,
    OrderType,
)
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.client.hummingbot_application import HummingbotApplication
from hummingbot.strategy.strategy_base import StrategyBase
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.triangular_arbitrage.triangular_arbitrage_pair import TriangleArbitragePair

NaN = float("nan")
s_decimal_0 = Decimal(0)
as_logger = None


cdef class TriangularArbitrageStrategy(StrategyBase):
    OPTION_LOG_STATUS_REPORT = 1 << 0
    OPTION_LOG_CREATE_ORDER = 1 << 1
    OPTION_LOG_ORDER_COMPLETED = 1 << 2
    OPTION_LOG_PROFITABILITY_STEP = 1 << 3
    OPTION_LOG_FULL_PROFITABILITY_STEP = 1 << 4
    OPTION_LOG_INSUFFICIENT_ASSET = 1 << 5
    OPTION_LOG_ALL = 0xfffffffffffffff

    @classmethod
    def logger(cls):
        global as_logger
        if as_logger is None:
            as_logger = logging.getLogger(__name__)
        return as_logger

    def __init__(self,                 
                 market_pairs: TriangleArbitragePair,
                 target_asset: str,
                 asset_list: list,
                 min_profitability: Decimal,
                 order_amount: float,
                 next_trade_delay_interval: float,
                 logging_options: int = OPTION_LOG_ORDER_COMPLETED,
                 status_report_interval: float = 60.0,
                 hb_app_notification: bool = False):        
       
        super().__init__()
        self._market_pairs = market_pairs
        self._target_asset = target_asset
        self._assetList = asset_list   
        self._min_profitability = min_profitability
        self._order_amount = order_amount
        self._logging_options = logging_options        
        self._all_markets_ready = False
        self._status_report_interval = status_report_interval
        self._last_timestamp = 0.0
        #self._next_trade_delay = next_trade_delay_interval
        self._next_trade_delay = 6000
        self._last_trade_timestamps = 0.0
        self._cool_off_logged = False
        self._hb_app_notification = hb_app_notification
        self._av_balance_dict = {}
        self._full_balance_dict = {}
        self._order_counter = 0
        self._asset1_balance = 0.0
        self._asset2_balance = 0.0
        self._asset3_balance = 0.0

        cdef:
            set all_markets = {self._market_pairs.first.market, self._market_pairs.second.market, self._market_pairs.third.market}            

        self.c_add_markets(list(all_markets))        
        self.logger().warning(f"Init strategy.")
       
    @property
    def tracked_limit_orders(self) -> List[Tuple[MarketBase, LimitOrder]]:
        return self._sb_order_tracker.tracked_limit_orders

    @property
    def tracked_limit_orders_data_frame(self) -> List[pd.DataFrame]:
        return self._sb_order_tracker.tracked_limit_orders_data_frame
    
    def format_status(self) -> str:
        cdef:
            list lines = []
            list warning_lines = []
        warning_lines.extend(self.network_warning([self._market_pairs.first, self._market_pairs.first, self._market_pairs.third]))

        markets_df = self.market_status_data_frame([self._market_pairs.first, self._market_pairs.first, self._market_pairs.third])
        lines.extend(["", "  Markets:"] +
                         ["    " + line for line in str(markets_df).split("\n")])

        assets_df = self.wallet_balance_data_frame([self._market_pairs.first])
        lines.extend(["", "  Assets:"] +
                         ["    " + line for line in str(assets_df).split("\n")])
           
        warning_lines.extend(self.balance_warning([self._market_pairs.first, self._market_pairs.first, self._market_pairs.third]))
        if len(warning_lines) > 0:
            lines.extend(["", "  *** WARNINGS ***"] + warning_lines)

        return "\n".join(lines)

    def notify_hb_app(self, msg: str):
        if self._hb_app_notification:
            from hummingbot.client.hummingbot_application import HummingbotApplication
            HummingbotApplication.main_application()._notify(msg)

    cdef c_tick(self, double timestamp):

        StrategyBase.c_tick(self, timestamp)

        cdef:
            int64_t current_tick = <int64_t>(timestamp // self._status_report_interval)
            int64_t last_tick = <int64_t>(self._last_timestamp // self._status_report_interval)
            bint should_report_warnings = ((current_tick > last_tick) and
                                           (self._logging_options & self.OPTION_LOG_STATUS_REPORT))
        try:
            if not self._all_markets_ready:
                self._all_markets_ready = all([market.ready for market in self._sb_markets])
                if not self._all_markets_ready:
                    # Markets not ready yet. Don't do anything.
                    if should_report_warnings:
                        self.logger().warning(f"Markets are not ready. No arbitrage trading is permitted.")
                    return
                else:
                    if self.OPTION_LOG_STATUS_REPORT:
                        self.logger().info(f"Markets are ready. Trading started.")

            if not all([market.network_status is NetworkStatus.CONNECTED for market in self._sb_markets]):
                if should_report_warnings:
                    self.logger().warning(f"Markets are not all online. No arbitrage trading is permitted.")
                return
            
            self.c_process_market_pair()
        finally:
            self._last_timestamp = timestamp
    
    cdef tuple c_calculate_arbitrage_1_2_3_profitability(self):
        cdef:
            object first_bid_price = float(self._market_pairs.first.get_price(False))
            object first_ask_price = float(self._market_pairs.first.get_price(True))
            object second_bid_price = float(self._market_pairs.second.get_price(False))
            object second_ask_price = float(self._market_pairs.second.get_price(True))
            object third_bid_price = float(self._market_pairs.third.get_price(False))
            object third_ask_price = float(self._market_pairs.third.get_price(True))
            double vol_1 = Decimal(self._order_amount)
            double vol_2 = Decimal(0.0)
            double vol_3 = Decimal(0.0)
            str side_1 = ""
            str side_2 = ""
            str side_3 = ""
            MarketBase first_market = self._market_pairs.first.market
            MarketBase second_market = self._market_pairs.second.market
            MarketBase third_market = self._market_pairs.third.market
            flag_balance_incorrect = False
            first = self._market_pairs.first
            second = self._market_pairs.second
            third = self._market_pairs.third

        market_pair = self._market_pairs
        #First step
        step_operation_asset = ""
        result_vol = 0.0
        if self._target_asset == market_pair.first.base_asset:
            step_operation_asset = market_pair.first.quote_asset
            vol_2 = first_bid_price * vol_1
            side_1 = "sell"
            quantized_vol_1 = first_market.c_quantize_order_amount(market_pair.first.trading_pair, Decimal(vol_1))
            self.logger().info(f"1) Pair:{market_pair.first.trading_pair} Side:{side_1}  ({market_pair.first.base_asset}:{vol_1}) => ({market_pair.first.quote_asset}:{vol_2})")
            #Balance check
            if quantized_vol_1 > self._av_balance_dict[first.base_asset]:
                self.logger().info(f"{quantized_vol_1} more than balance {first.base_asset}:{self._av_balance_dict[first.base_asset]}. Order cant be placed")
                flag_balance_incorrect = True
        elif self._target_asset == market_pair.first.quote_asset:
            step_operation_asset = market_pair.first.base_asset
            vol_2 = vol_1 / first_ask_price
            side_1 = "buy"
            quantized_vol_1 = first_market.c_quantize_order_amount(market_pair.first.trading_pair, Decimal(vol_2))
            self.logger().info(f"1) Pair:{market_pair.first.trading_pair} Side:{side_1}  ({market_pair.first.quote_asset}:{vol_1}) => ({market_pair.first.base_asset}:{vol_2})")
            #Balance check
            if vol_1 > self._av_balance_dict[first.quote_asset]:
                self.logger().info(f"{vol_1} more than balance {first.quote_asset}:{self._av_balance_dict[first.quote_asset]}")
                flag_balance_incorrect = True

        #Second step
        self.logger().info(f"TMP LOG - seecond asset:{second.base_asset}-{second.quote_asset}, step_operation_asset:{step_operation_asset}")
        if step_operation_asset == market_pair.second.base_asset:
            step_operation_asset = market_pair.second.quote_asset
            vol_3 = second_bid_price * vol_2
            side_2 = "sell"
            quantized_vol_2 = second_market.c_quantize_order_amount(market_pair.second.trading_pair, Decimal(vol_2))
            self.logger().info(f"2) Pair:{market_pair.second.trading_pair} Side:{side_2}  ({market_pair.second.base_asset}:{vol_2}) => ({market_pair.second.quote_asset}:{vol_3})")
            #Balance check
            if quantized_vol_2 > self._av_balance_dict[second.base_asset]:
                self.logger().info(f"{quantized_vol_2} more than balance {second.base_asset}:{self._av_balance_dict[second.base_asset]}. Order cant be placed")
                flag_balance_incorrect = True
        elif step_operation_asset == market_pair.second.quote_asset:
            step_operation_asset = market_pair.second.base_asset
            vol_3 = vol_2 / second_ask_price
            side_2 = "buy"
            quantized_vol_2 = second_market.c_quantize_order_amount(market_pair.second.trading_pair, Decimal(vol_3))
            self.logger().info(f"2) Pair:{market_pair.second.trading_pair} Side:{side_2}  ({market_pair.second.quote_asset}:{vol_2}) => ({market_pair.second.base_asset}:{vol_3})")
             #Balance check
            if vol_2 > self._av_balance_dict[second.quote_asset]:
                self.logger().info(f"{vol_2} more than balance {second.quote_asset}:{self._av_balance_dict[second.quote_asset]}")
                flag_balance_incorrect = True

        #Third step
        if step_operation_asset == market_pair.third.base_asset:
            step_operation_asset = market_pair.third.quote_asset
            result_vol = third_bid_price * vol_3
            side_3 = "sell"
            quantized_vol_3 = third_market.c_quantize_order_amount(market_pair.third.trading_pair, Decimal(vol_3))
            self.logger().info(f"3) Pair:{market_pair.third.trading_pair} Side:{side_3}  ({market_pair.third.base_asset}:{vol_3}) => ({market_pair.third.quote_asset}:{result_vol})")
            #Balance check
            if quantized_vol_3 > self._av_balance_dict[third.base_asset]:
                self.logger().info(f"{quantized_vol_3} more than balance {third.base_asset}:{self._av_balance_dict[third.base_asset]}. Order cant be placed")
                flag_balance_incorrect = True
        elif step_operation_asset == market_pair.third.quote_asset:
            step_operation_asset = market_pair.third.base_asset
            result_vol = vol_3 / third_ask_price
            side_3 = "buy"
            quantized_vol_3 = third_market.c_quantize_order_amount(market_pair.third.trading_pair, Decimal(result_vol))
            self.logger().info(f"3) Pair:{market_pair.third.trading_pair} Side:{side_3}  ({market_pair.third.quote_asset}:{vol_3}) => ({market_pair.third.base_asset}:{result_vol})")
             #Balance check
            if vol_3 > self._av_balance_dict[third.quote_asset]:
                self.logger().info(f"{vol_3} more than balance {third.quote_asset}:{self._av_balance_dict[third.quote_asset]}")
                flag_balance_incorrect = True

        self.logger().info(f"1_2_3 result_vol: {vol_1} => {result_vol}")

        if flag_balance_incorrect:
            profitability_1_2_3 = -100.0
        else:
            profitability_1_2_3 = ((result_vol - vol_1) / vol_1) * 100

        return profitability_1_2_3, quantized_vol_1, side_1, quantized_vol_2, side_2, quantized_vol_3, side_3

    cdef tuple c_calculate_arbitrage_3_2_1_profitability(self):
        cdef:
            object first_bid_price = float(self._market_pairs.first.get_price(False))
            object first_ask_price = float(self._market_pairs.first.get_price(True))
            object second_bid_price = float(self._market_pairs.second.get_price(False))
            object second_ask_price = float(self._market_pairs.second.get_price(True))
            object third_bid_price = float(self._market_pairs.third.get_price(False))
            object third_ask_price = float(self._market_pairs.third.get_price(True))
            double vol_1 = self._order_amount
            double vol_2 = 0.0
            double vol_3 = 0.0
            str side_1 = ""
            str side_2 = ""
            str side_3 = ""            
            MarketBase first_market = self._market_pairs.first.market
            MarketBase second_market = self._market_pairs.second.market
            MarketBase third_market = self._market_pairs.third.market
            flag_balance_incorrect = False
            first = self._market_pairs.first
            second = self._market_pairs.second
            third = self._market_pairs.third
        
        market_pair = self._market_pairs        

        #First step        
        step_operation_asset = ""
        result_vol = 0.0
        if self._target_asset == market_pair.third.base_asset:
            step_operation_asset = market_pair.third.quote_asset
            vol_2 = third_bid_price * vol_1
            side_1 = "sell"
            quantized_vol_1 = third_market.c_quantize_order_amount(market_pair.third.trading_pair, Decimal(vol_1))           
            self.logger().info(f"1) Pair:{market_pair.third.trading_pair} Side:{side_1}  ({market_pair.third.base_asset}:{vol_1}) => ({market_pair.third.quote_asset}:{vol_2})")
            #Balance check
            if quantized_vol_1 > self._av_balance_dict[third.base_asset]:
                self.logger().info(f"{quantized_vol_1} more than balance {third.base_asset}:{self._av_balance_dict[third.base_asset]}. Order cant be placed")
                flag_balance_incorrect = True
        elif self._target_asset == market_pair.third.quote_asset:
            step_operation_asset = market_pair.third.base_asset
            vol_2 = vol_1 / third_ask_price
            side_1 = "buy"
            quantized_vol_1 = third_market.c_quantize_order_amount(market_pair.third.trading_pair, Decimal(vol_2))
            self.logger().info(f"1) Pair:{market_pair.third.trading_pair} Side:{side_1}  ({market_pair.third.quote_asset}:{vol_1}) => ({market_pair.third.base_asset}:{vol_2})")
            #Balance check
            if vol_1 > self._av_balance_dict[third.quote_asset]:
                self.logger().info(f"{vol_1} more than balance {third.quote_asset}:{self._av_balance_dict[third.quote_asset]}")
                flag_balance_incorrect = True

        #Second step
        if step_operation_asset == market_pair.second.base_asset:
            step_operation_asset = market_pair.second.quote_asset
            vol_3 = second_bid_price * vol_2
            side_2 = "sell"
            quantized_vol_2 = second_market.c_quantize_order_amount(market_pair.second.trading_pair, Decimal(vol_2))
            self.logger().info(f"2) Pair:{market_pair.second.trading_pair} Side:{side_2}  ({market_pair.second.base_asset}:{vol_2}) => ({market_pair.second.quote_asset}:{vol_3})")
            #Balance check
            if quantized_vol_2 > self._av_balance_dict[second.base_asset]:
                self.logger().info(f"{quantized_vol_2} more than balance {second.base_asset}:{self._av_balance_dict[second.base_asset]}. Order cant be placed")
                flag_balance_incorrect = True
        elif step_operation_asset == market_pair.second.quote_asset:
            step_operation_asset = market_pair.second.base_asset
            vol_3 = vol_2 / second_ask_price
            side_2 = "buy"
            quantized_vol_2 = second_market.c_quantize_order_amount(market_pair.second.trading_pair, Decimal(vol_3))
            self.logger().info(f"2) Pair:{market_pair.second.trading_pair} Side:{side_2}  ({market_pair.second.quote_asset}:{vol_2}) => ({market_pair.second.base_asset}:{vol_3})")
            #Balance check
            if vol_2 > self._av_balance_dict[second.quote_asset]:
                self.logger().info(f"{vol_2} more than balance {second.quote_asset}:{self._av_balance_dict[second.quote_asset]}")
                flag_balance_incorrect = True

        #Third step
        if step_operation_asset == market_pair.first.base_asset:
            step_operation_asset = market_pair.first.quote_asset
            result_vol = first_bid_price * vol_3
            side_3 = "sell"
            quantized_vol_3 = first_market.c_quantize_order_amount(market_pair.first.trading_pair, Decimal(vol_3))
            self.logger().info(f"3) Pair:{market_pair.first.trading_pair} Side:{side_3}  ({market_pair.first.base_asset}:{vol_3}) => ({market_pair.first.quote_asset}:{result_vol})")
            #Balance check
            if quantized_vol_3 > self._av_balance_dict[first.base_asset]:
                self.logger().info(f"{quantized_vol_3} more than balance {first.base_asset}:{self._av_balance_dict[first.base_asset]}. Order cant be placed")
                flag_balance_incorrect = True
        elif step_operation_asset == market_pair.first.quote_asset:
            step_operation_asset = market_pair.first.base_asset
            result_vol = vol_3 / first_ask_price
            side_3 = "buy"
            quantized_vol_3 = first_market.c_quantize_order_amount(market_pair.first.trading_pair, Decimal(result_vol))
            self.logger().info(f"3) Pair:{market_pair.first.trading_pair} Side:{side_3}  ({market_pair.first.quote_asset}:{vol_3}) => ({market_pair.first.base_asset}:{result_vol})")
            #Balance check
            if vol_3 > self._av_balance_dict[first.quote_asset]:
                self.logger().info(f"{vol_3} more than balance {first.quote_asset}:{self._av_balance_dict[first.quote_asset]}")
                flag_balance_incorrect = True


        self.logger().info(f"3_2_1 result_vol: {vol_1} => {result_vol}")

        if flag_balance_incorrect:
            profitability_3_2_1 = -100.0
        else:
            profitability_3_2_1 = ((result_vol - vol_1) / vol_1) * 100       
        

        return profitability_3_2_1, quantized_vol_1, side_1, quantized_vol_2, side_2, quantized_vol_3, side_3

    cdef bint c_ready_for_new_orders(self):
        cdef:
            double time_left

        ready_to_trade_time = self._last_trade_timestamps + self._next_trade_delay
        if ready_to_trade_time > self._current_timestamp:
                time_left = self._current_timestamp - self._last_trade_timestamps - self._next_trade_delay
                if not self._cool_off_logged:
                    self.log_with_clock(
                        logging.INFO,
                        f"Cooling off from previous trade. "
                        f"Resuming in {int(time_left)} seconds."
                    )
                    self._cool_off_logged = True
                return False

        if self._cool_off_logged:
            self.log_with_clock(
                logging.INFO,
                f"Cool off completed. Arbitrage strategy is now ready for new orders."
            )
            # reset cool off log tag when strategy is ready for new orders
            self._cool_off_logged = False

        return True

    cdef c_process_market_pair(self):
        """
        Checks which direction is more profitable (buy/sell on exchange 2/1 or 1/2) and sends the more profitable
        direction for execution.

        :param market_pair: arbitrage market pair
        """

        if not self.c_ready_for_new_orders():
            return

        self.update_balance(generate_profit_log = True)

        #Forward chain parameters 1_2_3
        profitability_1_2_3, f_vol_1, f_side_1, f_vol_2, f_side_2, f_vol_3, f_side_3 = self.c_calculate_arbitrage_1_2_3_profitability()

        #Reverse chain parameters 3_2_1
        profitability_3_2_1, r_vol_3, r_side_3, r_vol_2, r_side_2, r_vol_1, r_side_1 = self.c_calculate_arbitrage_3_2_1_profitability()
        
        self.log_with_clock(logging.INFO,
                                    f"Profitability 1->2->3: {profitability_1_2_3}")
        self.log_with_clock(logging.INFO,
                                    f"Profitability 3->2->1: {profitability_3_2_1}")        

        min_orderVol_flag_1_2_3 = False
        if f_vol_1 == 0 or f_vol_2 == 0 or f_vol_3 == 0:
            orderVol_flag_1_2_3 = True
            self.logger().info(f"Profitability 1->2->3 order amount less than minimum")

        min_orderVol_flag_3_2_1 = False
        if f_vol_1 == 0 or f_vol_2 == 0 or f_vol_3 == 0:
            orderVol_flag_1_2_3 = True
            self.logger().info(f"Profitability 1->2->3 order amount less than minimum")
        
        if profitability_1_2_3 == -100.0 or profitability_3_2_1 == -100.0:
            self.logger().info(f"Incorrect balance")
            self._last_trade_timestamps = self._current_timestamp
            return
        
        if profitability_1_2_3 > profitability_3_2_1 and min_orderVol_flag_1_2_3 != True and profitability_1_2_3 > self._min_profitability:
        #if profitability_1_2_3 > profitability_3_2_1 and min_orderVol_flag_1_2_3 != True:
            self.log_with_clock(logging.INFO,
                                    f"Calculate forward  profitability_1_2_3: {profitability_1_2_3}")
            self.c_process_market_pair_inner(f_vol_1, f_side_1, f_vol_2, f_side_2, f_vol_3, f_side_3)
            return
        elif profitability_3_2_1 > profitability_1_2_3 and  min_orderVol_flag_3_2_1 != True and profitability_3_2_1 > self._min_profitability:
        #elif profitability_3_2_1 > profitability_1_2_3 and  min_orderVol_flag_3_2_1 != True:
            self.log_with_clock(logging.INFO,
                                    f"Calculate revers  profitability_3_2_1: {profitability_3_2_1}")
            self.c_process_market_pair_inner(r_vol_3, r_side_3, r_vol_2, r_side_2, r_vol_1, r_side_1)
            return

        self.log_with_clock(logging.INFO,
                                    f"profitability_1_2_3: {profitability_1_2_3}, profitability_3_2_1: {profitability_3_2_1} less then min profit {self._min_profitability}")
        

    cdef c_process_market_pair_inner(self, object vol_1, str side_1, object vol_2, str side_2, object vol_3, str side_3):
        cdef:
            first = self._market_pairs.first
            second = self._market_pairs.second
            third = self._market_pairs.third            

        self.log_with_clock(logging.INFO,
                                    f"Executing market order {side_1} of {first.trading_pair} with amount {vol_1}")
        self.log_with_clock(logging.INFO,
                                    f"Executing market order {side_2} of {second.trading_pair} with amount {vol_2}")
        self.log_with_clock(logging.INFO,
                                    f"Executing market order {side_3} of {third.trading_pair} with amount {vol_3}")
       

        # Set limit order expiration_seconds to _next_trade_delay for connectors that require order expiration for limit orders
        self._order_counter = 0
        if side_1 == "buy":
            self.c_buy_with_specific_market(first, vol_1, order_type=OrderType.MARKET)
        else:
            self.c_sell_with_specific_market(first, vol_1, order_type=OrderType.MARKET)
        
        if side_2 == "buy":
            self.c_buy_with_specific_market(second, vol_2, order_type=OrderType.MARKET)
        else:
            self.c_sell_with_specific_market(second, vol_2, order_type=OrderType.MARKET)

        if side_3 == "buy":
            self.c_buy_with_specific_market(third, vol_3, order_type=OrderType.MARKET)
        else:
            self.c_sell_with_specific_market(third, vol_3, order_type=OrderType.MARKET)
        
        self.update_balance(generate_profit_log = False)        

    cdef update_balance(self, bint generate_profit_log):
        cdef:
            first = self._market_pairs.first
            second = self._market_pairs.second
            third = self._market_pairs.third

        if generate_profit_log:
            _prev_full_balance_dict = self._full_balance_dict.copy()

        assets_df = self.wallet_balance_data_frame([first, second, third])
        for row in assets_df.itertuples():
            index = row[2] 
            self._av_balance_dict[index] = row[4]
            self._full_balance_dict[index] = row[3]

        if generate_profit_log:
            for asset, prev_balance in _prev_full_balance_dict.items():
                new_balance = self._full_balance_dict[asset]
                if prev_balance != new_balance:
                    delta_balance = new_balance - prev_balance
                    delta_percent = delta_balance / prev_balance
                    self.logger().info(f"{asset} balance:{new_balance}   delta vol:{delta_balance}   delta percent:{delta_percent}")

    cdef c_did_complete_buy_order(self, object buy_order_completed_event):
        cdef:
            object buy_order = buy_order_completed_event
            object market_trading_pair_tuple = self._sb_order_tracker.c_get_market_pair_from_order_id(buy_order.order_id)

        self._order_counter += 1
        if self._order_counter == 3:
            self.update_balance(generate_profit_log = True)

    cdef c_did_complete_sell_order(self, object sell_order_completed_event):
        cdef:
            object sell_order = sell_order_completed_event
            object market_trading_pair_tuple = self._sb_order_tracker.c_get_market_pair_from_order_id(sell_order.order_id)
        
        self._order_counter += 1
        if self._order_counter == 3:
            self.update_balance(generate_profit_log = True)
