from typing import (
    List,
    Tuple
)
from decimal import Decimal
from hummingbot.client.config.global_config_map import global_config_map
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.triangular_arbitrage.triangular_arbitrage_pair import TriangleArbitragePair
from hummingbot.strategy.triangular_arbitrage.triangular_arbitrage_config_map import triangular_arbitrage_config_map as ta_map
from hummingbot.strategy.triangular_arbitrage.t_arbitrage import TriangularArbitrageStrategy


def start(self):
    market = ta_map.get("market").value.lower()
    iteration_time = ta_map.get("iteration_time").value
    first_trading_pair = ta_map.get("first_market_trading_pair").value
    second_trading_pair = ta_map.get("second_market_trading_pair").value
    third_trading_pair = ta_map.get("third_market_trading_pair").value
    target_asset = ta_map.get("target_asset").value
    min_profitability = ta_map.get("min_profitability").value / Decimal("100")
    order_amount = ta_map.get("order_amount").value
    strategy_report_interval = global_config_map.get("strategy_report_interval").value   
    
    try:
        first_trading_pair: str = self._convert_to_exchange_trading_pair(market, [first_trading_pair])[0]
        firsrt_asset: Tuple[str, str] = self._initialize_market_assets(market, [first_trading_pair])[0]
        second_trading_pair: str = self._convert_to_exchange_trading_pair(market, [second_trading_pair])[0]
        second_asset: Tuple[str, str] = self._initialize_market_assets(market, [second_trading_pair])[0]
        third_trading_pair: str = self._convert_to_exchange_trading_pair(market, [third_trading_pair])[0]
        third_asset: Tuple[str, str] = self._initialize_market_assets(market, [third_trading_pair])[0]
    

        market_names: List[Tuple[str, List[str]]] = [
            (market, [first_trading_pair, second_trading_pair, third_trading_pair])
        ]

        self._initialize_wallet(token_trading_pairs=list(set(firsrt_asset + second_asset + third_asset)))
        self._initialize_markets(market_names)
        self.assets = set(firsrt_asset + second_asset + third_asset)
        self._assetList = list(self.assets)
        firsrt_asset_data = [self.markets[market], first_trading_pair] + list(firsrt_asset)
        second_asset_data = [self.markets[market], second_trading_pair] + list(second_asset)
        third_asset_data = [self.markets[market], third_trading_pair] + list(third_asset)
        first_trading_pair_tuple = MarketTradingPairTuple(*firsrt_asset_data)
        second_trading_pair_tuple = MarketTradingPairTuple(*second_asset_data)
        third_trading_pair_tuple = MarketTradingPairTuple(*third_asset_data)
        self.market_trading_pair_tuples = [first_trading_pair_tuple, second_trading_pair_tuple, third_trading_pair_tuple]
        self.market_pair = TriangleArbitragePair(first=first_trading_pair_tuple, second=second_trading_pair_tuple, third=third_trading_pair_tuple)
        
        self.strategy = TriangularArbitrageStrategy(
            next_trade_delay_interval=iteration_time,
            market_pairs=self.market_pair,
            target_asset = target_asset,
            asset_list = self._assetList,
            min_profitability=min_profitability,
            status_report_interval=strategy_report_interval,
            order_amount=order_amount,
            hb_app_notification=True
        )
    except ValueError as e:
        self._notify(str(e))
        return