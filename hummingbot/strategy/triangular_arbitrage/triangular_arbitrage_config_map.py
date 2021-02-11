from hummingbot.client.config.config_var import ConfigVar
from hummingbot.client.config.config_validators import (
    validate_exchange,
    validate_market_trading_pair,
    validate_decimal,
    validate_bool
)
from hummingbot.client.settings import required_exchanges, EXAMPLE_PAIRS
from decimal import Decimal
from hummingbot.client.config.config_helpers import (
    minimum_order_amount
)
from typing import Optional


def first_trading_pair_prompt():
    maker_market = triangular_arbitrage_config_map.get("market").value
    example = EXAMPLE_PAIRS.get(maker_market)
    return "Enter first token trading pair: %s%s >>> " % (
        maker_market,
        f" (e.g. {example})" if example else "",
    )

def second_trading_pair_prompt():
    maker_market = triangular_arbitrage_config_map.get("market").value
    example = EXAMPLE_PAIRS.get(maker_market)
    return "Enter second token trading pair: %s%s >>> " % (
        maker_market,
        f" (e.g. {example})" if example else "",
    )

def third_trading_pair_prompt():
    maker_market = triangular_arbitrage_config_map.get("market").value
    example = EXAMPLE_PAIRS.get(maker_market)
    return "Enter third token trading pair: %s%s >>> " % (
        maker_market,
        f" (e.g. {example})" if example else "",
    )

# strategy specific validators
def validate_target_asset(value: str) -> Optional[str]:
    try:
        """
        first = triangular_arbitrage_config_map.get("first_market_trading_pair").value
        third = triangular_arbitrage_config_map.get("third_market_trading_pair").value
        if value not in first:
            return f"The first pair {first.} should contain the target asset {value}."
        elif value not in third:
            return f"The first pair {first.} should contain the target asset {value}"
        """
    except Exception:
        return "Invalid target asset"

def validate_trading_pair(value: str) -> Optional[str]:
    market = triangular_arbitrage_config_map.get("market").value
    return validate_market_trading_pair(market, value)

def order_amount_prompt() -> str:
    maker_exchange = triangular_arbitrage_config_map["market"].value
    trading_pair = triangular_arbitrage_config_map.get("first_market_trading_pair").value
    base_asset, quote_asset = trading_pair.split("-")
    min_amount = minimum_order_amount(maker_exchange, trading_pair)
    return f"What is the amount of {base_asset} per order? (minimum {min_amount}) >>>: "

def validate_order_amount(value: str) -> Optional[str]:
    try:
        maker_exchange = triangular_arbitrage_config_map.get("market").value
        trading_pair = triangular_arbitrage_config_map.get("first_market_trading_pair").value
        min_amount = minimum_order_amount(maker_exchange, trading_pair)
        if Decimal(value) < min_amount:
            return f"Order amount must be at least {min_amount}."
    except Exception:
        return "Invalid order amount."

triangular_arbitrage_config_map = {
    "strategy": ConfigVar(
        key="strategy",
        prompt="",
        default="triangular_arbitrage"
    ),
    "market": ConfigVar(
        key="market",
        prompt="Enter your exchange name >>> ",
        prompt_on_new=True,
        validator=validate_exchange,
        on_validated=lambda value: required_exchanges.append(value),
    ),    
    "first_market_trading_pair": ConfigVar(
        key="first_market_trading_pair",
        prompt=first_trading_pair_prompt,
        prompt_on_new=True,
        validator=validate_trading_pair
    ),
    "second_market_trading_pair": ConfigVar(
        key="second_market_trading_pair",
        prompt=second_trading_pair_prompt,
        prompt_on_new=True,
        validator=validate_trading_pair
    ),
    "third_market_trading_pair": ConfigVar(
        key="third_market_trading_pair",
        prompt=third_trading_pair_prompt,
        prompt_on_new=True,
        validator=validate_trading_pair
    ),
    "target_asset": ConfigVar(
        key="target_asset",
        prompt="Target asset - must be included in the first and third pair",
        prompt_on_new=True,
        validator=validate_target_asset
    ),
    "min_profitability": ConfigVar(
        key="min_profitability",
        prompt="What is the minimum profitability for you to make a trade? (Enter 1 to indicate 1%) >>> ",
        prompt_on_new=True,
        validator=lambda v: validate_decimal(v, Decimal(-100), Decimal("100"), inclusive=True),
        type_str="decimal",
    ),
    "order_amount": ConfigVar(
        key="order_amount",
        prompt=order_amount_prompt,
        prompt_on_new=True,
        type_str="decimal",
        validator=validate_order_amount,
    ),
    "iteration_time": ConfigVar(
        key="iteration_time",
        prompt="Time to wait for a new iteration after order execution(in seconds)? >>> ",
        default=60.0,
        type_str="float",
        required_if=lambda: False,
        validator=lambda v: validate_decimal(v, min_value=0, inclusive=False)
    ) 
}
