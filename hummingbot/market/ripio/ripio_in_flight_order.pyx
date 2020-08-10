from decimal import Decimal
from typing import Optional, Dict, Any

from hummingbot.core.event.events import OrderType, TradeType
from hummingbot.market.ripio.ripio_market import RipioMarket
from hummingbot.market.in_flight_order_base import InFlightOrderBase


cdef class RipioInFlightOrder(InFlightOrderBase):
    def __init__(self,
                 client_order_id: str,
                 exchange_order_id: Optional[str],
                 trading_pair: str,
                 order_type: OrderType,
                 trade_type: TradeType,
                 price: Decimal,
                 amount: Decimal,
                 initial_state: str = "OPEN"):
        super().__init__(
            RipioMarket,
            client_order_id,
            exchange_order_id,
            trading_pair,
            order_type,
            trade_type,
            price,
            amount,
            initial_state
        )

    @property
    def order_pair(self) -> str:
        return self.trading_pair

    @property
    def is_done(self) -> bool:
        return self.last_state in {"CANC", "FILL"}

    @property
    def is_failure(self) -> bool:
        return self.last_state in {"CANC"}

    @property
    def is_cancelled(self) -> bool:
        return self.last_state in {"CANC"}

    @property
    def order_type_description(self) -> str:
        order_type = "MARKET" if self.order_type is OrderType.MARKET else "LIMIT"
        side = "BUY" if self.trade_type is TradeType.BUY else "SELL"
        return f"{order_type} {side}"

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> InFlightOrderBase:
        cdef:
            RipioInFlightOrder retval = RipioInFlightOrder(
                data["client_order_id"],
                data["exchange_order_id"],
                data["trading_pair"],
                getattr(OrderType, data["order_type"]),
                getattr(TradeType, data["trade_type"]),
                Decimal(data["price"]),
                Decimal(data["amount"]),
                data["last_state"]
            )
        retval.executed_amount_base = Decimal(data["executed_amount_base"])
        retval.executed_amount_quote = Decimal(data["executed_amount_quote"])
        retval.fee_asset = data["fee_asset"]
        retval.fee_paid = Decimal(data["fee_paid"])
        retval.last_state = data["last_state"]
        return retval
