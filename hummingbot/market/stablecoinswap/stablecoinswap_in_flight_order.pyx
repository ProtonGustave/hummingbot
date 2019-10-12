from decimal import Decimal
from typing import (
    Any,
    Dict,
    Optional,
)

from hummingbot.core.event.events import (
    OrderType,
    TradeType
)
from hummingbot.market.stablecoinswap.stablecoinswap_market import StablecoinswapMarket
from hummingbot.market.in_flight_order_base import InFlightOrderBase


cdef class StablecoinswapInFlightOrder(InFlightOrderBase):
    def __init__(self,
                 client_order_id: str,
                 exchange_order_id: Optional[str],
                 symbol: str,
                 order_type: OrderType,
                 trade_type: TradeType,
                 price: Decimal,
                 amount: Decimal,
                 tx_hash: Optional[str] = None,
                 initial_state: str = "open"):
        super().__init__(
            StablecoinswapMarket,
            client_order_id,
            exchange_order_id,
            symbol,
            order_type,
            trade_type,
            price,
            amount,
            initial_state,
        )
        self.tx_hash = tx_hash  # used for tracking market orders

    @property
    def is_done(self) -> bool:
        return self.last_state in {"filled", "canceled" "done"}

    @property
    def is_failure(self) -> bool:
        # This is the only known canceled state
        return self.last_state == "canceled"

    @property
    def is_cancelled(self) -> bool:
        return self.last_state == "canceled"

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> InFlightOrderBase:
        """
        :param data: json data from API
        :return: formatted InFlightOrder
        """
        cdef:
            StablecoinswapInFlightOrder retval = StablecoinswapInFlightOrder(
                client_order_id=data["client_order_id"],
                exchange_order_id=data["exchange_order_id"],
                symbol=data["symbol"],
                order_type=getattr(OrderType, data["order_type"]),
                trade_type=getattr(TradeType, data["trade_type"]),
                price=Decimal(data["price"]),
                amount=Decimal(data["amount"]),
                initial_state=data["last_state"],
                tx_hash=data["tx_hash"],
            )
        return retval
