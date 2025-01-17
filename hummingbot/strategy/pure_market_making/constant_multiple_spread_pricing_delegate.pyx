from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.market.market_base cimport MarketBase
from hummingbot.market.market_base import MarketBase
from .data_types import PricingProposal
from .pure_market_making_v2 cimport PureMarketMakingStrategyV2


cdef class ConstantMultipleSpreadPricingDelegate(OrderPricingDelegate):
    def __init__(self, bid_spread: float,
                 ask_spread: float,
                 order_interval_size: float,
                 number_of_orders: int):
        super().__init__()
        self._bid_spread = bid_spread
        self._ask_spread = ask_spread
        self._order_interval_size = order_interval_size
        self._number_of_orders = number_of_orders

    @property
    def bid_spread(self) -> float:
        return self._bid_spread

    @property
    def ask_spread(self) -> float:
        return self._ask_spread

    @property
    def number_of_orders(self) -> int:
        return self._number_of_orders

    @property
    def order_interval_size(self) -> float:
        return self._order_interval_size

    cdef object c_get_order_price_proposal(self,
                                           PureMarketMakingStrategyV2 strategy,
                                           object market_info,
                                           list active_orders):
        cdef:
            MarketBase maker_market = market_info.market
            OrderBook maker_order_book = maker_market.c_get_order_book(market_info.trading_pair)
            double top_bid_price = maker_order_book.c_get_price(False)
            double top_ask_price = maker_order_book.c_get_price(True)
            str market_name = maker_market.name
            double mid_price = (top_bid_price + top_ask_price) * 0.5
            list bid_prices = [maker_market.c_quantize_order_price(market_info.trading_pair,
                                                                   mid_price * (1.0 - self.bid_spread))]
            list ask_prices = [maker_market.c_quantize_order_price(market_info.trading_pair,
                                                                   mid_price * (1.0 + self.ask_spread))]

        for _ in range(self.number_of_orders -1):
            last_bid_price = bid_prices[-1]
            current_bid_price = maker_market.c_quantize_order_price(market_info.trading_pair,
                                                                    float(last_bid_price) * (1 - self.order_interval_size))
            bid_prices.append(current_bid_price)

            last_ask_price = ask_prices[-1]
            current_ask_price = maker_market.c_quantize_order_price(market_info.trading_pair,
                                                                    float(last_ask_price) * (1 + self.order_interval_size))
            ask_prices.append(current_ask_price)

        return PricingProposal(bid_prices, ask_prices)
