from async_timeout import timeout
import asyncio
from collections import (
    deque,
    OrderedDict
)
from decimal import Decimal
from libc.stdint cimport int64_t
import logging
import time
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
)
from web3 import Web3

from hummingbot.logger import HummingbotLogger
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.core.data_type.order_book_tracker import OrderBookTrackerDataSourceType
from hummingbot.core.event.events import (
    MarketEvent,
    BuyOrderCompletedEvent,
    SellOrderCompletedEvent,
    OrderFilledEvent,
    OrderCancelledEvent,
    MarketOrderFailureEvent,
    BuyOrderCreatedEvent,
    SellOrderCreatedEvent,
    TradeType,
    OrderType,
    TradeFee
)
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.utils.async_call_scheduler import AsyncCallScheduler
from hummingbot.core.utils.async_utils import (
    safe_ensure_future,
    safe_gather,
)
from hummingbot.wallet.ethereum.web3_wallet import Web3Wallet
from hummingbot.market.market_base cimport MarketBase
from hummingbot.logger import HummingbotLogger

from hummingbot.market.stablecoinswap.stablecoinswap_order_book_tracker import StablecoinswapOrderBookTracker

im_logger = None
s_decimal_0 = Decimal(0)

cdef class StablecoinswapMarketTransactionTracker(TransactionTracker):
    cdef:
        StablecoinswapMarket _owner

    def __init__(self, owner: StablecoinswapMarket):
        super().__init__()
        self._owner = owner

    cdef c_did_timeout_tx(self, str tx_id):
        TransactionTracker.c_did_timeout_tx(self, tx_id)
        self._owner.c_did_timeout_tx(tx_id)

cdef class StablecoinswapMarket(MarketBase):
    MARKET_RECEIVED_ASSET_EVENT_TAG = MarketEvent.ReceivedAsset.value
    MARKET_BUY_ORDER_COMPLETED_EVENT_TAG = MarketEvent.BuyOrderCompleted.value
    MARKET_SELL_ORDER_COMPLETED_EVENT_TAG = MarketEvent.SellOrderCompleted.value
    MARKET_WITHDRAW_ASSET_EVENT_TAG = MarketEvent.WithdrawAsset.value
    MARKET_ORDER_CANCELLED_EVENT_TAG = MarketEvent.OrderCancelled.value
    MARKET_ORDER_FILLED_EVENT_TAG = MarketEvent.OrderFilled.value
    MARKET_ORDER_FAILURE_EVENT_TAG = MarketEvent.OrderFailure.value
    MARKET_BUY_ORDER_CREATED_EVENT_TAG = MarketEvent.BuyOrderCreated.value
    MARKET_SELL_ORDER_CREATED_EVENT_TAG = MarketEvent.SellOrderCreated.value

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global im_logger
        if im_logger is None:
            im_logger = logging.getLogger(__name__)
        return im_logger

    def __init__(self,
                 wallet: Web3Wallet,
                 ethereum_rpc_url: str,
                 poll_interval: float = 5.0,
                 symbols: Optional[List[str]] = None,
                 trading_required: bool = True):
        super().__init__()
        self._order_book_tracker = StablecoinswapOrderBookTracker(symbols=symbols)
        self._trading_required = trading_required
        self._tx_tracker = StablecoinswapMarketTransactionTracker(self)
        self._wallet = wallet
        self._w3 = Web3(Web3.HTTPProvider(ethereum_rpc_url))
        self._last_timestamp = 0
        self._last_update_order_timestamp = 0
        self._last_update_next_nonce_timestamp = 0
        self._poll_interval = poll_interval
        self._in_flight_orders = {}
        self._next_nonce = None

    @staticmethod
    def split_symbol(symbol: str) -> Tuple[str, str]:
        try:
            quote_asset, base_asset = symbol.split('-')
            return base_asset, quote_asset
        except Exception:
            raise ValueError(f"Error parsing symbol {symbol}")

    cdef str c_buy(self,
                   str symbol,
                   object amount,
                   object order_type = OrderType.MARKET,
                   object price = s_decimal_0,
                   dict kwargs = {}):
        cdef:
            int64_t tracking_nonce = <int64_t>(time.time() * 1e6)
            str order_id = str(f"buy-{symbol}-{tracking_nonce}")

        # TODO: raise exception on not-MARKET order

        safe_ensure_future(self.execute_buy(order_id, symbol, amount, order_type, price))
        return order_id

    async def execute_buy(self,
                          order_id: str,
                          symbol: str,
                          amount: Decimal,
                          order_type: OrderType,
                          price: Decimal) -> str:
        cdef:
            object q_amt = self.c_quantize_order_amount(symbol, amount)
            object q_price = s_decimal_0

        # TODO: raise exception on not-MARKET order

        try:
            self.c_start_tracking_order(order_id, symbol, TradeType.BUY, q_amt)

            # TODO: place order
            self.logger().info(f"Created market buy order for {q_amt} {symbol}.")
            self.c_trigger_event(self.MARKET_BUY_ORDER_CREATED_EVENT_TAG,
                                 BuyOrderCreatedEvent(
                                     self._current_timestamp,
                                     order_type,
                                     symbol,
                                     float(q_amt),
                                     0.0,
                                     order_id
                                 ))
        except Exception as e:
            self.c_stop_tracking_order(order_id)
            self.logger().network(
                f"Error submitting buy order to Stablecoinswap for {amount} {symbol}.",
                exc_info=True,
                app_warning_msg=f"Failed to submit buy order to Stablecoinswap: {e}"
            )
            self.c_trigger_event(self.MARKET_ORDER_FAILURE_EVENT_TAG,
                                 MarketOrderFailureEvent(self._current_timestamp,
                                                         order_id,
                                                         order_type)
                                 )

    cdef str c_sell(self,
                    str symbol,
                    object amount,
                    object order_type = OrderType.MARKET,
                    object price = s_decimal_0,
                    dict kwargs = {}):
        cdef:
            int64_t tracking_nonce = <int64_t>(time.time() * 1e6)
            str order_id = str(f"sell-{symbol}-{tracking_nonce}")

        # TODO: raise exception on not-MARKET order

        safe_ensure_future(self.execute_sell(order_id, symbol, amount, order_type, price))
        return order_id

    async def execute_sell(self,
                           order_id: str,
                           symbol: str,
                           amount: Decimal,
                           order_type: OrderType,
                           price: Decimal) -> str:
        cdef:
            object q_amt = self.c_quantize_order_amount(symbol, amount)
            object q_price = s_decimal_0

            # TODO: raise exception on not-MARKET order

        try:
            self.c_start_tracking_order(order_id, symbol, TradeType.SELL, order_type, q_amt, q_price)
            # TODO: place order
            self.logger().info(f"Created market sell order for {q_amt} {symbol}.")
            self.c_trigger_event(self.MARKET_SELL_ORDER_CREATED_EVENT_TAG,
                                 SellOrderCreatedEvent(
                                     self._current_timestamp,
                                     order_type,
                                     symbol,
                                     float(q_amt),
                                     0.0,
                                     order_id
                                 ))
        except Exception as e:
            self.c_stop_tracking_order(order_id)
            self.logger().network(
                f"Error submitting sell order to Stablecoinswap for {amount} {symbol}.",
                exc_info=True,
                app_warning_msg=f"Failed to submit sell order to Stablecoinswap: {e}"
            )
            self.c_trigger_event(self.MARKET_ORDER_FAILURE_EVENT_TAG,
                                 MarketOrderFailureEvent(self._current_timestamp,
                                                         order_id,
                                                         order_type)
                                 )

    def quantize_order_amount(self, symbol: str, amount: Decimal, price: Decimal = s_decimal_0) -> Decimal:
        return self.c_quantize_order_amount(symbol, amount, price)

    cdef object c_quantize_order_amount(self, str symbol, object amount, object price = s_decimal_0):
        quantized_amount = MarketBase.c_quantize_order_amount(self, symbol, amount)
        base_asset, quote_asset = self.split_symbol(symbol)

        # Check against MINIMUM_MAKER_ORDER_SIZE_ETH return 0 if less than minimum.
        if base_asset == "ETH" and float(quantized_amount) < Decimal(self.MINIMUM_MAKER_ORDER_SIZE_ETH):
            return s_decimal_0
        elif quote_asset == "ETH":
            # Price is not passed in for market orders so price needs to be checked via the order book
            actual_price = Decimal(price or self.get_price(symbol, True))  # Since order side is unknown use higher price (buy)
            amount_quote = quantized_amount * actual_price
            if amount_quote < Decimal(self.MINIMUM_MAKER_ORDER_SIZE_ETH):
                return s_decimal_0
        return quantized_amount

    cdef object c_get_order_size_quantum(self, str symbol, object amount):
        cdef:
            base_asset = symbol.split("_")[1]
            base_asset_decimals = self._assets_info[base_asset]["decimals"]
        decimals_quantum = Decimal(f"1e-{base_asset_decimals}")
        return decimals_quantum

    cdef c_start_tracking_order(self,
                                str client_order_id,
                                str symbol,
                                object trade_type,
                                object order_type,
                                object amount,
                                object price):
        self._in_flight_orders[client_order_id] = IDEXInFlightOrder(
            client_order_id=client_order_id,
            exchange_order_id=None,
            symbol=symbol,
            order_type=order_type,
            trade_type=trade_type,
            price=price,
            amount=amount
        )
