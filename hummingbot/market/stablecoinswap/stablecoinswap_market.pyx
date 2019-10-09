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

# TODO implement
from hummingbot.market.stablecoinswap.stablecoinswap_order_book_tracker import StablecoinswapOrderBookTracker
# TODO implement
from hummingbot.market.stablecoinswap.stablecoinswap_in_flight_order cimport StablecoinswapInFlightOrder

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
        # ?
        self._next_nonce = None
        self._account_balances = {}

    @property
    def name(self) -> str:
        return "stablecoinswap"

    @property
    def order_books(self) -> Dict[str, OrderBook]:
        return self._order_book_tracker.order_books

    @property
    def status_dict(self) -> Dict[str, bool]:
        # TODO: change it somehow
        return {
            "order_books_initialized": self._order_book_tracker.ready,
            "account_balance": len(self._account_balances) > 0 if self._trading_required else True,
            "trading_rule_initialized": len(self._trading_rules) > 0 if self._trading_required else True
        }

    def ready(self) -> bool:
        return all(self.status_dict.values())

    @property
    def tracking_states(self) -> Dict[str, any]:
        return {
            key: value.to_json()
            for key, value in self._in_flight_orders.items()
        }

    def restore_tracking_states(self, saved_states: Dict[str, any]):
        self._in_flight_orders.update({
            key: StablecoinswapInFlightOrder.from_json(value)
            for key, value in saved_states.items()
        })

    async def get_active_exchange_markets(self) -> pd.DataFrame:
        """
        *required
        Used by the discovery strategy to read order books of all actively trading markets,
        and find opportunities to profit
        """
        # TODO
        return await CoinbaseProAPIOrderBookDataSource.get_active_exchange_markets()

    def get_all_balances(self) -> Dict[str, float]:
        return self._account_balances.copy()

    def _update_balances(self):
        self._account_balances = self.wallet.get_all_balances()

    async def start_network(self):
        # TODO: test
        if self._order_tracker_task is not None:
            self._stop_network()

        self._order_tracker_task = safe_ensure_future(self._order_book_tracker.start())
        if self._trading_required:
            self._status_polling_task = safe_ensure_future(self._status_polling_loop())
            tx_hashes = await self.wallet.current_backend.check_and_fix_approval_amounts(
                spender=self._wallet_spender_address
            )
            self._pending_approval_tx_hashes.update(tx_hashes)
            self._approval_tx_polling_task = safe_ensure_future(self._approval_tx_polling_loop())

    def _stop_network(self):
        if self._order_tracker_task is not None:
            self._order_tracker_task.cancel()
        if self._status_polling_task is not None:
            self._status_polling_task.cancel()
        if self._pending_approval_tx_hashes is not None:
            self._pending_approval_tx_hashes.clear()
        if self._approval_tx_polling_task is not None:
            self._approval_tx_polling_task.cancel()
        self._order_tracker_task = self._status_polling_task = self._approval_tx_polling_task = None

    async def stop_network(self):
        self._stop_network()

    async def check_network(self) -> NetworkStatus:
        if self._wallet.network_status is not NetworkStatus.CONNECTED:
            return NetworkStatus.NOT_CONNECTED

        # TODO: check if contract is tradable?

    cdef c_tick(self, double timestamp):
        cdef:
            int64_t last_tick = <int64_t>(self._last_timestamp / self._poll_interval)
            int64_t current_tick = <int64_t>(timestamp / self._poll_interval)

        self._tx_tracker.c_tick(timestamp)
        MarketBase.c_tick(self, timestamp)
        if current_tick > last_tick:
            if not self._poll_notifier.is_set():
                self._poll_notifier.set()
        self._last_timestamp = timestamp

    cdef object c_get_fee(self,
                          str base_currency,
                          str quote_currency,
                          object order_type,
                          object order_side,
                          object amount,
                          object price):
        cdef:
            int gas_estimate = 181000  # approximate gas usage for Swap func
            double transaction_cost_eth

        transaction_cost_eth = self._wallet.gas_price * gas_estimate / 1e18

        # TODO: poll contract fee and add as percent here

        return TradeFee(percent=0.0, flat_fees=[("ETH", transaction_cost_eth)])

    async def _status_polling_loop(self):
        while True:
            try:
                self._poll_notifier = asyncio.Event()
                await self._poll_notifier.wait()

                # TODO: add updates here
                # await safe_gather(
                #     self._update_market_order_status()
                # )
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    "Unexpected error while fetching account updates.",
                    exc_info=True,
                    app_warning_msg="Failed to fetch account updates on Stablecoinswap. Check network connection."
                )
                await asyncio.sleep(0.5)

    cdef double c_get_balance(self, str currency) except? -1:
        return float(self._account_balances.get(currency, 0.0))

    cdef double c_get_available_balance(self, str currency) except? -1:
        return float(self._account_balances.get(currency, 0.0))

    @staticmethod
    def split_symbol(symbol: str) -> Tuple[str, str]:
        try:
            quote_asset, base_asset = symbol.split("-")
            return base_asset, quote_asset
        except Exception:
            raise ValueError(f"Error parsing symbol {symbol}")

    cdef str c_buy(self,
                   str symbol,
                   object amount,
                   object order_type = OrderType.MARKET,
                   object price = s_decimal_0,
                   dict kwargs = {}):

        # only market order could be implemented
        if order_type is not OrderType.MARKET:
            raise NotImplementedError

        cdef:
            int64_t tracking_nonce = <int64_t>(time.time() * 1e6)
            str order_id = str(f"buy-{symbol}-{tracking_nonce}")

        safe_ensure_future(self.execute_buy(order_id, symbol, amount, order_type, price))
        return order_id

    async def execute_buy(self,
                          order_id: str,
                          symbol: str,
                          amount: Decimal,
                          order_type: OrderType,
                          price: Decimal) -> str:
        # only market order could be implemented
        if order_type is not OrderType.MARKET:
            raise NotImplementedError

        cdef:
            object q_amt = self.c_quantize_order_amount(symbol, amount)

        try:
            self.c_start_tracking_order(order_id, symbol, TradeType.BUY, q_amt, s_decimal_0)

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

        # only market order could be implemented
        if order_type is not OrderType.MARKET:
            raise NotImplementedError

        cdef:
            int64_t tracking_nonce = <int64_t>(time.time() * 1e6)
            str order_id = str(f"sell-{symbol}-{tracking_nonce}")

        safe_ensure_future(self.execute_sell(order_id, symbol, amount, order_type, price))
        return order_id

    async def execute_sell(self,
                           order_id: str,
                           symbol: str,
                           amount: Decimal,
                           order_type: OrderType,
                           price: Decimal) -> str:

        # only market order could be implemented
        if order_type is not OrderType.MARKET:
            raise NotImplementedError

        cdef:
            object q_amt = self.c_quantize_order_amount(symbol, amount)

        try:
            self.c_start_tracking_order(order_id, symbol, TradeType.SELL, order_type, q_amt, s_decimal_0)

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
        # TODO
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
        # TODO
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
        self._in_flight_orders[client_order_id] = StablecoinswapInFlightOrder(
            client_order_id=client_order_id,
            exchange_order_id=None,
            symbol=symbol,
            order_type=order_type,
            trade_type=trade_type,
            price=price,
            amount=amount
        )

    cdef c_stop_tracking_order(self, str order_id):
        if order_id in self._in_flight_orders:
            del self._in_flight_orders[order_id]
