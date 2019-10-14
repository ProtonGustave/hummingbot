#!/usr/bin/env python

import aiohttp
import asyncio
import logging
import pandas as pd
import time
from typing import (
    Any,
    Dict,
    List,
    Optional,
)

from web3 import Web3
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.order_book_tracker_entry import OrderBookTrackerEntry
from hummingbot.logger import HummingbotLogger
from hummingbot.market.stablecoinswap.stablecoinswap_order_book import StablecoinswapOrderBook
from hummingbot.market.stablecoinswap.stablecoinswap_market import StablecoinswapMarket
import hummingbot.market.stablecoinswap.stablecoinswap_contracts as stablecoinswap_contracts
from hummingbot.wallet.ethereum.erc20_token import ERC20Token
from hummingbot.wallet.ethereum.ethereum_chain import EthereumChain

class StablecoinswapAPIOrderBookDataSource(OrderBookTrackerDataSource):

    MESSAGE_TIMEOUT = 30.0
    PING_TIMEOUT = 10.0

    _stlaobds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._stlaobds_logger is None:
            cls._stlaobds_logger = logging.getLogger(__name__)
        return cls._stlaobds_logger

    def __init__(self,
                 w3: Web3,
                 symbols: Optional[List[str]] = None):
        super().__init__()
        self._symbols: Optional[List[str]] = symbols
        self._w3 = w3
        self._stl_cont = stablecoinswap_contracts.Stablecoinswap(self._w3)
        self._oracle_cont = stablecoinswap_contracts.PriceOracle(self._w3)

    # TODO
    async def get_active_exchange_markets(cls) -> pd.DataFrame:
        pass

    async def get_trading_pairs(self) -> List[str]:
        if not self._symbols:
            try:
                active_markets: pd.DataFrame = await self.get_active_exchange_markets()
                self._symbols = active_markets.index.tolist()
            except Exception:
                self._symbols = []
                self.logger().network(
                    f"Error getting active exchange information.",
                    exc_info=True,
                    app_warning_msg=f"Error getting active exchange information. Check network connection."
                )
        return self._symbols

    async def get_snapshot(self, trading_pair: str) -> Dict[str, Any]:

        # TODO: comment it
        # fetch rate(order price) and contract balances(order amount) 
        base_asset, quote_asset = StablecoinswapMarket. \
                split_symbol(trading_pair)
        base_asset_address = stablecoinswap_contracts. \
                Stablecoinswap.get_address_by_symbol(base_asset)
        quote_asset_address = stablecoinswap_contracts. \
                Stablecoinswap.get_address_by_symbol(quote_asset)

        base_asset_token = ERC20Token(self._w3, base_asset_address,
                EthereumChain.MAIN_NET) 
        quote_asset_token = ERC20Token(self._w3, quote_asset_address,
                EthereumChain.MAIN_NET)
        base_asset_balance = base_asset_token._contract.functions.balanceOf(
                stablecoinswap_contracts.STABLECOINSWAP_ADDRESS).call()
        quote_asset_balance = quote_asset_token._contract.functions.balanceOf(
                stablecoinswap_contracts.STABLECOINSWAP_ADDRESS).call()
        base_asset_decimals = base_asset_token._contract.functions.decimals().call()
        quote_asset_decimals = quote_asset_token._contract.functions.decimals().call()
        buy_amount = quote_asset_balance / 10 ** quote_asset_decimals
        sell_amount = base_asset_balance / 10 ** base_asset_decimals

        base_asset_normalized_price = self._oracle_cont. \
                normalized_token_price(base_asset_address) / (10 ** (18 - base_asset_decimals))
        quote_asset_normalized_price = self._oracle_cont. \
                normalized_token_price(quote_asset_address) / (10 ** (18 - quote_asset_decimals))
        buy_rate = quote_asset_normalized_price / base_asset_normalized_price
        sell_rate = base_asset_normalized_price / quote_asset_normalized_price

        snapshot: Dict[str, Any] = {
                "bids": [{
                    "price": buy_rate,
                    "amount": buy_amount
                    }],
                "asks": [{
                    "price": sell_rate,
                    "amount": sell_amount
                    }],
                }

        return snapshot

    async def get_tracking_pairs(self) -> Dict[str, OrderBookTrackerEntry]:
        # Get the currently active markets
        async with aiohttp.ClientSession() as client:
            trading_pairs: List[str] = await self.get_trading_pairs()
            retval: Dict[str, OrderBookTrackerEntry] = {}

            number_of_pairs: int = len(trading_pairs)
            for index, trading_pair in enumerate(trading_pairs):
                try:
                    snapshot: Dict[str, Any] = await self.get_snapshot(trading_pair)
                    snapshot_msg: OrderBookMessage = StablecoinswapOrderBook.snapshot_message_from_exchange(
                        snapshot,
                        metadata={"symbol": trading_pair},
                        timestamp=time.time()
                    )
                    order_book: OrderBook = self.order_book_create_function()
                    order_book.apply_snapshot(snapshot_msg.bids, snapshot_msg.asks, snapshot_msg.update_id)
                    retval[trading_pair] = OrderBookTrackerEntry(trading_pair, snapshot_msg.timestamp, order_book)
                    self.logger().info(f"Initialized order book for {trading_pair}. "
                                       f"{index + 1}/{number_of_pairs} completed.")
                    await asyncio.sleep(10.0)
                except Exception:
                    self.logger().error(f"Error getting snapshot for {trading_pair}. ", exc_info=True)
                    await asyncio.sleep(5)
            return retval

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                trading_pairs: List[str] = await self.get_trading_pairs()

                for trading_pair in trading_pairs:
                    try:
                        snapshot: Dict[str, Any] = await self.get_snapshot(trading_pair)
                        snapshot_message: OrderBookMessage = StablecoinswapOrderBook.snapshot_message_from_exchange(
                                snapshot,
                                metadata={"symbol": trading_pair},
                                timestamp=time.time()
                                )
                        output.put_nowait(snapshot_message)
                        self.logger().debug(f"Saved order book snapshot for {trading_pair}")
                        await asyncio.sleep(5.0)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        self.logger().error("Unexpected error.", exc_info=True)
                        await asyncio.sleep(5.0)
                    this_hour: pd.Timestamp = pd.Timestamp.utcnow().replace(minute=0, second=0, microsecond=0)
                    next_hour: pd.Timestamp = this_hour + pd.Timedelta(hours=1)
                    delta: float = next_hour.timestamp() - time.time()
                    await asyncio.sleep(delta)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error.", exc_info=True)
                await asyncio.sleep(5.0)

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        pass

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        pass
