#!/usr/bin/env python

import asyncio
from collections import (
    deque,
    defaultdict
)
import logging
import time
from typing import (
    Deque,
    Dict,
    List,
    Optional
)

from web3 import Web3
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import (
    OrderBookMessage,
    OrderBookMessageType
)
from hummingbot.core.data_type.order_book_tracker import (
    OrderBookTracker,
    OrderBookTrackerDataSourceType
)
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.logger import HummingbotLogger
from hummingbot.market.stablecoinswap.stablecoinswap_api_order_book_data_source import StablecoinswapAPIOrderBookDataSource


class StablecoinswapOrderBookTracker(OrderBookTracker):
    _stlobt_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._stlobt_logger is None:
            cls._stlobt_logger = logging.getLogger(__name__)
        return cls._stlobt_logger

    def __init__(self,
                 w3: Web3,
                 data_source_type: OrderBookTrackerDataSourceType = OrderBookTrackerDataSourceType.EXCHANGE_API,
                 symbols: Optional[List[str]] = None):
        super().__init__(data_source_type=data_source_type)
        self._data_source: Optional[OrderBookTrackerDataSource] = None
        self._saved_message_queues: Dict[str, Deque[OrderBookMessage]] = defaultdict(lambda: deque(maxlen=1000))
        self._w3 = w3
        self._symbols: Optional[List[str]] = symbols

    @property
    def data_source(self) -> OrderBookTrackerDataSource:
        # TODO: change Type to BLOCKCHAIN
        if not self._data_source:
            if self._data_source_type is OrderBookTrackerDataSourceType.EXCHANGE_API:
                self._data_source = StablecoinswapAPIOrderBookDataSource(symbols=self._symbols, w3 = self._w3)
            else:
                raise ValueError(f"data_source_type {self._data_source_type} is not supported.")
        return self._data_source

    @property
    def exchange_name(self) -> str:
        return "stablecoinswap"

    async def start(self):
        # self._order_book_trade_listener_task = safe_ensure_future(
        #     self.data_source.listen_for_trades(self._ev_loop, self._order_book_trade_stream)
        # )
        # self._order_book_diff_listener_task = safe_ensure_future(
        #     self.data_source.listen_for_order_book_diffs(self._ev_loop, self._order_book_diff_stream)
        # )
        self._order_book_snapshot_listener_task = safe_ensure_future(
            self.data_source.listen_for_order_book_snapshots(self._ev_loop, self._order_book_snapshot_stream)
        )
        self._refresh_tracking_task = safe_ensure_future(
            self._refresh_tracking_loop()
        )
        # self._order_book_diff_router_task = safe_ensure_future(
        #     self._order_book_diff_router()
        # )
        self._order_book_snapshot_router_task = safe_ensure_future(
            self._order_book_snapshot_router()
        )
