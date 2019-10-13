from libc.stdint cimport int64_t
from hummingbot.market.market_base cimport MarketBase
from hummingbot.core.data_type.transaction_tracker cimport TransactionTracker


cdef class StablecoinswapRelayMarket(MarketBase):
    cdef:
        str _wallet_spender_address
        object _wallet
        object _provider
        object _weth_token
        object _order_book_tracker
        dict _account_balances
        object _ev_loop
        object _poll_notifier
        double _last_timestamp
        double _last_failed_limit_order_timestamp
        double _last_update_limit_order_timestamp
        double _last_update_market_order_timestamp
        double _last_update_trading_rules_timestamp
        double _poll_interval
        # object _contract_fees
        dict _in_flight_limit_orders
        dict _in_flight_market_orders
        object _in_flight_pending_limit_orders
        object _in_flight_cancels
        object _in_flight_pending_cancels
        object _order_expiry_queue
        TransactionTracker _tx_tracker
        object _w3
        object _exchange
        object _coordinator
        bint _use_coordinator
        bint _pre_emptive_soft_cancels
        dict _withdraw_rules
        dict _trading_rules
        object _pending_approval_tx_hashes
        public object _status_polling_task
        public object _user_stream_event_listener_task
        public object _approval_tx_polling_task
        public object _order_tracker_task

    # cdef c_start_tracking_order(self,
    #                             str order_id,
    #                             str symbol,
    #                             object trade_type,
    #                             object order_type,
    #                             object amount,
    #                             object price,
    #                             str tx_hash)
    # cdef c_stop_tracking_order(self, str order_id)
