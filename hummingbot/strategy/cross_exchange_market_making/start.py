from typing import (
    List,
    Tuple
)

from hummingbot.client.config.global_config_map import global_config_map
from hummingbot.strategy.market_symbol_pair import MarketSymbolPair
from hummingbot.strategy.cross_exchange_market_making.cross_exchange_market_pair import CrossExchangeMarketPair
from hummingbot.strategy.cross_exchange_market_making.cross_exchange_market_making import CrossExchangeMarketMakingStrategy
from hummingbot.strategy.cross_exchange_market_making.cross_exchange_market_making_config_map import cross_exchange_market_making_config_map


def start(self):
    maker_market = cross_exchange_market_making_config_map.get("maker_market").value.lower()
    taker_market = cross_exchange_market_making_config_map.get("taker_market").value.lower()
    raw_maker_trading_pair = cross_exchange_market_making_config_map.get("maker_market_symbol").value
    raw_taker_trading_pair = cross_exchange_market_making_config_map.get("taker_market_symbol").value
    min_profitability = cross_exchange_market_making_config_map.get("min_profitability").value
    order_amount = cross_exchange_market_making_config_map.get("order_amount").value
    strategy_report_interval = global_config_map.get("strategy_report_interval").value
    limit_order_min_expiration = cross_exchange_market_making_config_map.get("limit_order_min_expiration").value
    cancel_order_threshold = cross_exchange_market_making_config_map.get("cancel_order_threshold").value
    active_order_canceling = cross_exchange_market_making_config_map.get("active_order_canceling").value
    adjust_order_enabled = cross_exchange_market_making_config_map.get("adjust_order_enabled").value
    top_depth_tolerance = cross_exchange_market_making_config_map.get("top_depth_tolerance").value
    order_size_taker_volume_factor = cross_exchange_market_making_config_map.get("order_size_taker_volume_factor").value
    order_size_taker_balance_factor = cross_exchange_market_making_config_map.get("order_size_taker_balance_factor").value
    order_size_portfolio_ratio_limit = cross_exchange_market_making_config_map.get("order_size_portfolio_ratio_limit").value
    anti_hysteresis_duration = cross_exchange_market_making_config_map.get("anti_hysteresis_duration").value

    # check if top depth tolerance is a list or if trade size override exists
    if isinstance(top_depth_tolerance, list) or "trade_size_override" in cross_exchange_market_making_config_map:
        self._notify("Current config is not compatible with cross exchange market making strategy. Please reconfigure")
        return

    market_names: List[Tuple[str, List[str]]] = [
        (maker_market, [raw_maker_trading_pair]),
        (taker_market, [raw_taker_trading_pair]),
    ]
    try:
        maker_assets: Tuple[str, str] = self._initialize_market_assets(maker_market, [raw_maker_trading_pair])[0]
        taker_assets: Tuple[str, str] = self._initialize_market_assets(taker_market, [raw_taker_trading_pair])[0]
    except ValueError as e:
        self._notify(str(e))
        return
    self._initialize_wallet(token_symbols=list(set(maker_assets + taker_assets)))
    self._initialize_markets(market_names)
    self.assets = set(maker_assets + taker_assets)
    maker_data = [self.markets[maker_market], raw_maker_trading_pair] + list(maker_assets)
    taker_data = [self.markets[taker_market], raw_taker_trading_pair] + list(taker_assets)
    maker_market_symbol_pair = MarketSymbolPair(*maker_data)
    taker_market_symbol_pair = MarketSymbolPair(*taker_data)
    self.market_symbol_pairs = [maker_market_symbol_pair, taker_market_symbol_pair]
    self.market_pair = CrossExchangeMarketPair(maker=maker_market_symbol_pair, taker=taker_market_symbol_pair)

    strategy_logging_options = (
        CrossExchangeMarketMakingStrategy.OPTION_LOG_CREATE_ORDER
        | CrossExchangeMarketMakingStrategy.OPTION_LOG_ADJUST_ORDER
        | CrossExchangeMarketMakingStrategy.OPTION_LOG_MAKER_ORDER_FILLED
        | CrossExchangeMarketMakingStrategy.OPTION_LOG_REMOVING_ORDER
        | CrossExchangeMarketMakingStrategy.OPTION_LOG_STATUS_REPORT
        | CrossExchangeMarketMakingStrategy.OPTION_LOG_MAKER_ORDER_HEDGED
    )
    self.strategy = CrossExchangeMarketMakingStrategy(
        market_pairs=[self.market_pair],
        min_profitability=min_profitability,
        status_report_interval=strategy_report_interval,
        logging_options=strategy_logging_options,
        order_amount=order_amount,
        limit_order_min_expiration=limit_order_min_expiration,
        cancel_order_threshold=cancel_order_threshold,
        active_order_canceling=active_order_canceling,
        adjust_order_enabled=adjust_order_enabled,
        top_depth_tolerance=top_depth_tolerance,
        order_size_taker_volume_factor=order_size_taker_volume_factor,
        order_size_taker_balance_factor=order_size_taker_balance_factor,
        order_size_portfolio_ratio_limit=order_size_portfolio_ratio_limit,
        anti_hysteresis_duration=anti_hysteresis_duration
    )
