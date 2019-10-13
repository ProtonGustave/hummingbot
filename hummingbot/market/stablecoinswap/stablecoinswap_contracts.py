import os
import json
from typing import (
    List,
)
from web3 import Web3
from web3.contract import (
    Contract,
)
from decimal import Decimal

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
DAI_ADDRESS = "0x89d24A6b4CcB1B6fAA2625fE562bDD9a23260359"
USDC_ADDRESS = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
PAX_ADDRESS = "0x8E870D67F660D95d5be530380D0eC0bd388289E1"
TUSD_ADDRESS = "0x0000000000085d4780B73119b644AE5ecd22b376"

STABLECOINSWAP_ADDRESS = "0x652511eBd2C77AA00BC3F0b910928418708aD2Ee"
PRICE_ORACLE_ADDRESS = "0x0bd23A23aD1e8f963EBf4A948c523bBd1267186e"

with open(os.path.join(os.path.dirname(__file__), 'stablecoinswap_abi.json')) as stablecoinswap_abi:
    stl_abi: List[any] = json.load(stablecoinswap_abi)

with open(os.path.join(os.path.dirname(__file__), 'price_oracle_abi.json')) as price_oracle_abi:
    oracle_abi: List[any] = json.load(price_oracle_abi)


class PriceOracle:
    PRICE_MULTIPLIER = 1e8

    def __init__(self,
                 w3: Web3,
                 address: str = PRICE_ORACLE_ADDRESS):
        self._address = address
        self._w3: Web3 = w3
        self._abi: List[any] = oracle_abi
        self._contract: Contract = self._w3.eth.contract(address=self._address, abi=self._abi)

    def get_supported_tokens(self) -> List[str]:
        """Return all contract supported tokens."""
        supported_tokens: List[str] = []

        for token_id in range(5):
            token_address: str = self._contract.functions.supportedTokens(token_id).call()

            if token_address == ZERO_ADDRESS:
                break

            supported_tokens.append(token_address)

        return supported_tokens

    def normalized_token_price(self, token_address) -> int:
        return self._contract.functions.normalized_token_prices(token_address).call()


class Stablecoinswap:
    # constants from contract
    FEE_MULTIPLIER: 1e5
    EXCHANGE_RATE_MULTIPLIER: 1e22
    TOKEN_PRICE_MULTIPLIER: 1e8

    def __init__(self,
                 w3: Web3,
                 address: str = STABLECOINSWAP_ADDRESS):
        self._address = address
        self._w3: Web3 = w3
        self._abi: List[any] = stl_abi
        self._contract: Contract = self._w3.eth.contract(address=self._address, abi=self._abi)

    @staticmethod
    def get_address_by_symbol(symbol):
        symbol.upper()

        if symbol is "DAI":
            return DAI_ADDRESS
        elif symbol is "TUSD":
            return TUSD_ADDRESS
        elif symbol is "USDC":
            return USDC_ADDRESS
        elif symbol is "PAX":
            return PAX_ADDRESS
        else:
            raise Exception("No such symbol found")

    @staticmethod
    def get_symbol_by_address(address):
        if address is DAI_ADDRESS:
            return "DAI"
        elif address is TUSD_ADDRESS:
            return "TUSD"
        elif address is USDC_ADDRESS:
            return "USDC"
        elif address is PAX_ADDRESS:
            return "PAX"
        else:
            raise Exception("No such address found")

    def is_trading_allowed(self) -> bool:
        return self._permission('tradingAllowed')

    def _permission(self, permission_name) -> bool:
        return self._contract.functions.permissions(permission_name).call()

    def is_token_for_buy(self, token_address: str) -> bool:
        """Check if it's possible to buy token."""
        return self._contract.functions.outputTokens(token_address).call()

    def is_token_for_sell(self, token_address: str) -> bool:
        """Check if it's possible to sell token."""
        return self._contract.functions.inputTokens(token_address).call()

    def token_exchange_rate_after_fees(self, input_token, output_token) -> int:
        """Return exchange rate after fees."""
        return self._contract.functions.tokenExchangeRateAfterFees(
                input_token, output_token).call()

    def token_output_amount_after_fees(self, input_token_amount, input_token,
            output_token) -> int:
        return self._contract.functions.tokenOutputAmountAfterFees(
                input_token_amount, input_token, output_token).call()

    def _get_trade_fee(self) -> Decimal:
        return self._contract.functions.fees('tradeFee').call()

    def _get_owner_fee(self) -> Decimal:
        return self._contract.functions.fees('ownerFee').call()

    def get_fees(self) -> Decimal:
        trade_fee = self._get_trade_fee()
        owner_fee = self._get_owner_fee()

        return trade_fee + owner_fee

    def get_exchange_rate(self, input_token, output_token) -> Decimal:
        fees = self.get_fees()
        rate_after_fees = self.token_exchange_rate_after_fees(input_token,
                output_token)

        return rate_after_fees / fees

    # def get_all_trading_pairs(self, token_addresses: List[str]) -> List[str]:
    #     """Check if token can be both sold and bought,
    #     then create pairs combinations
    #
    #     Pair format is DAI-TUSD
    #     """
    #     # filter unknown/untraidable tokens first
    #     matched_tokens: List[str] = []
    #
    #     for token_address in token_addresses:
    #         # find token name
    #         token_name: str = None
    #
    #         if token_address == DAI_ADDRESS:
    #             token_name = "DAI"
    #         elif token_address == PAX_ADDRESS:
    #             token_name = "PAX"
    #         elif token_address == TUSD_ADDRESS:
    #             token_name = 'TUSD'
    #         elif token_address == USDC_ADDRESS:
    #             token_name = 'USDC'
    #         else:
    #             break
    #
    #         # don't add already added token
    #         try:
    #             matched_tokens.index(token_name)
    #             break
    #         except ValueError:
    #             pass
    #
    #         # check if token is tradable
    #         if self.is_token_for_sell(token_address) is not True:
    #             break
    #
    #         if self.is_token_for_buy(token_address) is not True:
    #             break
    #
    #         matched_tokens.append(token_name)
    #
    #     if len(matched_tokens) < 2:
    #         return []
    #
    #     matched_tokens.sort()
    #
    #     # combine names to get traiding pairs
    #     pairs: List[str] = []
    #     tokens_num = len(matched_tokens)
    #
    #     for i in range(tokens_num):
    #         for j in range(i + 1, tokens_num):
    #             pairs.append(f"{matched_tokens[i]}-{matched_tokens[j]}")
    #
    #     return pairs

    @property
    def abi(self) -> List[any]:
        return self._abi

    @property
    def contract(self) -> Contract:
        return self._contract

    @property
    def address(self) -> str:
        return self._address