import random
import sys
from pandas_ta.overlap import ema
from pandas_ta.utils import get_offset, verify_series
import pandas as pd
from datetime import timedelta

from binance_trade_bot.auto_trader import AutoTrader
from binance_trade_bot.models.coin import Coin


class TAStrategy(AutoTrader):
    def initialize(self):
        super().initialize()
        self.target_coins = [Coin(coin) for coin in self.config.SUPPORTED_COIN_LIST]
        self.all_coins = [Coin(coin) for coin in self.config.SUPPORTED_COIN_LIST]
        self.all_coins.append(self.config.BRIDGE)
        self.prev_signal = {}
        for coin in self.target_coins:
            self.prev_signal[coin.symbol] = 'x'

    def scout(self):
        self.trigger_by_signal()

    def trigger_by_signal(self):
        current_date = self.manager.now()

        for coin in self.target_coins:
            signal, signal_info = self.get_signal(coin.symbol)

            if signal is None:
                # if self.prev_signal[coin.symbol] != signal:
                    # self.logger.info(f"{current_date} >> 'Waiting for the candle to be closed', target_coin: {coin.symbol}")
                self.prev_signal[coin.symbol] = signal
                continue

            if signal == '-':
                # if self.prev_signal[coin.symbol] != signal:
                #     self.logger.info(f"{current_date} >> signal: 'Do Nothing': {signal_info}, target_coin: {coin.symbol}")
                self.prev_signal[coin.symbol] = signal
                continue

            self.prev_signal[coin.symbol] = signal

            current_date = self.manager.now()
            prev_date = current_date - timedelta(minutes=1 * self.multiplier)
            prev_prices_raw = self.manager.get_ticker_price_in_range(coin.symbol + self.config.BRIDGE_SYMBOL, prev_date, current_date, self.multiplier)
            current_price = prev_prices_raw[len(prev_prices_raw)-1]
        
            target_coin_balance = self.manager.get_currency_balance(coin.symbol)
            bridge_coin_balance = self.manager.get_currency_balance(self.config.BRIDGE.symbol)
            buy_percent = self.config.STRATEGY_CONFIG['buy_percent']
            if buy_percent > 0:
                # print("target_coin_balance:", target_coin_balance)
                # print("bridge_coin_balance:", bridge_coin_balance)
                current_balances = self.manager.get_balances(self.all_coins)
                usd_balance = sum([balance for key, balance in self.manager.get_usd_balances(current_balances).items()])
                buy_amount = min((buy_percent/100)*usd_balance, bridge_coin_balance)
                # self.logger.info(f"{current_date} >> Balances, amount: {current_balances}, USD value: {usd_balance}")
            else:
                buy_amount = min(self.config.STRATEGY_CONFIG['buy_amount'], bridge_coin_balance)

            min_notional = self.manager.get_min_notional(coin.symbol, self.config.BRIDGE.symbol)
            min_qty = min_notional/current_price
            # print("min_qty:", min_qty)

            if signal == "buy" and target_coin_balance <= min_qty:
                if bridge_coin_balance >= buy_amount and buy_amount >= min_notional:
                    self.logger.info(f"{current_date} >> {coin.symbol}, signal: {signal}, current_price: {current_price}, {signal_info}")
                    self.buy(coin, buy_amount)
                    self.logger.info(f"{current_date} >> current balances: {self.manager.get_balances(self.all_coins)}\n")
                # else:
                #     self.logger.info(f"{current_date} >> {coin.symbol}, signal: {signal}, 'Not Enought Money!!\n")

            elif signal == "sell" and target_coin_balance > min_qty:
                self.logger.info(f"{current_date} >> {coin.symbol}, signal: {signal}, current_price: {current_price}, {signal_info}")
                self.sell(coin)
                self.logger.info(f"{current_date} >> current balances: {self.manager.get_balances(self.all_coins)}\n")

    def get_signal(self, coim_symbol):
        return "-"
        
    def buy(self, coin: Coin, buy_amount):
        buy_quantity = self.manager._buy_quantity(coin.symbol, self.config.BRIDGE.symbol, buy_amount)
        result = self.manager.buy_alt(coin, self.config.BRIDGE, self.manager.get_buy_price(
            coin + self.config.BRIDGE), buy_quantity)
        if result is not None:
            self.db.set_current_coin(coin)

    def sell(self, coin: Coin):
        sell_quantity = self.manager._sell_quantity(coin.symbol, self.config.BRIDGE.symbol)
        if sell_quantity == 0:
            return

        result = self.manager.sell_alt(coin, self.config.BRIDGE, self.manager.get_sell_price(
            coin + self.config.BRIDGE))
        if result is not None:
            self.db.set_current_coin(self.config.BRIDGE)

    def get_prev_prices_in_range(self, pair_symbol, start_date, end_date, range):
        prev_prices_raw = self.manager.get_ticker_price_in_range(pair_symbol, start_date, end_date, self.multiplier)
        # self.logger.info(f"start_date: {start_date}, end_date: {end_date}, prev_prices_raw: {prev_prices_raw}")
        if prev_prices_raw is None or len(prev_prices_raw) == 0:
            return None, None

        prev_prices_pd = pd.DataFrame({"close": prev_prices_raw})
        return prev_prices_raw, prev_prices_pd

    def initialize_current_coin(self):
        return
