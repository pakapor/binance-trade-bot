import random
import sys
from pandas_ta.overlap import ema
from pandas_ta.utils import get_offset, verify_series
import pandas as pd
from datetime import timedelta

from binance_trade_bot.auto_trader import AutoTrader
from binance_trade_bot.models.coin import Coin


class Strategy(AutoTrader):
    def initialize(self):
        super().initialize()
        # self.initialize_current_coin()
        self.target_coin = Coin(self.config.SUPPORTED_COIN_LIST[0])
        
        self.config_fast_ema = self.config.STRATEGY_CONFIG["fast_ema_period"]
        self.config_slow_ema = self.config.STRATEGY_CONFIG["slow_ema_period"]
        self.config_time_frame = self.config.STRATEGY_CONFIG["time_frame"]
        if self.config_time_frame == "min":
            self.multiplier = 1
        elif self.config_time_frame == "hr":
            self.multiplier = 60
        elif self.config_time_frame == "4hr":
            self.multiplier = 60*4
        elif self.config_time_frame == "day":
            self.multiplier = 60*24
        elif self.config_time_frame == "week":
            self.multiplier = 60*24*7
        elif self.config_time_frame == "month":
            self.multiplier = 60*24*30

    def scout(self):
        """
        Scout for potential jumps from the current coin to another coin
        """
        # check if previous buy order failed. If so, bridge scout for a new coin.
        # if self.failed_buy_order:
        #     self.bridge_scout()

        fast_ema, slow_ema, current_price = self.get_coin_ema(self.target_coin.symbol)
        signal = self.get_signal(current_price, fast_ema, slow_ema)

        current_coin = self.db.get_current_coin()
        if signal == "buy" and current_coin.symbol == self.config.BRIDGE.symbol:
            self.logger.info(f">> signal: {signal}, fast_ema: {fast_ema}, slow_ema: {slow_ema}, current_price: {current_price}")
            self.buy()

        elif signal == "sell" and current_coin.symbol == self.target_coin.symbol:
            self.logger.info(f">> signal: {signal}, fast_ema: {fast_ema}, slow_ema: {slow_ema}, current_price: {current_price}")
            self.sell()
    
    def get_coin_ema(self, symbol):
        current_date = self.manager.now()
        prev_date = current_date - timedelta(minutes=self.config_slow_ema * self.multiplier)

        prev_prices_raw = self.manager.get_ticker_price_in_range(symbol + self.config.BRIDGE_SYMBOL, prev_date, current_date, self.multiplier)
        if prev_prices_raw is None:
            return

        prev_prices = pd.DataFrame({"close": prev_prices_raw})
        current_price = prev_prices_raw[len(prev_prices_raw)-1]

        fast_ema_array = ema(prev_prices["close"], self.config_fast_ema)
        if fast_ema_array is None:
            return
        fast_ema = fast_ema_array[self.config_fast_ema-1]

        slow_ema_array = ema(prev_prices["close"], self.config_slow_ema)
        if slow_ema_array is None:
            return
        slow_ema = slow_ema_array[self.config_slow_ema-1]

        return fast_ema, slow_ema, current_price

    def get_signal(self, current_price, fast_ema, slow_ema):
        trend = "bull" if fast_ema > slow_ema else "bear"
        if trend == "bull":
            signal = "buy" if current_price > fast_ema else "-"
        elif trend == "bear":
            signal = "sell" if current_price < fast_ema else "-"
        else:
            signal = "-"
        return signal

    def buy(self):
        result = self.manager.buy_alt(self.target_coin, self.config.BRIDGE, self.manager.get_buy_price(
            self.target_coin + self.config.BRIDGE))
        if result is not None:
            self.db.set_current_coin(self.target_coin)

    def sell(self):
        result = self.manager.sell_alt(self.target_coin, self.config.BRIDGE, self.manager.get_buy_price(
            self.config.BRIDGE + self.target_coin))
        if result is not None:
            self.db.set_current_coin(self.config.BRIDGE)

    # def bridge_scout(self):
    #     current_coin = self.db.get_current_coin()
    #     if self.manager.get_currency_balance(current_coin.symbol) > self.manager.get_min_notional(
    #         current_coin.symbol, self.config.BRIDGE.symbol
    #     ):
    #         # Only scout if we don't have enough of the current coin
    #         return
    #     new_coin = super().bridge_scout()
    #     if new_coin is not None:
    #         self.db.set_current_coin(new_coin)

    def initialize_current_coin(self):
        return
        """
        Decide what is the current coin, and set it up in the DB.
        """
        if self.db.get_current_coin() is None:
            current_coin_symbol = self.config.CURRENT_COIN_SYMBOL
            if not current_coin_symbol:
                # random.choice(self.config.SUPPORTED_COIN_LIST)
                current_coin_symbol = self.target_coin

            self.logger.info(f"Setting initial coin to {current_coin_symbol}")

            if current_coin_symbol not in self.config.SUPPORTED_COIN_LIST:
                sys.exit(
                    "***\nERROR!\nSince there is no backup file, a proper coin name must be provided at init\n***")
            self.db.set_current_coin(current_coin_symbol)

            # if we don't have a configuration, we selected a coin at random... Buy it so we can start trading.
            if self.config.CURRENT_COIN_SYMBOL == "":
                current_coin = self.db.get_current_coin()
                self.logger.info(f"Purchasing {current_coin} to begin trading")
                self.manager.buy_alt(
                    current_coin, self.config.BRIDGE, self.manager.get_buy_price(
                        current_coin + self.config.BRIDGE)
                )
                self.logger.info("Ready to start trading")
            else:
                current_balance = self.manager.get_currency_balance(
                    current_coin_symbol)
                sell_price = self.manager.get_sell_price(
                    current_coin_symbol + self.config.BRIDGE.symbol)
                if current_balance is not None and current_balance * sell_price < self.manager.get_min_notional(current_coin_symbol, self.config.BRIDGE.symbol):
                    self.logger.info(
                        f"Purchasing {current_coin_symbol} to begin trading")
                    current_coin = self.db.get_current_coin()
                    self.manager.buy_alt(
                        current_coin, self.config.BRIDGE, self.manager.get_buy_price(
                            current_coin + self.config.BRIDGE)
                    )
                    self.logger.info("Ready to start trading")
