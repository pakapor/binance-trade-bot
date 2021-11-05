import random
import sys
from pandas_ta.overlap import ema
from pandas_ta.utils import get_offset, verify_series
import pandas as pd
from datetime import timedelta

from binance_trade_bot.auto_trader import AutoTrader
from binance_trade_bot.models.coin import Coin
from binance_trade_bot.strategies.base.technical_indicator_strategy import TAStrategy


class Strategy(TAStrategy):
    def initialize(self):
        super().initialize()
        self.target_coins = [Coin(coin) for coin in self.config.SUPPORTED_COIN_LIST]
        self.all_coins =  self.target_coins.copy().append(self.config.BRIDGE.symbol)
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

        self.prev_current_date = self.get_current_date()

    def get_coin_ema_in_range(self, pair_symbol, start_date, end_date, range):
        prev_prices_raw, prev_prices_pd = self.get_prev_prices_in_range(pair_symbol, start_date, end_date, range)
        if prev_prices_raw is None or prev_prices_pd is None:
            return None, None

        ema_array = ema(prev_prices_pd["close"], range)
        if ema_array is None:
            return None, None

        # print(ema_array, range)
        return ema_array.iloc[-1], prev_prices_raw

    def get_current_date(self):
        current_date = self.manager.now()
        if self.config_time_frame == "min":
            current_date = current_date.replace(second=0, microsecond=0)
        elif self.config_time_frame == "hr":
            current_date = current_date.replace(minute=0, second=0, microsecond=0)
        elif self.config_time_frame == "day":
            current_date = current_date.replace(hour=0, minute=0, second=0, microsecond=0)
        return current_date

    def get_coin_fast_slow_ema(self, symbol):
        current_date = self.get_current_date()
        # self.logger.info(f"current_date: {current_date}")
        if self.prev_current_date == current_date:
            return None, None, None, None

        self.prev_current_date = current_date
        prev_date_fast = current_date - timedelta(minutes=self.config_fast_ema * self.multiplier)
        prev_date_slow = current_date - timedelta(minutes=self.config_slow_ema * self.multiplier)

        # self.logger.info(f"prev_date_fast: {prev_date_fast}, prev_date_slow: {prev_date_slow}")

        fast_ema, prev_prices_raw_fast = self.get_coin_ema_in_range(symbol + self.config.BRIDGE_SYMBOL, prev_date_fast, current_date, self.config_fast_ema)
        if prev_prices_raw_fast is None or len(prev_prices_raw_fast) == 0:
            return None, None, None, None

        slow_ema, prev_prices_raw_slow = self.get_coin_ema_in_range(symbol + self.config.BRIDGE_SYMBOL, prev_date_slow, current_date, self.config_slow_ema)
        if prev_prices_raw_slow is None or len(prev_prices_raw_slow) == 0:
            return None, None, None, None

        current_price = prev_prices_raw_fast[len(prev_prices_raw_fast)-1]

        return fast_ema, slow_ema, current_price, prev_prices_raw_slow

    def get_signal(self, coim_symbol):
        fast_ema, slow_ema, current_price, raw_prices = self.get_coin_fast_slow_ema(coim_symbol)
        if fast_ema is None:
            return None, None
        # print("fast_ema:", fast_ema, ", slow_ema:", slow_ema)

        trend = "bull" if fast_ema > slow_ema else "bear"
        if trend == "bull":
            signal = "buy" if current_price > fast_ema else "-"
        elif trend == "bear":
            signal = "sell" if current_price < fast_ema else "-"
        else:
            signal = "-"

        return signal, {"fast_ema": fast_ema, "slow_ema": slow_ema}#, "prices": raw_prices