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
        # self.initialize_current_coin()
        self.target_coins = [Coin(coin) for coin in self.config.SUPPORTED_COIN_LIST]
        
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

    def get_coin_ema(self, symbol):
        current_date = self.manager.now()
        prev_date_fast = current_date - timedelta(minutes=self.config_fast_ema * self.multiplier)
        prev_date_slow = current_date - timedelta(minutes=self.config_slow_ema * self.multiplier)

        prev_prices_raw_fast = self.manager.get_ticker_price_in_range(symbol + self.config.BRIDGE_SYMBOL, prev_date_fast, current_date, self.multiplier)
        if prev_prices_raw_fast is None or len(prev_prices_raw_fast) == 0:
            return None, None, None

        prev_prices_raw_slow = self.manager.get_ticker_price_in_range(symbol + self.config.BRIDGE_SYMBOL, prev_date_slow, current_date, self.multiplier)
        if prev_prices_raw_slow is None or len(prev_prices_raw_slow) == 0:
            return None, None, None

        prev_prices_fast = pd.DataFrame({"close": prev_prices_raw_fast})
        prev_prices_slow = pd.DataFrame({"close": prev_prices_raw_slow})
        current_price = prev_prices_raw_fast[len(prev_prices_raw_fast)-1]

        fast_ema_array = ema(prev_prices_fast["close"], self.config_fast_ema)
        if fast_ema_array is None:
            return None, None, None
        fast_ema = fast_ema_array[self.config_fast_ema-1]

        slow_ema_array = ema(prev_prices_slow["close"], self.config_slow_ema)
        if slow_ema_array is None:
            return None, None, None
        slow_ema = slow_ema_array[self.config_slow_ema-1]

        return fast_ema, slow_ema, current_price

    def get_signal(self, coim_symbol):
        fast_ema, slow_ema, current_price = self.get_coin_ema(coim_symbol)
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

        return signal, {"fast_ema": fast_ema, "slow_ema": slow_ema}