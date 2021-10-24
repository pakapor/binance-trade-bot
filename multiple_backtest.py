from datetime import datetime

from binance_trade_bot import backtest
from binance_trade_bot.config import Config

if __name__ == "__main__":
    config = Config()
    
    history = []
    start_time = config.BACKTEST_START_DATE
    end_time = config.BACKTEST_END_DATE
    start_balances = config.BACKTEST_START_BALANCES.copy() or None
    print(f"BACKTEST from {start_time} to {end_time}")
    current_date = start_time.strftime("%d/%m/%Y")

    for manager in backtest(start_time, end_time, config.BACKTEST_INTERVAL, config.BACKTEST_YIELD_INTERVAL, start_balances, None, config):
        btc_value = manager.collate_coins("BTC")
        bridge_value = manager.collate_coins(manager.config.BRIDGE.symbol)
        btc_fees_value = manager.collate_fees("BTC")
        bridge_fees_value = manager.collate_fees(manager.config.BRIDGE.symbol)
        trades = manager.trades
        history.append((btc_value, bridge_value, trades, btc_fees_value, bridge_fees_value))
        btc_diff = round((btc_value - history[0][0]) / history[0][0] * 100, 3)
        bridge_diff = round((bridge_value - history[0][1]) / history[0][1] * 100, 3)
        if manager.datetime.strftime("%d/%m/%Y") != current_date and False:
            current_date = manager.datetime.strftime("%d/%m/%Y")
            print("------")
            print("TIME:", manager.datetime)
            print("TRADES:", trades)
            #print("PAID FEES:", manager.paid_fees)
            #print("BTC FEES VALUE:", btc_fees_value)
            print(f"{manager.config.BRIDGE.symbol} FEES VALUE:", bridge_fees_value)
            #print("BALANCES:", manager.balances)
            print("BTC VALUE:", btc_value, f"({btc_diff}%)")
            print(f"{manager.config.BRIDGE.symbol} VALUE:", bridge_value, f"({bridge_diff}%)")
            print("------")
    print("------")
    print("TIME:", manager.datetime)
    print("TRADES:", trades)
    print("POSITIVE COIN JUMPS:", manager.positve_coin_jumps)
    print("NEVATIVE COIN JUMPS:", manager.negative_coin_jumps)
    #print("PAID FEES:", manager.paid_fees)
    #print("BTC FEES VALUE:", btc_fees_value)
    print(f"{manager.config.BRIDGE.symbol} FEES VALUE:", bridge_fees_value)
    total_start_balance = sum([value for key, value in config.BACKTEST_START_BALANCES.items()])
    print("START USD BALANCE:", total_start_balance)
    print("BALANCES:", manager.balances)
    print("USD BALANCES:", manager.get_usd_balances(manager.balances))
    # print("BTC VALUE:", btc_value, f"({btc_diff}%)")
    
    bridge_diff = round((bridge_value / total_start_balance)*100, 3)
    print(f"{manager.config.BRIDGE.symbol} VALUE:", bridge_value, f"({bridge_diff}%)")
    print("------")
