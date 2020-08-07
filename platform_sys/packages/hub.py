from platform_sys.account import account_init
from platform_sys.account.cash import cash_account
from platform_sys.account.stock import stock_account
from platform_sys.account.settings import close_market_adjust, open_market_adjust
from platform_sys.packages.order_hub import order_hub
from platform_sys.data.data_prepare import market_data_df, trading_date_df
from platform_sys import strategy
import pandas as pd

import shelve
import json
with open('./platform_sys/settings/transaction_settings.json', 'r') as f:
    transaction_settings = json.load(f)


class Hub:
    def __init__(self, config: 'class', **kwargs: 'hyperparameters'):

        # overwrite order(from lower to higher): settings.json -> config cls -> hyperparameters
        for account in config.account_list:
            # currently, this frame only for one product, multi products will overwrite attrs
            if transaction_settings.get(account.name):
                self.trade_cost = transaction_settings[account.name]

        # setattr from config
        for key, value in self.config_attr_dict(config).items():
            setattr(self, key, value)

        # setattr from hyperparameters
        # order counts! tuning parameters overwrite config and settings.json attrs
        for key, value in kwargs.items():
            setattr(self, key, value)

    def back_test(self):
        for cur_time in self.back_test_init:
            print(cur_time)
            self.current_time = cur_time

            open_market_adjust(self)
            eval(f'strategy.{self.strategy}.before_market')(self)
            eval(f'strategy.{self.strategy}.market_open')(self)

            with shelve.open('./platform_sys/settings/order',
                             'r') as order_shelve:
                # order_shelve[dict]: key: str(int); value: Order[cls]
                self.order_hub(order_shelve, self)
            #下午三点以后执行操作
            close_market_adjust(self)
            eval(f'strategy.{self.strategy}.market_close')(self)

    @property
    def back_test_init(self):
        table_name = account_init(self)
        self.cash_account = cash_account(table_name)
        self.stock_account = stock_account(table_name)
        self.order_hub = order_hub()
        start_date = pd.to_datetime(self.start_date)
        end_date = pd.to_datetime(self.end_date)
        return trading_date_df.query(
            '@start_date <= Date <=@end_date').loc[:, 'Date'].tolist()

    @staticmethod
    def config_attr_dict(config_cls) -> dict:
        '''sort out built-in attr, i.e., __xxx__'''
        cls_dict = config_cls.__dict__
        return {
            key: cls_dict[key]
            for key in cls_dict.keys()
            if not (key.startswith('__') and key.endswith('__'))
        }
