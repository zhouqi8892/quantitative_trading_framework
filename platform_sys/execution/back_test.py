from ..account.cash import cash_account
from ..account.stock import stock_account
from ..account import account_init
from ..account.settings import close_market_adjust, open_market_adjust
from ..action.order import order_hub
from ..data.data_process import historical_data_df, date_referrer_df
from .. import strategy
import pandas as pd

import shelve
import json
with open('./platform_sys/settings/transaction_settings.json', 'r') as f:
    transaction_settings = json.load(f)


class Hub:
    def __init__(self, config: 'class', **kwargs: 'hyperparameters'):

        self.historical_data_df = historical_data_df
        self.date_referrer_df = date_referrer_df

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
        self.back_test_init()
        for cur_time in self.back_test_loopper:
            self.current_time = cur_time

            open_market_adjust(self)
            eval(f'strategy.{self.strategy}.before_market')(self)
            eval(f'strategy.{self.strategy}.market_open')(self)

            with shelve.open('./platform_sys/settings/order',
                             'r') as order_shelve:
                # order_shelve[dict]: key: str(int); value: Order[cls]
                self.order_hub(order_shelve, self)

            return
            #下午三点以后执行操作
            close_market_adjust(self)
            eval('strategy.%s.market_close' % config.strategy)(self)

    def back_test_init(self):
        start_date = pd.to_datetime(self.start_date)
        end_date = pd.to_datetime(self.end_date)
        self.back_test_loopper = self.date_referrer_df.query(
            '@start_date <= Date <=@end_date').loc[:,'Date'].tolist()
        table_name = account_init(self)
        self.cash_account = cash_account(table_name)
        self.stock_account = stock_account(table_name)
        self.order_hub = order_hub()


    @staticmethod
    def config_attr_dict(config_cls) -> dict:
        '''sort out built-in attr, i.e., __xxx__'''
        cls_dict = config_cls.__dict__
        return {
            key: cls_dict[key]
            for key in cls_dict.keys()
            if not (key.startswith('__') and key.endswith('__'))
        }


# def back_test_init(config):
#     class context:
#         trade_cost = config.trade_cost
#         table_name = account_init(config)
#         # 根据config.account_list初始化对应账户，以strategy_name_startdate_enddate_current_time命名，并返回该命名
#         # historical_data_df = raw_data_df_generation(
#         #     r'platform_sys\data\eodprices.h5'
#         # )
#         historical_data_df = pd.read_csv(r'platform_sys\data\raw_data.csv',
#                                          index_col=0,
#                                          header=0,
#                                          parse_dates=[2])

#     date_referrer_df = pd.read_excel(r'platform_sys\data\date_referrer.xlsx',
#                                      index_col=None,
#                                      header=3,
#                                      usecols=[0],
#                                      parse_dates=[0])

#     #frequency = config.frequency
#     start_date = pd.to_datetime(config.start_date)
#     end_date = pd.to_datetime(config.end_date)
#     date_list = date_referrer_df.query(
#         '@start_date <= Date <=@end_date').values.flatten()
#     i = 0
#     for date in date_list:
#         setattr(context, 'cash_account', cash_account(context.table_name))
#         # 创建现金账户实例，每期重新连接数据库以更新数据，后期若每天更新耗算力，考虑将session放到setting给所有更新内容引用
#         setattr(context, 'stock_account', stock_account(context.table_name))
#         # 创建股票账户实例，每天更新至context
#         setattr(context, 'current_date', date)
#         open_market_adjust(context)
#         eval('strategy.%s.preperation' % config.strategy)(context)
#         eval('strategy.%s.market_open' % config.strategy)(context)
#         #下午三点以后执行操作
#         close_market_adjust(context)
#         eval('strategy.%s.market_close' % config.strategy)(context)
#         i += 1
#         if i == 4:
#             break
