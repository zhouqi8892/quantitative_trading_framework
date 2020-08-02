import pandas as pd
from ..action.order import position_clear, position_adjust, Order


def before_market(context):
    pass


def market_open(context):
    # if context.current_date == pd.to_datetime('2010-3-4'):
    #     order(['000001', '000002'], [200, 100], context)
    # elif context.current_date == pd.to_datetime('2010-3-5'):
    #     order(['000001', '000004'], [100, 200], context)
    # elif context.current_date == pd.to_datetime('2010-3-8'):
    #     order_target(['000001', '000002', '000005'], [100, 300, 500], context)
    # else:
    #     position_adjust(context, percentage=0.9, method='equal weight')
    #     # pass
    Order(['000001','000003'],amount=[100,200])

    # print(context.stock_account.detail())
    # print(context.current_date)
def market_close(context):
    pass
