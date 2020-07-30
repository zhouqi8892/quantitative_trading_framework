from ..account.cash import cash_account
from ..account.stock import stock_account
from ..account import account_init
from ..account.settings import close_market_adjust, open_market_adjust
from .. import strategy
from . import raw_data_df_generation
import pandas as pd


def back_test_init(config):
    class context:
        trade_cost = config.trade_cost
        table_name = account_init(config)
        # 根据config.account_list初始化对应账户，以strategy_name_startdate_enddate_current_time命名，并返回该命名
        # historical_data_df = raw_data_df_generation(
        #     r'platform_sys\data\eodprices.h5'
        # )
        historical_data_df = pd.read_csv(r'platform_sys\data\raw_data.csv',
                                         index_col=0,
                                         header=0,
                                         parse_dates=[2])

    date_referrer_df = pd.read_excel(r'platform_sys\data\date_referrer.xlsx',
                                     index_col=None,
                                     header=3,
                                     usecols=[0],
                                     parse_dates=[0])

    #frequency = config.frequency
    start_date = pd.to_datetime(config.start_date)
    end_date = pd.to_datetime(config.end_date)
    date_list = date_referrer_df.query(
        '@start_date <= Date <=@end_date').values.flatten()
    i = 0
    for date in date_list:
        setattr(context, 'cash_account', cash_account(context.table_name))
        # 创建现金账户实例，每期重新连接数据库以更新数据，后期若每天更新耗算力，考虑将session放到setting给所有更新内容引用
        setattr(context, 'stock_account', stock_account(context.table_name))
        # 创建股票账户实例，每天更新至context
        setattr(context, 'current_date', date)
        open_market_adjust(context)
        eval('strategy.%s.preperation' % config.strategy)(context)
        eval('strategy.%s.market_open' % config.strategy)(context)
        #下午三点以后执行操作
        close_market_adjust(context)
        eval('strategy.%s.market_close' % config.strategy)(context)
        i += 1
        if i == 4:
            break
