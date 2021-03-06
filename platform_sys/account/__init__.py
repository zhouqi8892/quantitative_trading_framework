from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from .model import cash_account_fun, stock_account_fun
from datetime import datetime
from .settings import account
import json

with open('./platform_sys/settings/DB_url_settings.json', 'r') as f:
    db_url_dict = json.load(f)


def account_init(self):
    '''create account sql for backtest'''
    table_name = '{strategy_name}_{backtest_start_date}-{backtest_end_date}_{test_date}'.format(
        strategy_name=self.strategy,
        backtest_start_date=self.start_date,
        backtest_end_date=self.end_date,
        test_date=datetime.now().strftime('%Y/%m/%d_%H:%M'))

    def stock_account_init():
        Base = declarative_base()
        # 定义model对象:
        stock_account = stock_account_fun(Base, table_name)

        # 初始化数据库连接:
        engine = create_engine(db_url_dict[account.stock.name])

        # 创建DBSession类型:
        DBSession = sessionmaker(bind=engine)
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)
        session = DBSession()
        session.close()

    def cash_account_init():
        Base = declarative_base()
        # 定义model对象:
        cash_account = cash_account_fun(Base, table_name)

        # 初始化数据库连接:
        engine = create_engine(db_url_dict[account.cash.name])

        # 创建DBSession类型:
        DBSession = sessionmaker(bind=engine)
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)
        session = DBSession()
        sub_account = cash_account(currency='RMB',
                                   inout_cash=self.initial_capital,
                                   available_cash=self.initial_capital,
                                   transferable_cash=self.initial_capital,
                                   locked_cash=0,
                                   today_inout=0,
                                   starting_cash=self.initial_capital)
        session.add_all([sub_account])
        session.commit()
        session.close()

    init_fun_dict = {
        account.cash: cash_account_init(),
        account.stock: stock_account_init()
    }

    # initialize account by stated account_list
    for i in self.account_list:
        init_fun_dict[i]

    return table_name
