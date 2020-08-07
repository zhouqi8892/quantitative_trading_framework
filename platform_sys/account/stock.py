from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from platform_sys.account.model import stock_account_fun
from platform_sys.account.settings import account
import pandas as pd
import numpy as np
import json
with open('./platform_sys/settings/DB_url_settings.json', 'r') as f:
    db_url_dict = json.load(f)


class stock_account:
    '''实例化实现多账户管理,
    区分多头账户、空头账户、汇总账户'''
    def __init__(self, table_name):
        self.__table_name = table_name
        self.session, self.table = self.__orm_init()

    def __orm_init(self):
        Base = declarative_base()
        table = stock_account_fun(Base, self.__table_name)
        engine = create_engine(db_url_dict[account.stock.name])
        DBSession = sessionmaker(bind=engine)
        session = DBSession()
        return session, table

    def detail(self):
        '''写为method非attr的原因：实例化赋值attr时，为首次session，而后session变化attr不变。
        而method保证了引用最新的session。
        全账户信息dataframe呈现'''
        engine = create_engine(db_url_dict[account.stock.name])
        return pd.read_sql(self.__table_name, engine)

    def in_acount(self, code):
        return self.session.query(self.table).get(code)

    def code(self, code=None):
        if code is None:
            return np.array(
                [i.code for i in self.session.query(self.table).all()])
        else:
            return self.session.query(self.table).get(code).code

    def current_price(self, code=None):
        if code is None:
            return np.array([
                i.current_price for i in self.session.query(self.table).all()
            ], np.float)
        else:
            return np.array(
                self.session.query(self.table).get(code).current_price,
                np.float)

    def market_value(self, code=None):
        if code is None:
            return np.array(
                [i.market_value for i in self.session.query(self.table).all()],
                np.float)
        else:
            return np.array(
                self.session.query(self.table).get(code).market_value,
                np.float)

    def acc_avg_cost(self, code=None):
        if code is None:
            return np.array(
                [i.acc_avg_cost for i in self.session.query(self.table).all()],
                np.float)
        else:
            return np.array(
                self.session.query(self.table).get(code).acc_avg_cost,
                np.float)

    def init_time(self, code=None):
        if code is None:
            return np.array(
                [i.init_time for i in self.session.query(self.table).all()])
        else:
            return self.session.query(self.table).get(code).init_time

    def avg_cost(self, code=None):
        if code is None:
            return np.array(
                [i.avg_cost for i in self.session.query(self.table).all()],
                np.float)
        else:
            return np.array(
                self.session.query(self.table).get(code).avg_cost, np.float)

    def transaction_time(self, code=None):
        if code is None:
            return np.array([
                i.transaction_time
                for i in self.session.query(self.table).all()
            ])
        else:
            return self.session.query(self.table).get(code).transaction_time

    def locked_amount(self, code=None):
        if code is None:
            return np.array([
                i.locked_amount for i in self.session.query(self.table).all()
            ], np.int)
        else:
            return np.array(
                self.session.query(self.table).get(code).locked_amount, np.int)

    def total_amount(self, code=None):
        if code is None:
            return np.array(
                [i.total_amount for i in self.session.query(self.table).all()],
                np.int)
        else:
            return np.array(
                self.session.query(self.table).get(code).total_amount, np.int)

    def tradable_amount(self, code=None):
        if code is None:
            return np.array([
                i.tradable_amount
                for i in self.session.query(self.table).all()
            ], np.int)
        else:
            return np.array(
                self.session.query(self.table).get(code).tradable_amount,
                np.int)

    def today_amount(self, code=None):
        if code is None:
            return np.array(
                [i.today_amount for i in self.session.query(self.table).all()],
                np.int)
        else:
            return np.array(
                self.session.query(self.table).get(code).today_amount, np.int)

    def side(self, code=None):
        if code is None:
            return np.array(
                [i.side for i in self.session.query(self.table).all()])
        else:
            return self.session.query(self.table).get(code).side
