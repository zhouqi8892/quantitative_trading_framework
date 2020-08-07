from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from platform_sys.account.model import cash_account_fun
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import json

with open('./platform_sys/settings/DB_url_settings.json') as f:
    db_url_dict = json.load(f)


class cash_account:
    '''实例化实现多账户管理
    回测环境下实例化现金账户，在数据库中创建新的模拟账户'''
    def __init__(self, table_name):
        self.__table_name = table_name
        self.session, self.table = self.orm_init()

    def orm_init(self):
        Base = declarative_base()
        table = cash_account_fun(Base, self.__table_name)
        engine = create_engine(db_url_dict['cash'])
        DBSession = sessionmaker(bind=engine)
        session = DBSession()
        return session, table

    def detail(self):
        '''写为method非attr的原因：实例化赋值attr时，为首次session，而后session变化attr不变。
        而method保证了引用最新的session。
        全账户信息dataframe呈现'''
        engine = create_engine(db_url_dict['cash'])
        return pd.read_sql(self.__table_name, engine)

    @property
    def currency(self):
        return np.array(
            [i.currency for i in self.session.query(self.table).all()],
            np.float)[0]

    @property
    def inout_cash(self):
        return np.array(
            [i.inout_cash for i in self.session.query(self.table).all()],
            np.float)[0]

    @property
    def available_cash(self):
        return np.array(
            [i.available_cash for i in self.session.query(self.table).all()],
            np.float)[0]

    @property
    def transferable_cash(self):
        return np.array([
            i.transferable_cash for i in self.session.query(self.table).all()
        ], np.float)[0]

    @property
    def locked_cash(self):
        return np.array(
            [i.locked_cash for i in self.session.query(self.table).all()],
            np.float)[0]

    @property
    def margin(self):
        return np.array(
            [i.margin for i in self.session.query(self.table).all()],
            np.float)[0]

    @property
    def starting_cash(self):
        return np.array(
            [i.starting_cash for i in self.session.query(self.table).all()],
            np.float)[0]
