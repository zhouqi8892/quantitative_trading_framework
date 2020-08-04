from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from .model import stock_account_fun
from .settings import account
import pandas as pd
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

    def code(self):
        return [i.code for i in self.session.query(self.table).all()]

    def current_price(self):
        return [i.current_price for i in self.session.query(self.table).all()]

    def market_value(self):
        return [i.market_value for i in self.session.query(self.table).all()]

    def acc_avg_cost(self):
        return [i.acc_avg_cost for i in self.session.query(self.table).all()]

    def init_time(self):
        return [i.init_time for i in self.session.query(self.table).all()]

    def avg_cost(self):
        return [i.avg_cost for i in self.session.query(self.table).all()]

    def transaction_time(self):
        return [
            i.transaction_time for i in self.session.query(self.table).all()
        ]

    def locked_amount(self):
        return [i.locked_amount for i in self.session.query(self.table).all()]

    def total_amount(self):
        return [i.total_amount for i in self.session.query(self.table).all()]

    def tradable_amount(self):
        return [
            i.tradable_amount for i in self.session.query(self.table).all()
        ]

    def today_amount(self):
        return [i.today_amount for i in self.session.query(self.table).all()]

    def side(self):
        return [i.side for i in self.session.query(self.table).all()]
