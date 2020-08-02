from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from .model import cash_account_fun
from sqlalchemy import create_engine
import pandas as pd
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

    def currency(self):
        return [i.currency for i in self.session.query(self.table).all()]

    def inout_cash(self):
        return [i.inout_cash for i in self.session.query(self.table).all()]

    def available_cash(self):
        return [i.available_cash for i in self.session.query(self.table).all()]

    def transferable_cash(self):
        return [
            i.transferable_cash for i in self.session.query(self.table).all()
        ]

    def locked_cash(self):
        return [i.locked_cash for i in self.session.query(self.table).all()]

    def margin(self):
        return [i.margin for i in self.session.query(self.table).all()]

    def starting_cash(self):
        return [i.starting_cash for i in self.session.query(self.table).all()]
