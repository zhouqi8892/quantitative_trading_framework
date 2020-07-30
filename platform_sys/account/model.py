from sqlalchemy import Column, String, Integer, DateTime, DECIMAL
from sqlalchemy.dialects.mysql import FLOAT
'''添加数据库行内容时，续同步添加对应stock/cash.py的类方法，
及对应settings.py的联动变更计算公式'''


def cash_account_fun(Base, table_name):
    class cash_account(Base):
        # 表的名字:
        __tablename__ = table_name

        # 表的结构:

        currency = Column(String(16), primary_key=True)
        inout_cash = Column(DECIMAL(precision=15, scale=2))
        available_cash = Column(DECIMAL(precision=15, scale=2))
        transferable_cash = Column(DECIMAL(precision=15, scale=2))
        locked_cash = Column(DECIMAL(precision=15, scale=2))
        today_inout = Column(DECIMAL(precision=15, scale=2))
        starting_cash = Column(DECIMAL(precision=15, scale=2))

        def __repr__(self):
            return '<cash_account:inout_cash:%s available_cash:%s transferable_cash:%s locked_cash:%s today_inout:%s>' % (
                self.inout_cash, self.available_cash, self.transferable_cash,
                self.locked_cash, self.today_inout)

    return cash_account


def stock_account_fun(Base, table_name):
    class stock_account(Base):
        # 表的名字:
        __tablename__ = table_name

        # 表的结构:

        code = Column(String(16), primary_key=True)
        current_price = Column(DECIMAL(precision=15, scale=2))
        market_value = Column(DECIMAL(precision=15, scale=2))
        acc_avg_cost = Column(DECIMAL(precision=15, scale=2))
        avg_cost = Column(DECIMAL(precision=15, scale=2))
        init_time = Column(DateTime)
        transaction_time = Column(DateTime)
        locked_amount = Column(Integer)
        total_amount = Column(Integer)
        tradable_amount = Column(Integer)
        today_buy_amount = Column(Integer)
        side = Column(String(16))

        def __repr__(self):
            return '<stock_account:%s %s %s %s>' % (
                self.code, self.current_price, self.market_value,
                self.acc_avg_cost)

    return stock_account
