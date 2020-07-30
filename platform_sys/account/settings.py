from enum import Enum
from decimal import Decimal


class account(Enum):
    cash = 0
    stock = 1
    future = 2
    option = 3


db_url_dict = {
    account.cash: 'mysql://root:88923392@localhost:3306/cash_account',
    account.stock: 'mysql://root:88923392@localhost:3306/stock_account'
}


def stock_account_linkage(session, table, code, amount, price, extra_fee,
                          context):
    result = session.query(table).get(code)
    if result:  # 加仓/减仓
        result.current_price = price
        total_cost = Decimal(price * amount + extra_fee)
        amount = amount.astype(Decimal)
        result.acc_avg_cost = (
            (result.acc_avg_cost * result.total_amount + total_cost) /
            (result.total_amount +
             amount)) if result.total_amount + amount != 0 else 0
        if amount > 0:
            result.avg_cost = (result.avg_cost * result.total_amount +
                               total_cost) / (result.total_amount + amount)
            result.today_buy_amount += amount
        result.transaction_time = context.current_date
        if amount < 0:
            #挂单时locked_amount增加，现在减回去
            result.locked_amount += amount
        result.total_amount += amount
        result.market_value = price * result.total_amount
        result.transaction_time = context.current_date
        # session.commit()
        # session.close()
    else:  # 建仓
        stock_add = table(code=code,
                          current_price=price,
                          market_value=price * amount,
                          acc_avg_cost=(price * amount + extra_fee) / amount,
                          avg_cost=(price * amount + extra_fee) / amount,
                          init_time=context.current_date,
                          transaction_time=context.current_date,
                          locked_amount=0,
                          total_amount=amount,
                          tradable_amount=0,
                          today_buy_amount=amount,
                          side='long')
        session.add(stock_add)
        # session.commit()
        # session.close()


def cash_account_linkage(session, table, price, amount, extra_fee):
    result = session.query(table).get('RMB')
    total_cost = Decimal(amount * price + extra_fee)
    result.today_inout += -total_cost
    result.inout_cash += -total_cost
    if amount > 0:
        result.locked_cash += -total_cost
    if amount < 0:
        result.available_cash += -total_cost
    # session.commit()
    # session.close()


def before_buy_request(session_cash, table_cash, extra_fee, amount, price):
    result = session_cash.query(table_cash).get('RMB')
    total_cost = Decimal(amount * price + extra_fee)
    result.available_cash += -total_cost
    result.transferable_cash += -total_cost
    result.locked_cash += total_cost
    # session_cash.commit()


def before_sell_request(session_stock, table_stock, code, amount):
    result = session_stock.query(table_stock).get(code)
    if result:
        if abs(amount) > result.tradable_amount:
            # 请求卖出的数量上限
            amount = -result.tradable_amount
        result.tradable_amount += amount
        result.locked_amount += -amount
        # session_stock.commit()
        return True
    else:
        print(' %s not in account' % code)
        return False


def close_market_adjust(context):
    # 恢复账户如locked（取消挂单），tradable amount，today_amount,清空total_amount为0的股票
    session_cash = context.cash_account.session
    table_cash = context.cash_account.table
    result_cash = session_cash.query(table_cash).get('RMB')
    if result_cash.locked_cash > 0:
        result_cash.available_cash += result_cash.locked_cash
        result_cash.transferable_cash += result_cash.locked_cash
        result_cash.locked_cash += -result_cash.locked_cash
    if result_cash.today_inout != 0:
        # today_inout 正数部分转为transferable_cash，负数部分仅清零
        if result_cash.today_inout > 0:
            result_cash.transferable_cash += result_cash.today_inout
        result_cash.today_inout += -result_cash.today_inout
    session_cash.commit()

    session_stock = context.stock_account.session
    table_stock = context.stock_account.table
    results_untrade_stock = session_stock.query(table_stock).filter(
        table_stock.locked_amount != 0).all()
    for result_stock in results_untrade_stock:
        # 挂单未卖成功的份额从locked_amount转回tradble_amount
        result_stock.tradable_amount += result_stock.locked_amount
        result_stock.locked_amount += -result_stock.locked_amount
    session_stock.query(table_stock).filter(
        table_stock.total_amount == 0).delete()  # 删除清仓的股票

    session_stock.commit()


def open_market_adjust(context):
    historical_df = context.historical_data_df.copy()
    historical_df['code'] = historical_df['code'].apply(lambda x: x[:6])
    session_stock = context.stock_account.session
    table_stock = context.stock_account.table
    results = session_stock.query(table_stock).all()
    code_list = [result.code for result in results]
    df_result = historical_df[(historical_df.date == context.current_date)
                              & (historical_df.code.isin(code_list))]
    code_list = df_result.code.values
    price_list = df_result.price.values
    for code, price in zip(code_list, price_list):
        result = session_stock.query(table_stock).get(code)
        result.current_price = price
        result.market_value = price * result.total_amount
    results_today_stock = session_stock.query(table_stock).filter(
        table_stock.today_buy_amount != 0).all()
    for result_stock in results_today_stock:
        if result_stock.today_buy_amount > 0:
            # 新买入份额不计入tradable_amount, 当日结束后该份额移入tradable
            result_stock.tradable_amount += result_stock.today_buy_amount
        result_stock.today_buy_amount += -result_stock.today_buy_amount
