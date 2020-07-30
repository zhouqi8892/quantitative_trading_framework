from .settings import get_available_list, trade_process, trade_cost_cal
import numpy as np


def order(code, amount, context):
    current_date = context.current_date
    historical_df = context.historical_data_df.copy()
    historical_df['code'] = historical_df['code'].apply(lambda x: x[:6])
    code_list, amount_list, price_list = get_available_list(
        code, amount, historical_df, current_date)
    if len(code_list) == 0:
        print('选中股票市场上无数据，无法请求交易')
        return
    amount_list = (np.floor(amount_list / 100) * 100).astype(int)
    extra_fee_list = trade_cost_cal(amount_list, price_list,
                                    context.trade_cost)
    # 判断是否可交易，返回有效股票代码（市场有数据）
    table_stock = context.stock_account.table
    session_stock = context.stock_account.session
    table_cash = context.cash_account.table
    session_cash = context.cash_account.session
    buy_boolean_list = amount_list > 0
    sell_boolean_list = amount_list < 0
    trade_process(amount_list, price_list, code_list, extra_fee_list,
                  buy_boolean_list, sell_boolean_list, session_cash,
                  session_stock, table_cash, table_stock, context)


def order_target(code, target_amount, context):
    current_date = context.current_date
    historical_df = context.historical_data_df.copy()
    historical_df['code'] = historical_df['code'].apply(lambda x: x[:6])
    code_list, target_amount_list, price_list = get_available_list(
        code, target_amount, historical_df, current_date)
    if len(code_list) == 0:
        print('选中股票市场上无数据，无法购买')
        return
    table_stock = context.stock_account.table
    session_stock = context.stock_account.session
    amount_list = np.array([
        max(
            target_amount_list[i] -
            session_stock.query(table_stock).get(code_list[i]).total_amount,
            -session_stock.query(table_stock).get(code_list[i]).tradable_amount
        ) if session_stock.query(table_stock).get(code_list[i]) else
        target_amount_list[i] for i in range(len(code_list))
    ])
    amount_list = (np.floor(amount_list / 100) * 100).astype(int)
    extra_fee_list = trade_cost_cal(amount_list, price_list,
                                    context.trade_cost)
    table_cash = context.cash_account.table
    session_cash = context.cash_account.session
    buy_boolean_list = amount_list > 0
    sell_boolean_list = amount_list < 0
    trade_process(amount_list, price_list, code_list, extra_fee_list,
                  buy_boolean_list, sell_boolean_list, session_cash,
                  session_stock, table_cash, table_stock, context)


def position_clear(context):
    table_stock = context.stock_account.table
    session_stock = context.stock_account.session
    results = session_stock.query(table_stock).all()
    code = [result.code for result in results]
    amount = [-result.tradable_amount for result in results]
    current_date = context.current_date
    historical_df = context.historical_data_df.copy()
    historical_df['code'] = historical_df['code'].apply(lambda x: x[:6])
    code_list, amount_list, price_list = get_available_list(
        code, amount, historical_df, current_date)
    if len(code_list) == 0:
        print('选中股票市场上无数据，无法请求交易')
        return
    extra_fee_list = trade_cost_cal(amount_list, price_list,
                                    context.trade_cost)
    table_cash = context.cash_account.table
    session_cash = context.cash_account.session
    for code, amount, price, extra_fee in zip(code_list, amount_list,
                                              price_list, extra_fee_list):
        buy_sell(amount, session_cash, session_stock, table_cash, table_stock,
                 code, price, extra_fee, context)
    print('能卖的全卖了，清仓')


def order_value(code, value, context):
    pass


def order_target_value(code, target_value, context):
    pass


def position_adjust(context,
                    portfolio='unchanged',
                    percentage=1,
                    method='equal weight'):
    '''默认组合不变，只调整仓位，percentage=1则满仓(现金+stock_value),method: equal weight/equal value'''
    table_stock = context.stock_account.table
    session_stock = context.stock_account.session
    table_cash = context.cash_account.table
    session_cash = context.cash_account.session
    historical_df = context.historical_data_df.copy()
    historical_df['code'] = historical_df['code'].apply(lambda x: x[:6])

    results = session_stock.query(table_stock).all()
    code_in_account = [result.code for result in results]
    df_result_account = historical_df[
        (historical_df.date == context.current_date)
        & (historical_df.code.isin(code_in_account))]
    # 获取账户内可交易股票信息
    code_in_account_available_list = df_result_account.code.values
    price_in_account_available_list = df_result_account.price.values
    if portfolio == 'unchanged':
        portfolio_available_list = code_in_account_available_list
        price_portfolio_available_list = price_in_account_available_list
    else:
        historical_df['code'] = historical_df['code'].apply(lambda x: x[:6])
        df_result_portfolio = historical_df[
            (historical_df.date == context.current_date)
            & (historical_df.code.isin(portfolio))]
        # 获取换仓组合中可交易股票信息
        portfolio_available_list = df_result_portfolio.code.values
        price_portfolio_available_list = df_result_portfolio.price.values

    code_sell_list = code_in_account_available_list[np.isin(
        code_in_account_available_list, portfolio_available_list, invert=True)]
    # 账户中不在换仓组合中的股票必卖出
    sell_price_list = price_in_account_available_list[np.isin(
        code_in_account_available_list, portfolio_available_list, invert=True)]
    sell_target_amount_list = [0] * len(code_sell_list)

    avai_cash_value = session_cash.query(table_cash).get('RMB').available_cash
    avai_stock_value = sum([
        session_stock.query(table_stock).get(code).current_price *
        session_stock.query(table_stock).get(code).tradable_amount
        for code in code_in_account_available_list
    ])
    total_value = float(avai_cash_value + avai_stock_value)
    # 计算有效总仓位价值

    if method == 'equal weight':
        weight_list = np.ones(len(portfolio_available_list)) * 100
    elif method == 'value weight':
        pass
    elif method == 'price weight':
        weight_list = 100 * price_portfolio_available_list

    portfolio_share = np.floor(
        total_value * percentage /
        (weight_list * price_portfolio_available_list).sum())
    if portfolio_share == 0:
        print('构建组合不足1份，换仓失败')
        return
    portfolio_amount_list = portfolio_share * weight_list
    code_list = np.append(portfolio_available_list, code_sell_list)
    price_list = np.append(price_in_account_available_list, sell_price_list)
    target_amount_list = np.append(portfolio_amount_list,
                                   sell_target_amount_list)
    amount_list = np.array([
        max(
            target_amount_list[i] -
            session_stock.query(table_stock).get(code_list[i]).total_amount,
            -session_stock.query(table_stock).get(code_list[i]).tradable_amount
        ) if session_stock.query(table_stock).get(code_list[i]) else
        target_amount_list[i] for i in range(len(code_list))
    ])
    amount_list = (np.floor(amount_list / 100) * 100).astype(int)
    extra_fee_list = trade_cost_cal(amount_list, price_list,
                                    context.trade_cost)

    buy_boolean_list = amount_list > 0
    sell_boolean_list = amount_list < 0
    trade_process(amount_list, price_list, code_list, extra_fee_list,
                  buy_boolean_list, sell_boolean_list, session_cash,
                  session_stock, table_cash, table_stock, context)
