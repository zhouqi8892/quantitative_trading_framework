from enum import Enum
import numpy as np
from ..account.settings import stock_account_linkage, cash_account_linkage, before_buy_request, before_sell_request


class order_cost(Enum):
    brokerage_fee = 0
    stamp_tax = 1


def get_available_list(code, amount, historical_df, current_date):
    '''用于判断待买卖股票是否在市场上有数据（可交易），
    return code、amount、price去除不可买后的list'''
    assert isinstance(
        code, list) or type(code) is str, 'code shoule be str or list with str'
    assert (isinstance(amount, list) and isinstance(amount[0], np.int32)
            ) or type(amount) is int, 'amount should be int or list with int'
    code_list = np.array(code if type(code) is list else [code])
    amount_list = np.array(amount if type(amount) is list else [amount])
    code_available_list = [
        i if historical_df[(historical_df.date == current_date)
                           & (historical_df.code == i)].shape[0] == 1 else None
        for i in code_list
    ]
    df_result = historical_df[(historical_df.date == current_date)
                              & (historical_df.code.isin(code_list))]
    code_available_list = df_result.code.values
    price_available_list = df_result.price.values
    amount_available_list = amount_list[np.isin(code_list,
                                                code_available_list)]
    return code_available_list, amount_available_list, price_available_list


def test_available_capital(amount_list, price_list, context):
    market_value_list = amount_list * price_list
    extra_fee_list = trade_cost_cal(amount_list, price_list,
                                    context.trade_cost)
    total_cost = np.sum(market_value_list + extra_fee_list)
    if context.cash_account.session.query(context.cash_account.table).get(
            'RMB').available_cash >= total_cost:
        return True
    else:
        return False


def trade_cost_cal(amount_list, price_list, trade_cost):
    '''券商手续费双向收取，印花税卖出时收取，后期甄别'''
    market_value_list = np.abs(amount_list) * price_list
    brokerage_fee = np.maximum(
        trade_cost[order_cost.brokerage_fee.name][0] * market_value_list,
        trade_cost[order_cost.brokerage_fee.name][1])
    market_value_list[amount_list > 0] = 0
    market_value_list[amount_list < 0] = market_value_list[
        amount_list < 0] * trade_cost[order_cost.stamp_tax.name]
    stamp_tax = market_value_list
    return np.round(brokerage_fee + stamp_tax, 2)


def trade_process(amount_list, price_list, code_list, extra_fee_list,
                  buy_boolean_list, sell_boolean_list, session_cash,
                  session_stock, table_cash, table_stock, context):
    if len(amount_list[buy_boolean_list]) > 0 and len(
            amount_list[sell_boolean_list]) == 0:
        #只有买单请求
        test_result = test_available_capital(amount_list[buy_boolean_list],
                                             price_list[buy_boolean_list],
                                             context)
        if test_result:
            for code, amount, price, extra_fee in zip(
                    code_list[buy_boolean_list], amount_list[buy_boolean_list],
                    price_list[buy_boolean_list],
                    extra_fee_list[buy_boolean_list]):
                buy_sell(amount, session_cash, session_stock, table_cash,
                         table_stock, code, price, extra_fee, context)
        else:
            print('资金不够无法购买')
    elif len(amount_list[buy_boolean_list]) == 0 and len(
            amount_list[sell_boolean_list]) > 0:
        #只有卖单请求
        for code, amount, price, extra_fee in zip(
                code_list[sell_boolean_list], amount_list[sell_boolean_list],
                price_list[sell_boolean_list],
                extra_fee_list[sell_boolean_list]):
            buy_sell(amount, session_cash, session_stock, table_cash,
                     table_stock, code, price, extra_fee, context)
    elif len(amount_list[buy_boolean_list]) > 0 and len(
            amount_list[sell_boolean_list]) > 0:
        # 既有买单又有卖单请求
        test_result = test_available_capital(amount_list[buy_boolean_list],
                                             price_list[buy_boolean_list],
                                             context)
        if test_result:  # 检验资金量是否支持直接买
            for code, amount, price, extra_fee in zip(
                    code_list[buy_boolean_list], amount_list[buy_boolean_list],
                    price_list[buy_boolean_list],
                    extra_fee_list[buy_boolean_list]):
                buy_sell(amount, session_cash, session_stock, table_cash,
                         table_stock, code, price, extra_fee, context)
            for code, amount, price, extra_fee in zip(
                    code_list[sell_boolean_list],
                    amount_list[sell_boolean_list],
                    price_list[sell_boolean_list],
                    extra_fee_list[sell_boolean_list]):
                buy_sell(amount, session_cash, session_stock, table_cash,
                         table_stock, code, price, extra_fee, context)
        else:  # 不支持则先卖后检验能否买
            print('资金不足，尝试先卖后买')
            for code, amount, price, extra_fee in zip(
                    code_list[sell_boolean_list],
                    amount_list[sell_boolean_list],
                    price_list[sell_boolean_list],
                    extra_fee_list[sell_boolean_list]):
                buy_sell(amount, session_cash, session_stock, table_cash,
                         table_stock, code, price, extra_fee, context)
            test_result = test_available_capital(amount_list[buy_boolean_list],
                                                 price_list[buy_boolean_list],
                                                 context)
            if test_result:
                for code, amount, price, extra_fee in zip(
                        code_list[buy_boolean_list],
                        amount_list[buy_boolean_list],
                        price_list[buy_boolean_list],
                        extra_fee_list[buy_boolean_list]):
                    buy_sell(amount, session_cash, session_stock, table_cash,
                             table_stock, code, price, extra_fee, context)
            else:
                print('先卖后买也失败，资金不足')


def buy_sell(amount, session_cash, session_stock, table_cash, table_stock,
             code, price, extra_fee, context):
    if amount > 0:
        # request buy，inout_cash unchanged, avai_cash down, trans_cash down, locked_cash up
        # request...
        before_buy_request(session_cash, table_cash, extra_fee, amount, price)
        if True:  # 查询返回成交成功
            cash_account_linkage(session_cash, table_cash, price, amount,
                                 extra_fee)
            stock_account_linkage(session_stock, table_stock, code, amount,
                                  price, extra_fee, context)
            print('buy %s %s' % (amount, code))
        else:
            print('买入失败，须回滚')
    elif amount < 0:
        # sell
        if not before_sell_request(session_stock, table_stock, code, amount):
            return
        if True:
            stock_account_linkage(session_stock, table_stock, code, amount,
                                  price, extra_fee, context)
            cash_account_linkage(session_cash, table_cash, price, amount,
                                 extra_fee)
            print('sell %s %s' % (-amount, code))
        else:
            print('卖出失败，须回滚')
