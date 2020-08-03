from enum import Enum
import numpy as np
from ..account.settings import stock_account_linkage, cash_account_linkage, before_buy_request, before_sell_request


class order_cost(Enum):
    brokerage_fee = 0
    stamp_tax = 1


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
                                 extra_fee, context)
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
                                 extra_fee, context)
            print('sell %s %s' % (-amount, code))
        else:
            print('卖出失败，须回滚')
