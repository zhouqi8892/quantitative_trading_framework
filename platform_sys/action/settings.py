from enum import Enum
import numpy as np
from platform_sys.account.settings import stock_account_linkage, cash_account_linkage, before_buy_request, before_sell_request


class order_cost(Enum):
    brokerage_fee = 0
    stamp_tax = 1


# 待position_target适配后可删除
def test_available_capital(amount_list, price_list, extra_fee_list, context):
    market_value_list = amount_list * price_list
    total_cost = np.sum(market_value_list + extra_fee_list)
    if context.cash_account.session.query(context.cash_account.table).get(
            'RMB').available_cash >= total_cost:
        return True
    else:
        return False


# 待position_target适配后可删除
def trade_process(amount_list, price_list, code_list, extra_fee_list, context):

    buy_boolean_list = amount_list > 0

    # sell first
    for code, amount, price, extra_fee in zip(
            code_list[~buy_boolean_list], amount_list[~buy_boolean_list],
            price_list[~buy_boolean_list], extra_fee_list[~buy_boolean_list]):
        buy_sell(amount, code, price, extra_fee, context)

    for code, amount, price, extra_fee in zip(
            code_list[buy_boolean_list], amount_list[buy_boolean_list],
            price_list[buy_boolean_list], extra_fee_list[buy_boolean_list]):
        buy_sell(amount, code, price, extra_fee, context)


# 待position_target适配后可删除
def buy_sell(amount, code, price, extra_fee, context):
    if amount > 0:
        # request buy，inout_cash unchanged, avai_cash down, trans_cash down, locked_cash up
        # request...
        before_buy_request(extra_fee, amount, price, context)
        if True:  # 查询返回成交成功
            cash_account_linkage(price, amount, extra_fee, context)
            stock_account_linkage(code, amount, price, extra_fee, context)
            print('buy %s %s' % (amount, code))
        else:
            print('买入失败，须回滚')
    elif amount < 0:
        # sell
        if not before_sell_request(code, amount, context):
            return
        if True:
            stock_account_linkage(code, amount, price, extra_fee, context)
            cash_account_linkage(price, amount, extra_fee, context)
            print('sell %s %s' % (-amount, code))
        else:
            print('卖出失败，须回滚')
