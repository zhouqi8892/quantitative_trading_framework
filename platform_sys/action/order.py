from platform_sys.action.settings import trade_process, order_cost
from platform_sys.data.data_prepare import market_data_df
from platform_sys.account.settings import account_linkage
from platform_sys.positions.weight import weight_fun_dict
import numpy as np
import pandas as pd
import shelve
import inspect
import json

with open('./platform_sys/settings/transaction_settings.json', 'r') as f:
    transaction_settings = json.load(f)

# flag='n': initialize new blank shelve to store order
with shelve.open(r'.\platform_sys\settings\order', flag='n') as order_shelve:
    order_shelve['0'] = 0


class Order:
    '''integrated order command'''

    ORDER_TYPE = [
        'amount', 'target_amount', 'value', 'target_value', 'position_target'
    ]
    __slots__ = [
        'code', 'order_type', 'quote_price', 'order_establish_time',
        'TYPE_chosen', 'method', 'product'
    ] + ORDER_TYPE

    def __init__(self, code, product='stock', **kwargs):

        # test for whether indicate multiple ORDER_TYPE
        ORDER_TYPE_test_boolean = np.isin(list(kwargs.keys()), self.ORDER_TYPE)
        if ORDER_TYPE_test_boolean.sum() != 1:
            raise Exception(
                '[Order] indicate one of amount/target_amount/value/target_value parameter'
            )
        else:
            self.TYPE_chosen = np.array(list(
                kwargs.keys()))[ORDER_TYPE_test_boolean][0]

        # confirm limit/market order_type
        if 'quote_price' in kwargs.keys():
            self.order_type = 'lmt'
        else:
            self.order_type = 'mkt'

        len_test_list = []
        # kwargs should only contain one of ORDER_TYPE and price if order_type='lmt'
        # if input irrelevant paras, will raise error for __slots__ constrain
        for k, v in kwargs.items():
            # convert single order to list
            v = np.array(
                [v]) if not isinstance(v, (list, np.ndarray)) else np.array(v)
            len_test_list += [len(v)]
            setattr(self, k, v)

        self.code = np.array(
            [code]) if not isinstance(code,
                                      (list, np.ndarray)) else np.array(code)
        self.product = product

        # test for length of each input list
        if len(set(len_test_list + [len(self.code)])) != 1:
            raise Exception('[Order] length of input list should be same')

        with shelve.open(r'.\platform_sys\settings\order',
                         flag='c') as order_shelve:
            # append orders to shelve
            key = str(int(list(order_shelve.keys())[-1]) + 1)
            order_shelve[key] = self
            order_shelve['0'] += 1

    def __str__(self):
        return 'code[para] must be indicated\n\
            choose to indicate one of amount/target_amount/value/target_value\n\
            if order_type="lmt", must define price, otherwise, "mkt" by default'

    def __call__(self, current_time):
        self.order_establish_time = current_time
        return self

    def order_validity(self):
        # 停牌无法买卖逻辑
        # return (valid Order, invalid Order)
        order_establish_time = self.order_establish_time
        tmp_list = []
        for code in self.code:
            if market_data_df.query(
                    'code == @code & time ==@order_establish_time').size != 0:
                tmp_list += [True]
            else:
                tmp_list += [False]
        else:
            boolean_array = np.array(tmp_list)
        # !!!涨停牌逻辑未加入!!!
        return self.order_split(boolean_array)

    def order_split(self, boolean_array):
        '''split into two Order object with valid and invalid orders'''
        # two new object
        # 尝试精简化用于valid/invalid和valid/dealt/cancel剥离
        valid_order = valid_Order(self, boolean_array)
        invalid_order = invalid_Order(self, ~boolean_array)
        return valid_order, invalid_order

    def instance_attr(self):
        '''return self defined attributes'''
        instance_attr = inspect.getmembers(
            self, lambda x: not (inspect.isroutine(x)))
        return [
            attr for attr in instance_attr
            if not (attr[0].startswith('__') and attr[0].endswith('__'))
        ]


class invalid_Order(Order):
    def __init__(self, parent_self, boolean_array):
        parent_instance_attr = parent_self.instance_attr()
        for k, v in parent_instance_attr:
            setattr(
                self, k, v[boolean_array] if isinstance(v, np.ndarray)
                and len(boolean_array) == len(v) else v)


class valid_Order(Order):
    def __init__(self, parent_self, boolean_array):
        parent_instance_attr = parent_self.instance_attr()
        for k, v in parent_instance_attr:
            setattr(
                self, k, v[boolean_array] if isinstance(v, np.ndarray)
                and len(boolean_array) == len(v) else v)

    def order_match(self,
                    context) -> '(valid_order, dealt_order, canceled_order)':

        #expiry test
        if context.current_time.date() > self.order_establish_time.date():
            valid_order, dealt_order, canceled_order = None, None, self.canceled_order(
                context.current_time)
        # 待考虑涨跌停
        elif self.order_type == 'mkt':
            # assume all stocks in this order will deal, no need to split
            # 未用split 故没有boolean，可以统一用boolean(all True) split的形式
            valid_order, dealt_order, canceled_order = None, dealt_Order(
                self, context.current_time), None
            # 对dealt_order进行进一步筛选，若资金充足则全通过，
            # 不足则考虑1.减少amount至足额购买，2.减少购买的股票。重新更新.
            # 减少部分进入canceled_order,考虑给canceled_order加入merge method用于合并
            # 资金充足性检验
            # test = dealt_order.capital_availability(context)
            # if not isinstance(test,bool):
            #     dealt_order, new_canceled_order = test
            #     canceled_order.merge(new_canceled_order)

        elif self.order_type == 'lmt':
            # 目前未考虑分批成交，若lmt必然会分批成交，届时需将订单拆分同order_plit操作
            # some of stocks in the order will deal, some will not --> need split
            # 分批成交，如果len(dealt_order.code)=0，须返回None,以不执行METHOD_chosen操作
            pass
        return valid_order, dealt_order, canceled_order

    def canceled_order(self, cur_time):
        '''add information for dealt order. e.g., canceled time'''
        setattr(self, 'order_canceled_time', cur_time)

        def merge(self):
            '''merge new_canceled_order due to unavailable cap'''
            pass


class dealt_Order(valid_Order):
    def __init__(self, parent_self, order_dealt_time):
        parent_instance_attr = parent_self.instance_attr()

        boolean_array = []  # 备用，后续加入split后改

        for k, v in parent_instance_attr:
            setattr(
                self, k, v[boolean_array] if isinstance(v, np.ndarray)
                and len(boolean_array) == len(v) else v)

        self.order_dealt_time = order_dealt_time
        self.transaction_price = self.transaction_price_cal()
        self.transaction_cost = self.trade_cost()
        ORDER_METHOD = dict(amount=self.order,
                            target_amount=self.order_target,
                            value=self.order_value,
                            target_value=self.order_target_value)
        self.METHOD_chosen = ORDER_METHOD[self.TYPE_chosen]

    def transaction_price_cal(self, *args):

        if args:
            code_list, = args
        else:
            code_list = self.code

        # !!!should consider slippage!!!
        order_dealt_time = self.order_dealt_time
        if self.order_type == 'mkt':
            tmp_price_list = []
            for code in code_list:
                tmp_price_list += [
                    market_data_df.query(
                        'code == @code and time == @order_dealt_time')
                    ['close'].iloc[0]
                ]
            return np.array(tmp_price_list)

    def trade_cost(self, *args):
        '''券商手续费双向收取，印花税卖出时收取，后期甄别'''
        if args:
            amount, transaction_price = args
        else:
            amount, transaction_price = self.amount, self.transaction_price

        trade_cost_dict = transaction_settings[self.product]
        market_value_list = np.abs(amount) * transaction_price
        brokerage_fee = np.maximum(
            trade_cost_dict[order_cost.brokerage_fee.name][0] *
            market_value_list,
            trade_cost_dict[order_cost.brokerage_fee.name][1])
        market_value_list[amount > 0] = 0
        market_value_list[amount < 0] = market_value_list[
            amount < 0] * trade_cost_dict[order_cost.stamp_tax.name]
        stamp_tax = market_value_list
        transaction_cost = (brokerage_fee + stamp_tax).round(2)
        return transaction_cost

    def capital_availability(self, context):
        # net value considers positive and negative amount
        net_market_value_list = self.amount * self.transaction_price
        transaction_cost_list = self.transaction_cost

        total_cost = (net_market_value_list + transaction_cost_list).sum()
        if context.cash_account.session.query(context.cash_account.table).get(
                'RMB').available_cash >= total_cost:
            return True
        else:
            # 待补充 1.减少amount至足额购买，2.减少购买的股票，
            # 返回new dealt obejct, new canceled order
            return False

    def trade_process(self, amount_list, context, *args):
        # 考虑添加 transaction_amount 实际成交数量至self
        if args:
            price_list, code_list, extra_fee_list = args
        else:
            price_list, code_list, extra_fee_list = self.transaction_price, self.code, self.transaction_cost

        buy_boolean_list = amount_list > 0
        # sell first
        for code, amount, price, extra_fee in zip(
                code_list[~buy_boolean_list], amount_list[~buy_boolean_list],
                price_list[~buy_boolean_list],
                extra_fee_list[~buy_boolean_list]):
            self.buy_sell(amount, code, price, extra_fee, context)

        for code, amount, price, extra_fee in zip(
                code_list[buy_boolean_list], amount_list[buy_boolean_list],
                price_list[buy_boolean_list],
                extra_fee_list[buy_boolean_list]):
            self.buy_sell(amount, code, price, extra_fee, context)

    @staticmethod
    def buy_sell(amount, code, price, extra_fee, context):
        linkage = account_linkage(code, amount, price, extra_fee)
        if amount > 0:
            # request buy，inout_cash unchanged, avai_cash down, trans_cash down, locked_cash up
            # request...
            linkage.before_buy_request(context)
            if True:  # 查询返回成交成功
                linkage.cash_account_linkage(context)
                linkage.stock_account_linkage(context)
                print('buy %s %s' % (amount, code))
            else:
                print('买入失败，须回滚')
        elif amount < 0:
            # sell
            if not linkage.before_sell_request(context):
                return
            if True:
                linkage.stock_account_linkage(context)
                linkage.cash_account_linkage(context)
                print('sell %s %s' % (-amount, code))
            else:
                print('卖出失败，须回滚')

    def order(self, context):
        amount_list = (np.floor(self.amount / 100) * 100).astype(int)
        self.trade_process(amount_list, context)

    def order_target(self, context):
        code_list, target_amount_list = self.code, self.target_amount
        table_stock = context.stock_account.table
        session_stock = context.stock_account.session
        amount_list = np.array([
            max(
                target_amount_list[i] - session_stock.query(table_stock).get(
                    code_list[i]).total_amount,
                -session_stock.query(table_stock).get(
                    code_list[i]).tradable_amount)
            if session_stock.query(table_stock).get(code_list[i]) else
            target_amount_list[i] for i in range(len(code_list))
        ])
        amount_list = (np.floor(amount_list / 100) * 100).astype(int)
        self.trade_process(amount_list, context)

    @staticmethod
    def order_value(code, value, context):
        pass

    @staticmethod
    def order_target_value(code, target_value, context):
        pass

    def position_clear(self, context):
        table_stock = context.stock_account.table
        session_stock = context.stock_account.session
        results = session_stock.query(table_stock).all()
        code = [result.code for result in results]
        amount_list = [-result.tradable_amount for result in results]
        # 上述查询放在Order init内进行
        self.trade_process(amount_list, context)

    @staticmethod
    def position_target(context,
                        code='unchanged',
                        position_target=1,
                        method='equal weight'):
        '''默认组合不变，只调整仓位，percentage=1则满仓(现金+stock_value),method: equal weight/equal value'''
        pass

    def position_target(self, portfolio_list, percentage, method, context):
        '''调仓函数'''
        table_stock = context.stock_account.table
        session_stock = context.stock_account.session
        results = session_stock.query(table_stock).all()
        total_amount_list = [result.total_amount for result in results]
        stock_in_account = np.array([result.code for result in results])
        stock_union_set = set(stock_in_account) | set(portfolio_list)
        stock_amount_df = pd.DataFrame(0,
                                       index=stock_union_set,
                                       columns=['account_amount'])
        stock_amount_df.account_amount.update(
            pd.Series(total_amount_list, index=stock_in_account))

        # stocks in account but not in target_list are to be sold out
        clear_stock_list = stock_in_account[
            ~np.isin(stock_in_account, portfolio_list)]
        stock_amount_df['target_amount'] = pd.Series(0, index=clear_stock_list)

        # if target_list and account_list not overlap completely, some are exposed to sell
        # sell them first to update total_avai_value in cash account
        if clear_stock_list.size != 0:
            stock_amount_df.query('target_amount == 0')
            price_list = self.transaction_price_cal(*(clear_stock_list, ))
            amount_list = np.zeros(clear_stock_list.size).astype(int)
            extra_fee_list = self.trade_cost(*(amount_list, price_list))
            self.trade_process(amount_list, context,
                               *(price_list, clear_stock_list, extra_fee_list))

        # some stocks are to be bounght or positions are to be adjusted if in account already
        if len(portfolio_list) != 0:
            # 直接赋值，早先卖出的非组合账户内股票部分成nan，后续只针对target_list部分
            stock_amount_df.target_amount = pd.Series(weight_fun_dict[method](
                portfolio_list, percentage, context),
                                                      index=portfolio_list)

            # 判断组合权重是否构建成功，不成功返回[np.nan...]
            if not all(stock_amount_df.target_amount.isnull()):
                stock_amount_df[
                    'diff'] = stock_amount_df.target_amount - stock_amount_df.account_amount
                # 下单按非整份数，实际交易份数又内部函数规则确定
                stock_amount_df['diff'] = stock_amount_df['diff'].apply(
                    lambda x: 0 if np.abs(x) < 100 else x)

                price_list = self.transaction_price_cal(*(portfolio_list, ))
                amount_list = stock_amount_df.loc[portfolio_list,
                                                  'diff'].to_numpy()
                extra_fee_list = self.trade_cost(*(amount_list, price_list))
                self.trade_process(
                    amount_list, context,
                    *(price_list, portfolio_list, extra_fee_list))


class canceled_Order(valid_Order):
    pass