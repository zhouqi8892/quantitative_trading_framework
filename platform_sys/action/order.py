from .settings import get_available_list, trade_process, trade_cost_cal
from ..data.data_process import historical_data_df
from functools import partial
import numpy as np
import shelve

# flag='n': initialize new blank shelve to store order
with shelve.open(r'.\platform_sys\settings\order', flag='n') as order_shelve:
    order_shelve['0'] = 0


class Order:
    '''code[para] must be indicated
    choose to indicate one of amount/target_amount/value/target_value
    if order_type="lmt", must define price, otherwise, "mkt" by default
    '''
    ORDER_TYPE = ['amount', 'target_amount', 'value', 'target_value']

    __slots__ = [
        'code', 'order_type', 'price', 'type_chosen', 'kwargs_keys',
        'order_dealt_time', 'order_establish_time', 'METHOD_chosen',
        'TYPE_chosen'
    ] + ORDER_TYPE

    def __init__(self, code, order_type='mkt', **kwargs):

        # test for whether indicate multiple ORDER_TYPE
        ORDER_TYPE_test_boolean = np.isin(list(kwargs.keys()), self.ORDER_TYPE)
        if ORDER_TYPE_test_boolean.sum() != 1:
            raise Exception(
                '[Order] indicate one of amount/target_amount/value/target_value parameter'
            )
        else:
            self.TYPE_chosen = np.array(list(
                kwargs.keys()))[ORDER_TYPE_test_boolean][0]

        # validate combination of order_type and price
        if (order_type == 'mkt' and 'price' in kwargs.keys()) or (
                order_type == 'lmt' and 'price' not in kwargs.keys()):
            raise Exception(
                '[Order] indicate order_type="lmt" with price, else order_type="mkt"(by default) without price'
            )

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
        self.order_type = order_type
        self.kwargs_keys = list(kwargs.keys())

        # test for length of each input list
        if len(set(len_test_list + [len(self.code)])) != 1:
            raise Exception('[Order] length of input list should be same')

        with shelve.open(r'.\platform_sys\settings\order',
                         flag='c') as order_shelve:
            # append orders to shelve
            key = str(max([int(k) for k in order_shelve.keys()]) + 1)
            order_shelve[key] = self
            order_shelve['0'] += 1

    def __call__(self, current_time):
        self.order_establish_time = current_time
        return self

    def order_validity(self):
        # 考虑涨跌停、停牌无法买卖逻辑
        # return (valid Order, invalid Order)
        order_establish_time = self.order_establish_time
        tmp_list = []
        for code in self.code:
            if historical_data_df.query(
                    'code == @code & date ==@order_establish_time').size != 0:
                tmp_list += [True]
            else:
                tmp_list += [False]
        else:
            boolean_array = np.array(tmp_list)
        # !!!涨停牌逻辑未加入!!!
        return self.order_split(boolean_array)

    def order_split(self, boolean_array):
        '''split into two Order object with available and unavailable orders'''
        valid_order = valid_Order(
            self.code[boolean_array], self.order_type,
            **{k: getattr(self, k)[boolean_array]
               for k in self.kwargs_keys})(self.order_establish_time)
        invalid_order = self.__class__(
            self.code[~boolean_array], self.order_type,
            **{k: getattr(self, k)[~boolean_array]
               for k in self.kwargs_keys})(self.order_establish_time)
        return valid_order, invalid_order

    def order_match(self, match_time
                    ) -> '(valid_order, dealt_order, canceled_order)':

        #expiry test
        if match_time.date() > self.order_establish_time.date():
            valid_order, dealt_order, canceled_order = None, None, self.canceled_order(
                match_time)

        elif self.order_type == 'mkt':
            # assume all stocks in this order will deal, no need to split
            valid_order, dealt_order, canceled_order = None, self.dealt_order(
                match_time), None

        elif self.order_type == 'lmt':
            # 目前未考虑分批成交，若lmt必然会分批成交，届时需将订单拆分同order_plit操作
            # some of stocks in the order will deal, some will not --> need split
            pass
        return valid_order, dealt_order, canceled_order

    def dealt_order(self, cur_time):
        '''add information for dealt order. e.g., dealt price, commission fee, dealt time'''
        self.order_dealt_time = cur_time
        # !!!should consider slippage!!!
        if self.order_type == 'mkt':
            for code in self.code:
                self.price = historical_data_df.query(
                    'code == @code and date == @cur_time')['price'].iloc[0]
        return self

    def canceled_order(self, cur_time):
        '''add information for dealt order. e.g., canceled time'''
        setattr(self, 'order_canceled_time', cur_time)


class valid_Order(Order):
    def __init__(self, code, order_type, **kwargs):
        super().__init__(code=code, order_type=order_type, **kwargs)
        ORDER_METHOD = dict(amount=self.order,
                            target_amount=self.order_target,
                            value=self.order_value,
                            target_value=self.order_target_value)
        self.METHOD_chosen = partial(
            ORDER_METHOD[self.TYPE_chosen],
            **{self.TYPE_chosen: getattr(self, self.TYPE_chosen)})

    def order(self, amount, context):
        code_list, amount_list, price_list = get_available_list(
            list(self.code), list(amount), historical_data_df,
            self.order_dealt_time)
        if len(code_list) == 0:
            print('选中股票市场上无数据，无法请求交易')
            return
        amount_list = (np.floor(amount_list / 100) * 100).astype(int)
        extra_fee_list = trade_cost_cal(amount_list, price_list,
                                        context.trade_cost)
        # 判断是否可交易，返回有效股票代码（市场有数据）
        table_stock, session_stock = context.stock_account.table, context.stock_account.session
        table_cash, session_cash = context.cash_account.table, context.cash_account.session
        buy_boolean_list = amount_list > 0
        sell_boolean_list = amount_list < 0
        trade_process(amount_list, price_list, code_list, extra_fee_list,
                      buy_boolean_list, sell_boolean_list, session_cash,
                      session_stock, table_cash, table_stock, context)

    @staticmethod
    def order_target(code, target_amount, context):
        current_date = context.current_date
        historical_df = context.historical_data_df.copy()
        code_list, target_amount_list, price_list = get_available_list(
            code, target_amount, historical_df, current_date)
        if len(code_list) == 0:
            print('选中股票市场上无数据，无法购买')
            return
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
        extra_fee_list = trade_cost_cal(amount_list, price_list,
                                        context.trade_cost)
        table_cash = context.cash_account.table
        session_cash = context.cash_account.session
        buy_boolean_list = amount_list > 0
        sell_boolean_list = amount_list < 0
        trade_process(amount_list, price_list, code_list, extra_fee_list,
                      buy_boolean_list, sell_boolean_list, session_cash,
                      session_stock, table_cash, table_stock, context)

    @staticmethod
    def order_value(code, value, context):
        pass

    @staticmethod
    def order_target_value(code, target_value, context):
        pass


class order_hub():
    def __init__(self):

        self.valid_order = {}
        self.invalid_order = {}

        self.canceled_order = {}
        self.dealt_order = {}
        self.latest_order_idx = 0

    def __call__(self, order_shelve, context):
        '''check if new order comes and do match_process for valid order'''

        # at each loop time, check if new order comes
        # if come with new, classify new comer into valid/invalid order(validate_process)
        if int(order_shelve['0']) > self.latest_order_idx:

            # e.g., latest_order_idx=0, order_shelve['0']=2--> diff=2(last two are new comer [-2:])
            diff = int(order_shelve['0']) - self.latest_order_idx

            # record current_time as order_establish_time
            new_orders = {
                k: v(context.current_time)
                for k, v in list(order_shelve.items())[-diff:]
            }

            self.validate_process(new_orders)

            # update latest_order_idx for next judgement
            self.latest_order_idx = int(order_shelve['0'])

        # match_process conducts at each loop time
        self.match_process(context)

    def validate_process(self, new_orders):
        '''classify new orders to valid_order/invalid_order'''
        for k, v in new_orders.items():
            valid_orders, invalid_orders = v.order_validity()
            if len(valid_orders.code) != 0:
                self.valid_order.update({k: valid_orders})
            if len(invalid_orders.code) != 0:
                self.invalid_order.update({k: invalid_orders})

    def match_process(self, context):
        '''match valid_order at each loop time,
        once matched, move to dealt_order and synchronize relevant account
        once expiry, move to canceled_order'''

        # 'copy' original items to avoid error caused by dict change(update valid_order)
        valid_order_items = list(self.valid_order.items())

        for k, v in valid_order_items:
            valid_order, dealt_order, canceled_order = v.order_match(
                context.current_time)

            # update dict if valid_order is not None, else delete
            if valid_order:
                self.valid_order.update({k: valid_order})
            else:
                del self.valid_order[k]

            if dealt_order:
                if self.dealt_order.get(k):
                    # already exist, cannot overwrite directly
                    # this case happens when the order was partly dealt previously
                    self.dealt_order.update(
                        {k: [self.dealt_order.get(k)] + [dealt_order]})
                else:
                    self.dealt_order.update({k: dealt_order})

                # synchronize relevant account
                dealt_order.METHOD_chosen(context=context)

            if canceled_order:
                if self.canceled_order.get(k):
                    # already exist, cannot overwrite directly
                    # this case happens when the order was partly canceled previously
                    self.canceled_order.update(
                        {k: [self.canceled_order.get(k)] + [canceled_order]})
                else:
                    self.canceled_order.update({k: canceled_order})


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
