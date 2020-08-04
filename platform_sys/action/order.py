from platform_sys.action.settings import trade_process, order_cost
from platform_sys.data.data_prepare import market_data_df
from platform_sys.account.settings import account_linkage
import numpy as np
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

    def transaction_price_cal(self):
        # !!!should consider slippage!!!
        order_dealt_time = self.order_dealt_time
        if self.order_type == 'mkt':
            tmp_price_list = []
            for code in self.code:
                tmp_price_list += [
                    market_data_df.query(
                        'code == @code and time == @order_dealt_time')
                    ['close'].iloc[0]
                ]
            return np.array(tmp_price_list)

    def trade_cost(self):
        '''券商手续费双向收取，印花税卖出时收取，后期甄别'''
        trade_cost_dict = transaction_settings[self.product]
        market_value_list = np.abs(self.amount) * self.transaction_price
        brokerage_fee = np.maximum(
            trade_cost_dict[order_cost.brokerage_fee.name][0] *
            market_value_list,
            trade_cost_dict[order_cost.brokerage_fee.name][1])
        market_value_list[self.amount > 0] = 0
        market_value_list[self.amount < 0] = market_value_list[
            self.amount < 0] * trade_cost_dict[order_cost.stamp_tax.name]
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

    def trade_process(self, amount_list, context):

        buy_boolean_list = amount_list > 0
        price_list, code_list, extra_fee_list = self.transaction_price, self.code, self.transaction_cost
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
        self.trade_process(amount_list,context)

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
        extra_fee_list = self.trade_cost(amount_list, context.trade_cost)
        buy_boolean_list = amount_list > 0
        sell_boolean_list = amount_list < 0
        trade_process(amount_list, self.transaction_price, self.code,
                      extra_fee_list, buy_boolean_list, sell_boolean_list,
                      context)

    @staticmethod
    def order_value(code, value, context):
        pass

    @staticmethod
    def order_target_value(code, target_value, context):
        pass

    @staticmethod
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
            buy_sell(amount, session_cash, session_stock, table_cash,
                     table_stock, code, price, extra_fee, context)
        print('能卖的全卖了，清仓')

    @staticmethod
    def position_target(context,
                        code='unchanged',
                        position_target=1,
                        method='equal weight'):
        '''默认组合不变，只调整仓位，percentage=1则满仓(现金+stock_value),method: equal weight/equal value'''
        # 考虑amount在实例Order内就生成
        table_stock = context.stock_account.table
        session_stock = context.stock_account.session
        table_cash = context.cash_account.table
        session_cash = context.cash_account.session
        historical_df = context.historical_data_df.copy()
        historical_df['code'] = historical_df['code'].apply(lambda x: x[:6])

        results = session_stock.query(table_stock).all()
        code_in_account = [result.code for result in results]
        df_result_account = historical_df[
            (historical_df.time == context.current_date)
            & (historical_df.code.isin(code_in_account))]
        # 获取账户内可交易股票信息
        code_in_account_available_list = df_result_account.code.values
        price_in_account_available_list = df_result_account.price.values
        if code == 'unchanged':
            portfolio_available_list = code_in_account_available_list
            price_portfolio_available_list = price_in_account_available_list
        else:
            historical_df['code'] = historical_df['code'].apply(
                lambda x: x[:6])
            df_result_portfolio = historical_df[
                (historical_df.time == context.current_date)
                & (historical_df.code.isin(code))]
            # 获取换仓组合中可交易股票信息
            portfolio_available_list = df_result_portfolio.code.values
            price_portfolio_available_list = df_result_portfolio.price.values

        code_sell_list = code_in_account_available_list[np.isin(
            code_in_account_available_list,
            portfolio_available_list,
            invert=True)]
        # 账户中不在换仓组合中的股票必卖出
        sell_price_list = price_in_account_available_list[np.isin(
            code_in_account_available_list,
            portfolio_available_list,
            invert=True)]
        sell_target_amount_list = [0] * len(code_sell_list)

        avai_cash_value = session_cash.query(table_cash).get(
            'RMB').available_cash
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
            total_value * position_target /
            (weight_list * price_portfolio_available_list).sum())
        if portfolio_share == 0:
            print('构建组合不足1份，换仓失败')
            return
        portfolio_amount_list = portfolio_share * weight_list
        code_list = np.append(portfolio_available_list, code_sell_list)
        price_list = np.append(price_in_account_available_list,
                               sell_price_list)
        target_amount_list = np.append(portfolio_amount_list,
                                       sell_target_amount_list)
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

        buy_boolean_list = amount_list > 0
        sell_boolean_list = amount_list < 0
        trade_process(amount_list, price_list, code_list, extra_fee_list,
                      buy_boolean_list, sell_boolean_list, session_cash,
                      session_stock, table_cash, table_stock, context)


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
            valid_order, dealt_order, canceled_order = v.order_match(context)

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
                dealt_order.METHOD_chosen(context)

            if canceled_order:
                if self.canceled_order.get(k):
                    # already exist, cannot overwrite directly
                    # this case happens when the order was partly canceled previously
                    self.canceled_order.update(
                        {k: [self.canceled_order.get(k)] + [canceled_order]})
                else:
                    self.canceled_order.update({k: canceled_order})
