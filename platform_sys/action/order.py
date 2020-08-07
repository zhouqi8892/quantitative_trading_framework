from platform_sys.action.settings import order_cost
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
    '''Order class is used to make order for certain products.
    ------------------------------------------------------------
    Currently, it supports CN stock, but will support more products in the future\n
    Usage guidance:
    ------------------------------------------------------------
    required argument: code[ default:[ ] ]: list/str[available for one stock only]\n
    optional argument(positional): product[default:'stock'], ps. currently support stock only\n
    optional argument(keyword only):\n
    1.amount/target_amount/value/target_value/position_target:\n
    ---------!choose to define one of them to indicate order method(test1)!-----------

    1.1. amount/target_amount/value/target_value: list of number/number[one stock only]\n
    classic sets for orderring\n
    1.2. position_target: number between [0,1]\n
    defined within (0,1] to switch position to defined code list with defined position percentage and under market price\n
    defined as (0,1] together with code=[] to adjust current position level\n
    defined as 0 together with code=[](by defualt,test2) to clear current position\n
    i.e., sell stocks in account but not in target code list, buy stocks not in account but in target code list, and adjust opsition for stocks in both account and target code list.\n

    2.quote_price: list of number/number[one stock only]\n
    if one of classic order sets(1.1) is defined, quote price can be set in order to make limit price order\n
    if quote price is defined, order_type[para] will be automatically defined as 'lmt', otherwise as 'mkt'\n
    list length should be the same as code list's(test3)\n

    3.weight_type: str in ['value_weight','equal_value_weight','euqual_amount_weight','equal_risk_weight']\n
    parameter is designed to cooperate with position_target[1.2] to determine amount/value distribution among stocks in target portfolio list\n
    Examples
    --------------------------------------------------------
    1.1.1.Order(code=['code1','code2'], amount=[amount1,amount2], optional:[ price=[price1,price2] ]):\n
    market[limit] order for code1 with amount1 [and price1]\n
    mkt[lmt] order for code2 with amount2 [and price2]\n
    1.1.2.Order(code=['code1','code2'], target_amount=[target_amount1,target_amount2], optional:[ price=[price1,price2] ]):\n
    market[mkt] order for code1 to target_amount1 in account [with price1]\n
    limit[lmt] order for code2 to target_amount2 in account [with price2]\n
    1.1.3.Order(code=['code1','code2'],value=[value1,value2], optional:[ price=[price1,price2] ]):\n
    market[limit] order for code1 with value1 [and price1]\n
    mkt[lmt] order for code2 with value2 [and price2]\n
    1.1.4.Order(code=['code1','code2'], target_value=[target_value1,target_value2], optional:[ price=[price1,price2] ]):\n
    market[mkt] order for code1 to target_value1 in account [with price1]\n
    limit[lmt] order for code2 to target_value2 in account [with price2]\n
    1.2.1.Order(code=['code1','code2'], position_target=0.5[ number within (0,1] ], weight_type='value_weight'):\n
    switch current position to code1 and code2 under target total value: 0.5 * total available(tradable) assets value(cash+stocks)\n
    1.2.2.Order(code=[](empty list by default, can omit), position_target=0)\n
    clear current account position\n
    1.2.3.Order(code=[](empty list by default, can omit), position_target=0.5[ number within (0,1] ]\n
    adjust current account position to 50% of total asset value
    '''

    classic_order_methods = [
        'amount', 'target_amount', 'value', 'target_value'
    ]

    portfolio_adjust_methods = ['position_target']

    ORDER_METHODS = classic_order_methods + portfolio_adjust_methods

    __slots__ = ORDER_METHODS + [
        'code', 'target_stock_list', 'product', 'order_type', 'quote_price',
        'order_establish_time', 'METHOD_chosen', 'weight_type'
    ]

    def __init__(self, code=[], product='stock', **kwargs):

        assert isinstance(code, list), '[type ilegal] code: list'

        # assign product type
        self.product = product

        # confirm limit/market order_type
        if 'quote_price' in kwargs.keys():
            self.order_type = 'lmt'
        else:
            self.order_type = 'mkt'

        # test1 for whether define multiple parameters in ORDER_METHODS
        ORDER_METHODS_test_boolean = np.isin(list(kwargs.keys()),
                                             self.ORDER_METHODS)
        if ORDER_METHODS_test_boolean.sum() != 1:
            raise Exception(
                'ambiguous inputs[**kwargs] define ORDER_METHODS more than one'
            )
        else:
            # assign chosen order method
            self.METHOD_chosen = np.array(list(
                kwargs.keys()))[ORDER_METHODS_test_boolean][0]

            len_test_list = []
            # kwargs should only contain one of ORDER_METHODS and price if order_type='lmt'
            # if input irrelevant paras, will raise error for __slots__ constrain
            for k, v in kwargs.items():
                # convert classic orderring method's value into np.ndarray(list)
                if k in self.classic_order_methods + ['quote_price']:
                    # if the number is extremely large, dtype will be 'O', denoting python object(int/float)
                    assert isinstance(v, list), f'[type ilegal] {k}: list'
                    v = np.array(v)
                    assert np.issubdtype(
                        v.dtype, np.number
                    ), 'np.array([amount/price/value]).dtype: np.number'
                    len_test_list += [len(v)]

                elif k in self.portfolio_adjust_methods:

                    assert np.issubdtype(
                        np.array(v).dtype, np.number
                    ) and 0 <= v <= 1, 'position_target: number within [0,1]'

                    assert 'quote_price' not in kwargs.keys(
                    ), 'ambiguous input[quote_price] when calling position_target'

                    if v == 0:
                        # test2 for code remaining default value(empty list) when clear position
                        assert code == [], 'position_target=0 denotes clear current position, code must be an empty list[by default]'
                        assert 'weight_type' not in kwargs.keys(
                        ), 'ambiguous input[weight_type] when attempting to clear current position'
                    else:
                        assert kwargs.get(
                            'weight_type', None
                        ) in weight_fun_dict.keys(
                        ), "weight_type not defined or not in ['value_weight','equal_value_weight','euqual_amount_weight','equal_risk_weight']"
                    self.target_stock_list = np.array(code)

                setattr(self, k, v)

            # no test for legality of code
            # if some of codes are illegal, they will be classified to invalid order
            # due to no such code type in mkt data(query('code==')->empty dataframe)
            if self.METHOD_chosen in self.classic_order_methods:
                self.code = np.array(code)

                # test3 for length accordance of list inputs
                assert len(set(len_test_list + [len(self.code)])
                           ) == 1, 'all list inputs must be with length'
                assert 'weight_type' not in kwargs.keys(
                ), 'ambiguous input[weight_type] when calling classic order method'

        with shelve.open(r'.\platform_sys\settings\order',
                         flag='c') as order_shelve:
            # append orders to shelve
            key = str(int(list(order_shelve.keys())[-1]) + 1)
            order_shelve[key] = self
            order_shelve['0'] += 1

    def __call__(self, context):
        self.order_establish_time = context.current_time
        if self.METHOD_chosen in self.portfolio_adjust_methods:
            self.code = self.position_METHOD_code(context)
        return self

    def position_METHOD_code(self, context):
        '''when position_target applied, stocks in account are to be involved
        assign self.code with a union set between target porfolio list and stock list in account'''

        # special case for adjust current position level
        if self.target_stock_list.size == 0 and self.position_target != 0:
            # if unchanged, meaning that adjust position level of stocks in account only
            self.target_stock_list = context.stock_account.code()
        return np.unique(
            np.append(context.stock_account.code(), self.target_stock_list))

    def order_validity(self, context) -> '(valid_Order, invalid_Order)':

        # 停牌无法买卖逻辑用有无市场数据替代
        # 假设无市场数据是因为无交易而非数据本身不全
        order_establish_time = self.order_establish_time
        bool_list = []
        for code in self.code:
            if market_data_df.query(
                    'code == @code & time ==@order_establish_time').size != 0:
                bool_list += [True]
            else:
                bool_list += [False]
        else:
            boolean_array = np.array(bool_list)
        # 待加入资金验证逻辑capital_availability(context)，对通过停牌检验的list同步添加quote price for mkt order,及trade cost
        # 待加入账户股票可卖检验
        # 待加入空仓命令clear逻辑无效
        # 考虑让position_METHOD跳过以上检验，在valid_order内完成确定amount过程
        return self.order_split(boolean_array, context)

    def capital_availability(self, context):
        # net value considers positive and negative amount
        net_market_value_list = self.amount * self.transaction_price
        transaction_cost_list = self.transaction_cost
        # 同步加入cost attr， position_METHOD cost 在valid call内加

        total_cost = (net_market_value_list + transaction_cost_list).sum()
        if context.cash_account.session.query(context.cash_account.table).get(
                'RMB').available_cash >= total_cost:
            return True
        else:
            # 待补充 1.减少amount至足额购买，2.减少购买的股票，
            # 返回new dealt obejct, new canceled order
            return False

    def order_split(self, boolean_array, context):
        '''split into two Order object with valid and invalid orders'''
        # two new object
        # 尝试精简化用于valid/invalid和valid/dealt/cancel剥离
        valid_order = valid_Order(self, boolean_array)(context)
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
        # !!!未考虑target_list的boolean


class valid_Order(Order):
    def __init__(self, parent_self, boolean_array):
        parent_instance_attr = parent_self.instance_attr()
        for k, v in parent_instance_attr:
            setattr(
                self, k, v[boolean_array] if isinstance(v, np.ndarray)
                and len(boolean_array) == len(v) else v)

    def __call__(self, context):
        if self.order_type == 'mkt':
            self.quote_price = self.set_price(self.order_establish_time)
            # 待加入self.attr trade cost

        if self.METHOD_chosen in self.portfolio_adjust_methods:
            self.position_METHOD_init(context)
        # 待加入同步资金账户逻辑(context)
        return self

    def set_price(self, time, slippage=False):
        '''set quote price for market order'''
        # !!!should consider slippage!!!
        # 滑点不能在这里考虑，position_target的amount未知，
        # 故待放在最后成交的account linkage内考虑
        # slippage_rate = transaction_settings[
        #     self.product]['slippage'] if slippage else 0

        # def price_cal(price, amount):
        #     if amount > 0:
        #         return price * (1 + slippage_rate)
        #     elif amount < 0:
        #         return price * (1 - slippage_rate)
        #     else:
        #         return price

        tmp_price_list = []
        for idx in range(self.code.size):
            code = self.code[idx]
            price = market_data_df.query(
                'code == @code and time == @time')['close'].iloc[0]
            tmp_price_list += [price]

        return np.array(tmp_price_list)

    def position_METHOD_init(self, context):
        '''position_METHOD_init generates (target)amount/(target)value under valid_Order(clssified order) to avoid considering unavailable stocks'''
        target_stock_list = self.target_stock_list
        stock_in_account = context.stock_account.code()
        amount_in_account = context.stock_account.total_amount()
        clear_stock_list = stock_in_account[
            ~np.isin(stock_in_account, target_stock_list)]
        # initialize df with all 0 value in account_amount col
        adjust_df = pd.DataFrame(0,
                                 index=self.code,
                                 columns=['account_amount'])

        # account_amount of stocks in account is updated
        # ps. not all stocks in account are updated, unavailable(paused) stocks exclude(not in index)
        adjust_df.account_amount.update(
            pd.Series(amount_in_account, index=stock_in_account))

        # stocks in account but not in target_list are to be sold out
        adjust_df['target_amount'] = pd.Series(0, index=clear_stock_list)

        # stocks in target_list
        adjust_df['target_value'] = pd.Series(
            weight_fun_dict[self.weight_type](adjust_df, self.position_target,
                                              context),
            index=target_stock_list)
        self.adjust_df = adjust_df

    def order_match(self,
                    context) -> '(valid_order, dealt_order, canceled_order)':

        # expiry test
        if context.current_time.date() > self.order_establish_time.date():
            valid_order, dealt_order, canceled_order = None, None, canceled_Order(
                context.current_time)
        # 待考虑涨跌停,若涨停保留在valid order 等下次match
        # valid order --> canceled_order & new valid order(涨跌停无法交易，价格未触碰到的) & dealing order/dealt
        # 考虑逻辑：挂单即扣减可用资金
        elif self.order_type == 'mkt':
            # assume all stocks in this order will deal, no need to split
            # 未用split 故没有boolean，可以统一用boolean(all True) split的形式
            valid_order, dealt_order, canceled_order = None, dealt_Order(
                self, context.current_time), None
            # 对dealt_order进行进一步筛选，若资金充足则全通过，
            # 不足则考虑1.减少amount至足额购买，2.减少购买的股票。重新更新.
            # 减少部分进入canceled_order,考虑给canceled_order加入merge method用于合并
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


class dealing_Order:
    pass


class canceled_Order(valid_Order):
    def __init__(self, cur_time):
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
        self.transaction_price = self.set_price(order_dealt_time)
        ORDER_METHOD = dict(amount=self.order,
                            target_amount=self.order_target,
                            value=self.order_value,
                            target_value=self.order_target_value,
                            position_target=self.target_position)
        self.METHOD_chosen = ORDER_METHOD[self.METHOD_chosen]

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

    def trade_cost(self, amount, transaction_price):
        '''券商手续费双向收取，印花税卖出时收取，后期甄别'''

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

    def trade_process(self, code_list, price_list, amount_list, extra_fee_list,
                      context):
        # 考虑添加 transaction_amount 实际成交数量至self

        buy_boolean_list = amount_list > 0
        sell_boolean_list = ~buy_boolean_list
        # sell first
        for code, amount, price, extra_fee in zip(
                code_list[sell_boolean_list], amount_list[sell_boolean_list],
                price_list[sell_boolean_list],
                extra_fee_list[sell_boolean_list]):
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

    def order(self, context, *args):
        if args:
            price_list, code_list, amount_list = args
        else:
            price_list, code_list, amount_list = self.transaction_price, self.code, self.amount
        amount_list = (np.floor(amount_list / 100) * 100).astype(int)
        transaction_cost = self.trade_cost(amount_list, price_list)
        self.trade_process(code_list, price_list, amount_list,
                           transaction_cost, context)

    def order_target(self, context, *args):
        if args:
            price_list, code_list, target_amount_list = args
        else:
            price_list, code_list, target_amount_list = self.transaction_price, self.code, self.target_amount
        stock_account = context.stock_account
        amount_list = np.array([
            (target_amount_list[i] -
             stock_account.total_amount(code_list[i]) if
             target_amount_list[i] > stock_account.locked_amount(code_list[i])
             else -stock_account.tradable_amount(code_list[i]))
            if stock_account.in_acount(code_list[i]) else target_amount_list[i]
            for i in range(code_list.size)
        ])
        amount_list = (np.floor(amount_list / 100) * 100).astype(int)
        transaction_cost = self.trade_cost(amount_list, price_list)
        self.trade_process(code_list, price_list, amount_list,
                           transaction_cost, context)

    def order_value(self, context, *args):
        if args:
            price_list, code_list, order_value = args
        else:
            price_list, code_list, order_value = self.transaction_price, self.code, self.order_value
        amount_list = order_value / price_list
        self.order(context, *(price_list, code_list, amount_list))

    def order_target_value(self, context, *args):
        if args:
            price_list, code_list, target_value_list = args
        else:
            price_list, code_list, target_value_list = self.transaction_price, self.code, self.target_value
        target_amount_list = target_value_list / price_list
        self.order_target(context,
                          *(price_list, code_list, target_amount_list))

    def target_position(self, context):
        adjust_df = self.adjust_df.loc[self.code]
        adjust_df['transaction_price'] = pd.Series(self.transaction_price,
                                                   index=self.code)

        # deal with stocks in account but not in target_list
        # target_amount = 0, otherwise = nan
        # nan == nan -> False, 0 == 0 -> True
        order_target_df = adjust_df.query('target_amount == target_amount')
        if order_target_df.size != 0:
            self.order_target(
                context,
                *(order_target_df['transaction_price'].to_numpy(),
                  order_target_df.index.to_numpy(),
                  order_target_df['target_amount'].to_numpy()))

        # deal with stocks in target_list
        # target_value = number, otherwise = nan
        order_target_value_df = adjust_df.query('target_value == target_value')
        if order_target_value_df.size != 0:
            self.order_target_value(
                context,
                *(order_target_value_df['transaction_price'].to_numpy(),
                  order_target_value_df.index.to_numpy(),
                  order_target_value_df['target_value'].to_numpy()))
