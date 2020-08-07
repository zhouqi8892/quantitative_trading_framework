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

            # record context.current_time as order_establish_time
            # update code to be traded(including stocks in account) if apply portfolio_adjust_methods
            new_orders = {
                k: v(context)
                for k, v in list(order_shelve.items())[-diff:]
            }

            self.validate_process(new_orders, context)

            # update latest_order_idx for next judgement
            self.latest_order_idx = int(order_shelve['0'])

        # match_process conducts at each loop time
        self.match_process(context)

    def validate_process(self, new_orders, context):
        '''classify new orders to valid_order/invalid_order'''
        for k, v in new_orders.items():
            valid_orders, invalid_orders = v.order_validity(context)
            if len(valid_orders.code) != 0:
                self.valid_order.update({k: valid_orders})
            if len(invalid_orders.code) != 0:
                self.invalid_order.update({k: invalid_orders})
                print(invalid_orders.code, '下单指令作废')

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
