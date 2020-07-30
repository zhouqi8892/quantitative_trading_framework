from platform_sys.execution.back_test import back_test_init
from platform_sys.account.settings import account
from platform_sys.action.settings import order_cost
from datetime import datetime
'''
1.确定回测阶段
2.确定回测频率
3.确认起始资金
4.确认交易费率
5.确认策略选择
6.指定账户数据库名接入/新建模拟账户
'''


class config:
    strategy = 'strategy_demo'
    start_date = '2010/03/04'
    end_date = '2018/03/04'
    frequency = 'daily'  # 'or tick, minute'
    initial_capital = 1000000000
    account_list = [account.cash, account.stock]
    trade_cost = {
        order_cost.brokerage_fee: [2 / 10000, 5],
        order_cost.stamp_tax: 1 / 1000
    }


if __name__ == "__main__":
    starttime = datetime.now()

    back_test_init(config)
    # from mongoengine import connect
    # a = connect('test', host='mongodb：// localhost / china_money_benchmark')
    endtime = datetime.now()

    print('回测完成，总耗时%s秒' % (endtime - starttime).seconds)
