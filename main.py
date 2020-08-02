from platform_sys.execution.back_test import Hub
from platform_sys.account.settings import account
from platform_sys.action.settings import order_cost
from functools import partial
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
    start_date = '2018/06/29'
    end_date = '2020/03/04'
    frequency = 'daily'  # 'or tick, minute'
    initial_capital = 1000000000
    account_list = [account.cash, account.stock]


if __name__ == "__main__":
    # starttime = datetime.now()

    # back_test_init(config)
    # from mongoengine import connect
    # a = connect('test', host='mongodb：// localhost / china_money_benchmark')
    # endtime = datetime.now()

    # print('回测完成，总耗时%s秒' % (endtime - starttime).seconds)

    # partial(Hub, config)  #partial to fix config, input hyperparameters, aka optimization paras
    Hub(config).back_test()