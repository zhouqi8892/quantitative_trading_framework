import pandas as pd
import numpy as np


def equal_amount(portfolio_list, percentage, context):
    '''equal amount先将账户中组合以外的股票卖出再计算组合weight，仓位更精确'''
    position_dict = context.portfolio.subportfolios[0].long_positions
    current_data = get_current_data()
    #-----------------------计算账户有效资产总价值（现金+股票）-----------------------
    cash_avai_value = context.portfolio.subportfolios[0].available_cash
    account_stock_value = context.portfolio.subportfolios[0].positions_value
    account_paused_stock_total_value = sum([
        position_dict[stock].value for stock in position_dict.keys()
        if current_data[stock].paused
    ])  # 排除停牌不可卖的股票价值
    total_avai_value = cash_avai_value + account_stock_value - account_paused_stock_total_value
    #---------------------------------------------------------------------------------

    weight_list = np.ones(len(portfolio_list))
    # equal amount
    price_portfolio_available_list = np.array(
        [current_data[code].last_price for code in portfolio_list])
    portfolio_share = np.floor(total_avai_value * percentage /
                               price_portfolio_available_list.sum())
    if portfolio_share == 0:
        log.info('构建组合不足1份，换仓失败')
        return []
    else:
        portfolio_amount_list = portfolio_share * weight_list
        return portfolio_amount_list


def equal_weighted(portfolio_list, percentage, context):
    '''equal amount先将账户中组合以外的股票卖出再计算组合weight，仓位更精确'''
    position_dict = context.portfolio.subportfolios[0].long_positions
    current_data = get_current_data()
    #-----------------------计算账户有效资产总价值（现金+股票）-----------------------
    cash_avai_value = context.portfolio.subportfolios[0].available_cash
    account_stock_value = context.portfolio.subportfolios[0].positions_value
    account_paused_stock_total_value = sum([
        position_dict[stock].value for stock in position_dict.keys()
        if current_data[stock].paused
    ])  # 排除停牌不可卖的股票价值
    total_avai_value = cash_avai_value + account_stock_value - account_paused_stock_total_value
    #---------------------------------------------------------------------------------

    value_list = np.ones(
        len(portfolio_list)) * total_avai_value / len(portfolio_list)
    # equal values
    price_portfolio_available_list = np.array(
        [current_data[code].last_price for code in portfolio_list])

    portfolio_amount_list = value_list / price_portfolio_available_list
    return portfolio_amount_list


def value_weighted(portfolio_list, percentage, context):
    '''equal amount先将账户中组合以外的股票卖出再计算组合weight，仓位更精确'''
    from jqdata import get_valuation
    from dateutil.relativedelta import relativedelta
    #mkt_cap_df = get_valuation(portfolio_list, end_date=context.current_dt-relativedelta(days=1), count=1, fields=['circulating_market_cap'])
    #有bug，若当日上市，则无上一日市值，数据库返回df直接少一行，而不是给nan
    mkt_cap_df = get_valuation(portfolio_list,
                               end_date=context.current_dt,
                               count=1,
                               fields=['circulating_market_cap'])
    value_weighted_list = mkt_cap_df[
        'circulating_market_cap'].values / mkt_cap_df[
            'circulating_market_cap'].sum()
    position_dict = context.portfolio.subportfolios[0].long_positions
    current_data = get_current_data()
    #-----------------------计算账户有效资产总价值（现金+股票）-----------------------
    cash_avai_value = context.portfolio.subportfolios[0].available_cash
    account_stock_value = context.portfolio.subportfolios[0].positions_value
    account_paused_stock_total_value = sum([
        position_dict[stock].value for stock in position_dict.keys()
        if current_data[stock].paused
    ])  # 排除停牌不可卖的股票价值
    total_avai_value = cash_avai_value + account_stock_value - account_paused_stock_total_value
    #---------------------------------------------------------------------------------

    value_list = total_avai_value * value_weighted_list
    # values weighted
    price_portfolio_available_list = np.array(
        [current_data[code].last_price for code in portfolio_list])

    portfolio_amount_list = value_list / price_portfolio_available_list
    return portfolio_amount_list


weight_fun_dict = {
    'equal_amount': equal_amount,
    'value_weighted': value_weighted,
    'price_weighted': 0,
    'equal_weighted': equal_weighted,
}
