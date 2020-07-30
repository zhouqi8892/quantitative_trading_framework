import pandas as pd
import numpy as np
import statsmodels.api as sm
if __name__ == "__main__":
    import jqdatasdk as jq
    import datetime
    from dateutil.relativedelta import relativedelta
else:
    from ..action.order import order, order_target, position_clear, position_adjust


def preperation(context):
    pass


def market_open(context):
    print(context.current_date)
    if context.current_date == pd.to_datetime('2010-3-4'):
        order(['000001', '000002'], [200, 100], context)
    elif context.current_date == pd.to_datetime('2010-3-5'):
        order(['000001', '000004'], [100, 200], context)
    elif context.current_date == pd.to_datetime('2010-3-8'):
        order_target(['000001', '000002', '000005'], [100, 300, 500], context)
    else:
        position_adjust(context, percentage=0.9, method='equal weight')
        # pass


    # print(context.stock_account.detail())
    # print(context.current_date)
def market_close(context):
    pass


if __name__ == "__main__":
    jq.auth('13375779399', '88923392')
    # jq.auth('15869688376', '688376')
    count = 12
    unit = '1M'
    stock_df = jq.get_all_securities(['stock'], '22000101')
    stock_df_exc_ST_DELIST = stock_df[
        (~stock_df.display_name.str.contains('ST'))
        & (~stock_df.display_name.str.contains('退市'))]
    price_df = jq.get_price(list(stock_df_exc_ST_DELIST.index),
                            end_date=datetime.date.today(),
                            count=1,
                            fill_paused=False)
    price_df.sort_values('code', inplace=True)
    avai_stock_list = list(price_df[~pd.isna(price_df['volume'])].code)
    # margin_list = jq.get_marginsec_stocks() 获取可融券列表
    market_df = jq.get_bars(avai_stock_list,
                            count,
                            unit, ['date', 'close'],
                            fq_ref_date=datetime.date.today())
    market_df.index.names = ['code', 'idx']
    market_df_copy = market_df.copy()
    market_df_copy['year'] = market_df_copy['date'].apply(lambda x: x.year)
    market_df_copy['month'] = market_df_copy['date'].apply(lambda x: x.month)
    market_df_copy.index = market_df_copy.index.droplevel(1)
    market_df_copy['pd_datetime'] = pd.to_datetime(market_df_copy.date)
    market_df_copy.reset_index(inplace=True)

    #---------------------------------get data--------------------------
    date_list = market_df_copy.date.unique()

    char_df = pd.DataFrame()
    for date in date_list:
        q = jq.query(jq.valuation.day, jq.valuation.code,
                     jq.valuation.circulating_market_cap,
                     jq.valuation.pb_ratio, jq.valuation.pe_ratio_lyr).filter(
                         jq.valuation.code.in_(avai_stock_list))
        result = jq.get_fundamentals(q, date)
        char_df = char_df.append(result)
    char_df.day = pd.to_datetime(char_df.day)
    char_df.circulating_market_cap = char_df.circulating_market_cap.apply(
        np.log)
    char_df.pb_ratio = char_df.pb_ratio.apply(lambda x: np.log(1 / x))
    char_df.pe_ratio_lyr = 1 / char_df.pe_ratio_lyr
    char_df.sort_values(['day', 'code'], inplace=True)

    #---------------------------------merge_df--------------------------

    year = datetime.date.today().year
    month = datetime.date.today().month

    def return_cal(df):
        judge_date = df.date.iloc[0] + relativedelta(months=count)
        if df.shape[
                0] == count and judge_date.year == year and judge_date.month == month:
            df['return'] = df['close'].diff() / df['close'].shift()
            # 记得删除第一行,刷一波数据不连续的股票
            return df.iloc[1:, :]

    market_df_integrate = market_df_copy.groupby(
        'code', group_keys=False).apply(return_cal)

    market_df_integrate = pd.merge(market_df_integrate,
                                   char_df,
                                   'left',
                                   left_on=['pd_datetime', 'code'],
                                   right_on=['day', 'code'])

    def drop_stock_with_nan(df):
        if not pd.isna(df).values.any():
            return df

    market_df_integrate = market_df_integrate.groupby(
        'code', group_keys=False).apply(drop_stock_with_nan)
    # ---------------------------reg result------------------------------------
    market_df_integrate['const'] = 1

    def ols_params(data, xcols, ycol):
        return sm.OLS(data[ycol], data[xcols]).fit().params[:]

    ycol = 'return'
    xcols = ['circulating_market_cap', 'pb_ratio', 'pe_ratio_lyr', 'const']
    res = market_df_integrate.groupby(['year', 'month']).apply(ols_params,
                                                               ycol=ycol,
                                                               xcols=xcols)
    result_df = res.apply(lambda x: pd.Series(
        [np.mean(x), np.sqrt(len(x)) * np.mean(x) / np.std(x)],
        index=['estimator', 'T_value'])).iloc[:, :-1]

    # -----------------------group sort------------------------------------
    group_num = 4

    def assign_group_num(series, break_point_df):
        def fun(x, break_point_list):
            # lebel1 is the biggest
            return group_num - len(break_point_list[break_point_list < x])

        item = series.name
        series = series.apply(lambda x: fun(x, break_point_df[item].values))
        return series

    char_list = ['circulating_market_cap', 'pb_ratio', 'pe_ratio_lyr']
    max_date = market_df_integrate.date.max()
    rank_df = market_df_integrate[
        (market_df_integrate.year == max_date.year)
        & (market_df_integrate.month == max_date.month)].copy()
    break_point_df = rank_df.quantile(
        [1 / group_num * i for i in range(1, group_num)]).loc[:, char_list]
    temp_df = rank_df.loc[:, char_list].apply(
        lambda x: assign_group_num(x, break_point_df), axis=0)
    rank_df.update(temp_df)
    rank_df = rank_df.loc[:, ['code'] + char_list]

    # -----------------------pick stock------------------------------------
    treshold_val = 2
    portfolio_min_num = 20
    risk_factor_list = result_df.loc['estimator', :].values
    # 考虑factor大小对char的倾向
    T_list = result_df.loc['T_value', :].values
    idx = np.arange(len(T_list))[np.abs(T_list) >= treshold_val]
    for i in idx:
        rank_df = rank_df[rank_df.iloc[:, i + 1] == (
            1 if T_list[i] > 0 else group_num)]
    selected_code_list = rank_df.code.values if rank_df.shape[
        0] >= portfolio_min_num else []
    # 组合个股数量少于20则不交易
    print(selected_code_list)