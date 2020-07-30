from datetime import datetime
import pandas as pd
import h5py


def raw_data_df_generation(path):
    # logging.info('开始解析H5')
    print('开始解析H5')
    starttime = datetime.now()
    f = h5py.File(path, "r")
    test_s_array = f['eodprices']['table'][:]
    code_array = test_s_array['S_INFO_WINDCODE'].astype('U')
    date_array = test_s_array['TRADE_DT'].astype('U')
    price_array = test_s_array['values_block_0'].flatten()
    df = pd.DataFrame(columns=['code', 'date', 'price'])
    df['code'] = code_array
    df['date'] = pd.to_datetime(date_array)
    df['price'] = price_array
    df.sort_values(['code', 'date'], inplace=True)
    df.drop_duplicates(inplace=True)
    df.reset_index(drop=True, inplace=True)
    endtime = datetime.now()
    # logging.info('数据解析完成，耗时%s秒' % (endtime - starttime).seconds)
    print('数据解析完成，阶段耗时%s秒' % (endtime - starttime).seconds)
    return df
