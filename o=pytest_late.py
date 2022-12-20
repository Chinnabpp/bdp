import pandas as pd
import pyspark
import numpy as np



def test_Port_ID_Not_loaded (cash_bucket, trigger_key, aurora_details):
    cash_file_path = f"s3://{cash_bucket}/{trigger_key}"
    cash_flow_df=pd.read_csv('C:\\Users\\sssaw\Downloads\\bdp-main\.venv\\bdp-main\\s3_data_df.csv')
    fund_df=pd.read_csv('C:\\Users\\sssaw\Downloads\\bdp-main\.venv\\bdp-main\\database_df.csv')
    result_df=pd.merge(cash_flow_df, fund_df, left_on='Port ID', right_on='port_id', how='left')
    final_df=result_df[result_df.port_id.isnull()]
    print('Port ID not joined:')
    print(final_df.loc[:,'Port ID'])

def test_renamed_columns(cash_df,fund_df,eff_dt):
    transform_df=transform(cash_df,fund_df,eff_dt)
    transform_columns_df = transform_df.columns
    assert transform_columns_df.count() == 14
    assert transform_columns_df[0] == 'net_cash_flow'
    assert transform_columns_df[1] == 'Subscriptions'
    assert transform_columns_df[2] == 'Redemptions'
    assert transform_columns_df[3] == 'exchanges_in'
    assert transform_columns_df[4] == 'exchanges_out'
    assert transform_columns_df[5] == 'conversions_in'
    assert transform_columns_df[6] == 'conversions_out'
    assert transform_columns_df[7] == 'reinvested_income'
    assert transform_columns_df[8] == 'reinvested_short_term_capital_gains'
    assert transform_columns_df[9] == 'reinvested_long_term_capital_gains'
    assert transform_columns_df[10] == 'port_id'
    assert transform_columns_df[11] == 'time_period_code'
    assert transform_columns_df[12] == 'effective_date'
    assert transform_columns_df[13] == 'file_key'

def test_columns_datatypes(cash_df,fund_df,eff_dt):
    transform_df=transform(cash_df,fund_df,eff_dt)
    assert transform_df.dtypes['net_cash_flow'] == np.int64 or transform_df.dtypes['net_cash_flow'] == np.float64
    assert transform_df.dtypes['Subscriptions'] == np.int64 or transform_df.dtypes['Subscriptions'] == np.float64
    assert transform_df.dtypes['Redemptions'] == np.int64 or transform_df.dtypes['Redemptions'] == np.float64
    assert transform_df.dtypes['exchanges_in'] == np.int64 or transform_df.dtypes['exchanges_in'] == np.float64
    assert transform_df.dtypes['exchanges_out'] == np.int64 or transform_df.dtypes['exchanges_out'] == np.float64
    assert transform_df.dtypes['conversions_in'] == np.int64 or transform_df.dtypes['conversions_in'] == np.float64
    assert transform_df.dtypes['Conversions_out'] == np.int64 or transform_df.dtypes['Conversions_out'] == np.float64
    assert transform_df.dtypes['reinvested_income'] == np.int64 or transform_df.dtypes['reinvested_income'] == np.float64
    assert transform_df.dtypes['reinvested_short_term_capital_gains'] == np.int64 or transform_df.dtypes['reinvested_short_term_capital_gains'] == np.float64
    assert transform_df.dtypes['reinvested_long_term_capital_gains'] == np.int64 or transform_df.dtypes['reinvested_long_term_capital_gains'] == np.float64
    assert transform_df.dtypes['effective_date'] == np.object_
