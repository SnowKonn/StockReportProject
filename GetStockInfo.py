import requests
import pandas as pd
import datetime as dt
import numpy as np

from io import StringIO
from datetime import datetime
from Package.Data_Base_Method import *

# FIXME: temp library
import csv

def set_asset_code_name_table(local_db_instance):
    create_table_query = """
        CREATE TABLE IF NOT EXISTS asset_codes(
            
            market text,
            asset_code text PRIMARY KEY,
            asset_name text
            )
    """

    local_db_instance.excecute_sql_query(create_table_query)

def set_daily_asset_price_table(local_db_instance):
    create_table_query = """
        CREATE TABLE IF NOT EXISTS asset_price(
            id integer PRIMARY KEY AUTOINCREMENT,
            days text,
            asset_code text,
            open real,
            close real,
            high real,
            low real,
            volume real
            )
    """

    local_db_instance.excecute_sql_query(create_table_query)


def set_krx_individual_data_table(local_db_instance):
    create_table_query = """
        CREATE TABLE IF NOT EXISTS krx_individual_data(
            days text,
            asset_code text,
            asset_name text,
            managed_asset text,
            EPS text,
            PER text,
            BPS text,
            PBR text,
            dividend text
            )
    """

    local_db_instance.excecute_sql_query(create_table_query)

def set_krx_sector_data_table(local_db_instance):
    create_table_query = """
        CREATE TABLE IF NOT EXISTS krx_sector_data(
            days text,
            market_name text,
            asset_code text,
            asset_name text,
            sector_name text,
            price text,
            market_value text
            )
    """

    local_db_instance.excecute_sql_query(create_table_query)

def get_krx_individual_data(date_list):

    krx_otp_url = 'http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.125 Safari/537.36',
        'Referer': krx_otp_url
    }
    sector_data_list = []
    for i in range(len(date_list)):
        date = date_list[i]
        print(date)
        params = dict(name='fileDown',
                      filetype='csv',
                      url="MKD/13/1302/13020401/mkd13020401",
                      market_gubun='ALL',
                      gubun='1',
                      schdate=date,
                      pagePath="/contents/MKD/13/1302/13020401/MKD13020401.jsp")

        res = requests.get(krx_otp_url, params=params, headers=headers)
        otp = res.text

        download_url = 'http://file.krx.co.kr/download.jspx'
        down_sector = requests.post(download_url, params={'code': otp}, headers=headers)
        sector_data = down_sector.text

        sector_data_df = pd.read_csv(StringIO(sector_data))
        sector_data_df['days'] = date
        sector_data_list.append(sector_data_df)

    result_df = pd.concat(sector_data_list, axis=0)

    return result_df


def get_krx_sector_data(date_list):
    krx_otp_url = 'http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.125 Safari/537.36',
        'Referer': krx_otp_url
    }
    sector_data_list = []
    for i in range(len(date_list)):
        date = date_list[i]
        print(date)
        params = dict(name='fileDown',
                      filetype='csv',
                      url='MKD/03/0303/03030103/mkd03030103',
                      tp_cd='ALL',
                      date=date,
                      lang='ko',
                      pagePath='/contents/MKD/03/0303/03030103/MKD03030103.jsp')

        res = requests.get(krx_otp_url, params=params, headers=headers)
        otp = res.text

        download_url = 'http://file.krx.co.kr/download.jspx'
        down_sector = requests.post(download_url, params={'code': otp}, headers=headers)
        sector_data = down_sector.text

        sector_data_df = pd.read_csv(StringIO(sector_data))
        sector_data_df['days'] = date
        sector_data_list.append(sector_data_df)

    result_df = pd.concat(sector_data_list, axis=0)

    return result_df

def set_unique_asset_codes_to_local_db(local_db_instance):
    select_asset_code_sector = 'SELECT days, market_name, asset_code, asset_name FROM krx_sector_data'
    results = local_db_instance.select_db(select_asset_code_sector)
    asset_code_df = pd.DataFrame(results, columns=['days', 'market_name', 'asset_code', 'asset_name'])
    asset_code_df = asset_code_df.sort_values(['asset_code', 'days'], ascending=False)
    asset_code_unique = asset_code_df.drop_duplicates('asset_code')

    db_columns = ['market', 'asset_code', 'asset_name']
    df_columns = ['market_name', 'asset_code', 'asset_name']

    asset_code_unique_db_form = asset_code_unique[df_columns].copy()
    asset_code_unique_db_form.rename(columns={'market_name': 'market'}, inplace=True)

    local_db_instance.insert_non_exist_row_database_multi_rows('asset_codes', db_columns,
                                                               np.array(asset_code_unique_db_form))

    select_asset_code_sector = 'SELECT days, asset_code, asset_name FROM krx_individual_data'
    results = local_db_instance.select_db(select_asset_code_sector)
    asset_code_df = pd.DataFrame(results, columns=['days', 'asset_code', 'asset_name'])
    asset_code_df = asset_code_df.sort_values(['asset_code', 'days'], ascending=False)
    asset_code_unique = asset_code_df.drop_duplicates('asset_code')

    db_columns = ['asset_code', 'asset_name']
    df_columns = ['asset_code', 'asset_name']

    asset_code_unique_db_form = asset_code_unique[df_columns].copy()
    local_db_instance.insert_non_exist_row_database_multi_rows('asset_codes', db_columns,
                                                               np.array(asset_code_unique_db_form))

if __name__ == "__main__":
    db_path = 'Data/db_instance.db'
    local_db_instance = LocalDBMethods(db_path)
    set_krx_sector_data_table(local_db_instance)
    set_krx_individual_data_table(local_db_instance)

    load_period_year = 0.3
    date_interval = 7 * 12
    loop_n = int(load_period_year * 365 / date_interval)

    date_list = [(datetime.now() - dt.timedelta(i * date_interval) - dt.timedelta(1)).strftime('%Y%m%d') for i in range(loop_n)]

    # Get sector data from krx
    result_df = get_krx_sector_data(date_list)

    column_names_market_sector = {
        'days': 'days', '시장구분' :'market_name', '종목코드': 'asset_code', '종목명':'asset_name',
        '산업분류': 'sector_name', '현재가(종가)': 'price', '시가총액(원)' :'market_value'
    }

    result_df.rename(columns=column_names_market_sector, inplace=True)
    result_df = result_df.loc[:, result_df.columns != '전일대비']

    local_db_instance.insert_non_exist_row_database_multi_rows('krx_sector_data', result_df.columns.to_list(),
                                                               np.array(result_df))
    # Get individual stock data from krx
    result_df = get_krx_individual_data(date_list)

    column_names_individual = {
        '종목코드': 'asset_code', '종목명':'asset_name','관리여부': 'managed_asset', 'EPS': 'EPS', 'PER': 'PER',
        'BPS': 'BPS', 'PBR': 'PBR', '주당배당금': 'dividend', 'days': 'days'
    }

    result_df.rename(columns=column_names_individual, inplace=True)
    result_df = result_df[column_names_individual.values()]

    local_db_instance.insert_non_exist_row_database_multi_rows('krx_individual_data', result_df.columns.to_list(),
                                                               np.array(result_df))


    ## Get unique asset codes
    # Save the unique asset codes and save the corresponding asset name with the latest company name
    # (ex 동부화재 -> DB화재보험)
    set_asset_code_name_table(local_db_instance)
    set_unique_asset_codes_to_local_db(local_db_instance)

    # Make the stock ticker form asset code table
    select_asset_codes = "SELECT market, asset_code from asset_codes"
    asset_code_lists = local_db_instance.select_db(select_asset_codes)
    asset_code_df = pd.DataFrame(asset_code_lists, columns=['market', 'asset_code'])
    asset_code_df['ticker'] = '-'

    for i in range(len(asset_code_df)):
        market = asset_code_df.loc[i, 'market']
        asset_code = asset_code_df.loc[i, 'asset_code']

        if market == '코스닥':
            asset_code_df.loc[i, 'ticker'] = asset_code + '.KQ'
        elif market == '코스피':
            asset_code_df.loc[i, 'ticker'] = asset_code + '.KS'

    asset_ticker_list = asset_code_df['ticker'].copy()
    asset_ticker_list = asset_ticker_list.loc[asset_ticker_list != '-']
    with open('Data/code_ticker.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(asset_ticker_list)

    set_daily_asset_price_table(local_db_instance)

    # stock_prices = pd.read_csv('Data/stock_price.csv')
    # stock_prices['Date'] = stock_prices['Date'].map(lambda x: ''.join(x.split('-')))
    # stock_prices['code'] = stock_prices['code'].map(lambda x: (x.split('.'))[0])
    # stock_prices = stock_prices.rename(columns={'Date':'days', 'code':'asset_code'})
    # local_db_instance.insert_database_multi_rows('asset_price', stock_prices.columns.tolist(), np.array(stock_prices))