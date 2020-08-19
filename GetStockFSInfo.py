import time
import requests
import pandas as pd
import datetime as dt
import numpy as np
from bs4 import BeautifulSoup
import configparser as cp
import re

from io import StringIO
from datetime import datetime
from Package.Data_Base_Method import *
from selenium import webdriver


# FIXME: temp library
import csv

def get_initial_request_info(local_db_instance):

    driver = webdriver.Chrome()
    driver.get('http://www.fnspace.com/DataMart/RequestInfo?aid=A000002&pid=P0002')
    time.sleep(3)
    sample = driver.find_element_by_css_selector("[data-id='Result']")
    sample.click()
    time.sleep(3)

    result_list = pd.read_html(driver.page_source)

    request_info_df = result_list[1]
    request_info_df.to_csv('Data/request_info.csv', encoding='euc-kr')
    request_info_df.rename(columns={'결과 변수명': 'var_name', '결과 변수타입': 'var_type', '단위': 'unit', '설명': 'description'},
                           inplace=True)
    request_info_df.to_sql('fs_request_codes', local_db_instance.conn, if_exists='replace')

if __name__ == "__main__":
    initial = False
    db_path = 'Data/db_instance.db'
    local_db_instance = LocalDBMethods(db_path)

    cfg = cp.ConfigParser()
    cfg.read("./Config/config.ini")
    # Set initial request data codes
    if initial:
        get_initial_request_info(local_db_instance)

    requests_columns = ['var_name', 'var_type', 'unit', 'description']
    requests_info = local_db_instance.select_db('SELECT %s FROM fs_request_codes' % ', '.join(requests_columns))
    requests_info_df = pd.DataFrame(requests_info)
    requests_info_df.columns = requests_columns
    request_info_codes = requests_info_df.iloc[22:61, 0]
    exclude_request_codes = ['M122720', 'M122710', 'M123200', 'M115020', 'M115850']

    request_info_codes = request_info_codes.loc[~request_info_codes.map(lambda x: x in exclude_request_codes)]

    request_info_codes_list = request_info_codes.tolist()
    app_key = cfg._defaults['app_key']

    distinct_date_code_query = 'SELECT distinct(days) FROM krx_sector_data'
    distinct_date_list = local_db_instance.select_db(distinct_date_code_query)
    distinct_date_list = pd.DataFrame(distinct_date_list)[0].to_list()
    distinct_date_list = sorted(distinct_date_list)

    exclude_pref_list = ['005745', '007815', '010145', '008355', '003945', '000687', '000425', '000687', '007815',
                         '006375', '017555', '007595', '009385', '016575', '004255', '003075', '004149', '004147']

    select_column_list = ['days', 'market_name', 'asset_code', 'asset_name', 'market_value', 'sector_name']
    df_tot_list = []
    for i in range(len(distinct_date_list)):

        market_value_select_query = "SELECT %s FROM krx_sector_data where days = '%s'" % (', '.join(select_column_list),
                                                                                              distinct_date_list[i])

        results_df = pd.DataFrame(local_db_instance.select_db(market_value_select_query), columns=select_column_list)
        results_df['market_value'] = results_df['market_value'].map(lambda x: int(x.replace(',', '', 100)))
        results_df_sorted = results_df.sort_values('market_value', ascending=False)
        pref_stock_list = results_df_sorted['asset_name'].map(lambda x: ('(' in x) & ('우' in x))

        results_df_pref_cleaned = results_df_sorted.loc[~pref_stock_list, :]

        spac_stock_list = results_df_pref_cleaned['asset_name'].map(lambda x: ('스팩' in x))
        results_df_spac_cleaned = results_df_pref_cleaned.loc[~spac_stock_list, :]

        regex = re.compile(r'.+우[A-Z]*$')

        woo_list = results_df_spac_cleaned['asset_name'].map(lambda x: True if regex.match(x) else False)
        woo_stock_list = results_df_spac_cleaned.loc[woo_list, :]

        regex = re.compile(r'[ ]*[0-9]*우[A-Z]*$')
        stock_name_modified = woo_stock_list['asset_name'].map(lambda x: regex.split(x)[0])
        true_pref_stock_list = stock_name_modified.map(lambda x: x in results_df_spac_cleaned['asset_name'].tolist())
        true_pref_stock_index = woo_stock_list.loc[true_pref_stock_list, :].index

        results_df_pref_re_cleaned = results_df_spac_cleaned.loc[~results_df_spac_cleaned.index.isin(true_pref_stock_index)]

        regex = re.compile(r'.+[0-9]+호$')
        regex_2 = re.compile(r'.+[0-9]+호SPAC$')
        stock_name_modified = results_df_pref_re_cleaned['asset_name'].map(lambda x: True if ((regex.match(x) is not None)
                                                                                              |(regex_2.match(x) is not None))
                                                                                          else False)


        results_df_tot_cleaned = results_df_pref_re_cleaned.loc[~stock_name_modified]
        exclude_stock_list = results_df_tot_cleaned['asset_code'].map(lambda x: x in exclude_pref_list)
        results_df_tot_cleaned = results_df_tot_cleaned.loc[~exclude_stock_list]
        results_df_tot_cleaned = results_df_tot_cleaned.drop_duplicates(subset=['asset_code'])

        regex = re.compile(r'.+우[A-Z]*$')

        # woo_list =  results_df_tot_cleaned['asset_name'].map(lambda x: True if regex.match(x) else False)
        # pref_list.append(results_df_tot_cleaned.loc[woo_list, :])
        df_tot_list.append(results_df_tot_cleaned)
    del results_df_spac_cleaned, results_df_pref_cleaned, results_df_pref_re_cleaned, results_df_sorted, results_df

    top_rank_thr = 80
    top_rank_stocks_df_list = []
    for i in range(len(df_tot_list)):
        top_rank_stocks_df_list.append(df_tot_list[i].iloc[:top_rank_thr, 2:6].copy())

    top_rank_asset_df = pd.concat(top_rank_stocks_df_list, ignore_index=True)
    medicine_idx = top_rank_asset_df['sector_name'].map(lambda x: (x == '의약품') | (x == '제약') | (x == '기타서비스'))
    top_rank_asset_df_exclude_medicine = top_rank_asset_df.loc[~medicine_idx]
    top_rank_asset_df_exclude_medicine.drop_duplicates(subset='asset_code')




