### Reference URL: https://dart-fss.readthedocs.io/en/latest/welcome.html

import pandas as pd
import numpy as np
import dart_fss as dart
from dart_fss import get_corp_list
import configparser as cp

# 모든 상장된 기업 리스트 불러오기
cfg = cp.ConfigParser()
cfg.read("./Config/config.ini")
api_key = cfg._defaults['dart_fss_key']
dart.set_api_key(api_key=api_key)

corp_list = get_corp_list()


# 증권 코드를 이용한 찾기
samsung = corp_list.find_by_stock_code('005930')

# 다트에서 사용하는 회사코드를 이용한 찾기
samsung = corp_list.find_by_corp_code('00126380')

# "삼성"을 포함한 모든 공시 대상 찾기
corps = corp_list.find_by_name('삼성')

# "삼성"을 포함한 모든 공시 대상중 코스피 및 코스닥 시장에 상장된 공시 대상 검색(Y: 코스피, K: 코스닥, N:코넥스, E:기타)
# corps = corp_list.find_by_name('삼성', market=['Y','K']) # 아래와 동일

# "휴대폰" 생산품과 연관된 공시 대상
corps = corp_list.find_by_product('휴대폰')

# "휴대폰" 생산품과 연관된 공시 대상 중 코스피 시장에 상장된 대상만 검색
corps = corp_list.find_by_product('휴대폰', market='Y')

# 섹터 리스트 확인
corp_list.sectors

# "텔레비전 방송업" 섹터 검색
corps = corp_list.find_by_sector('텔레비전 방송업')

reports = samsung.search_filings(bgn_de='20190301', end_de='20190531')

# 2010년 1월 1일부터 현재까지 모든 사업보고서 검색
reports = samsung.search_filings(bgn_de='20100101', pblntf_detail_ty='a001')

# 2010년 1월 1일부터 현재까지 모든 사업보고서의 최종보고서만 검색
reports = samsung.search_filings(bgn_de='20100101', pblntf_detail_ty='a001', last_reprt_at='Y')

# 2010년 1월 1일부터 현재까지 사업보고서, 반기보고서, 분기보고서 검색
reports = samsung.search_filings(bgn_de='20100101', pblntf_detail_ty=['a001', 'a002', 'a003'])

# 2012년 1월 1일부터 현재까지 연간 연결재무제표 검색
fs = samsung.extract_fs(bgn_de='20120101')



# 2012년 1월 1일부터 현재까지 분기 연결재무제표 검색 (연간보고서, 반기보고서 포함)
fs_quarter = samsung.extract_fs(bgn_de='20120101', report_tp='quarter')

# 2012년 1월 1일부터 현재까지 개별재무제표 검색
fs_separate = samsung.extract_fs(bgn_de='20120101', separate=True)



# 삼성전자 code
corp_code = '00126380'

# 모든 상장된 기업 리스트 불러오기
corp_list = get_corp_list()

# 삼성전자
samsung = corp_list.find_by_corp_name(corp_code=corp_code)

# 2012년 01월 01일 부터 연결재무제표 검색
# fs = samsung.extract_fs(bgn_de='20120101') 와 동일
fs = dart.fs.extract(corp_code=corp_code, bgn_de='20120101')

# 연결재무상태표
df_fs = fs['bs'] # 또는 df = fs[0] 또는 df = fs.show('bs')
# 연결재무상태표 추출에 사용된 Label 정보
labels_fs = fs.labels['bs']

# 연결손익계산서
df_is = fs['is'] # 또는 df = fs[1] 또는 df = fs.show('is')
# 연결손익계산서 추출에 사용된 Label 정보
labels_is = fs.labels['is']

# 연결포괄손익계산서
df_ci = fs['cis'] # 또는 df = fs[2] 또는 df = fs.show('cis')
# 연결포괄손익계산서 추출에 사용된 Label 정보
labels_ci = fs.labels['cis']

# 현금흐름표
df_cf = fs['cf'] # 또는 df = fs[3] 또는 df = fs.show('cf')
# 현금흐름표 추출에 사용된 Label 정보
labels_cf = fs.labels['cf']