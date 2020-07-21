# -*- conding: utf-8 -*-
# version 1.0.2
# parallels 사용자 유의사항
# 공유폴더 다운로드 체크해제 ->
# 사용하는 vm우클릭 ->
# 구성 -> 옵션 -> 공유 -> 구성 ->
# 다운로드 체크 해제

import os
import pathlib
from time import sleep

import pymysql

import sqlalchemy

#  selenium 이란 여러 언어에서 웹드라이버를 통해
#  웹 자동화 테스트 혹은 웹 자동화를 도와주는 라이브러리
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import InternalError

pymysql.install_as_MySQLdb()

from library.simulator_func_mysql import *

BACKSPACE = '\ue003'
ENTER = '\ue007'
TAB = '\ue004'
END = '\ue010'


class KINDCrawler:
    def __init__(self):
        db_url = URL(
            drivername="mysql+mysqldb",
            username=cf.db_id,
            password=cf.db_passwd,
            host=cf.db_ip,
            port=cf.db_port,
            database='daily_buy_list'
        )
        self.db_engine = create_engine(db_url)
        self.variable_setting()

    def variable_setting(self):
        self.FNAME_PATTERN = '투자??종목*.xls'
        # 2007년 이전에는 kind 상에 데이터 없다.
        # 크롤링 시작일
        self.DEFAULT_START_DATE = datetime.date(2007, 1, 1)
        # 엑셀에서 5000개만 담을 수 있어서 100일 단위로 조회하여 데이터를 불러옴
        self.rotate_period = 100

        # 아래 두 줄을 촬영 후 craw 함수로 옮겼습니다.
        # options = webdriver.ChromeOptions()
        # self.driver = webdriver.Chrome("C:\chromedriver\chromedriver", options=options)

        self.download_path = pathlib.Path.home() / 'Downloads'
        self.today = datetime.date.today()

    # 현재 다운로드 폴더 안에 있는 엑셀파일을 삭제
    def clean_excel(self):
        for fname in self.download_path.glob(self.FNAME_PATTERN):
            os.remove(fname)

    # kind 사이트에 달력에 날짜를 설정하는 함수
    def date_select(self, start, end):
        selected_tag_a = self.driver.find_element_by_css_selector('input#startDate')
        selected_tag_a.click()

        # 칸에서 가장 끝으로 이동
        selected_tag_a.send_keys(Keys.END)

        # kind 사이트의 날짜를 하나씩 지우는 로직
        for i in range(1, 12):
            # Keys 선언으로 가면 관련 코드 다 나와있다 (ctrl + keys 클릭)
            selected_tag_a.send_keys(Keys.BACKSPACE)
        selected_tag_a.send_keys(start.strftime('%Y%m%d'))

        selected_tag_a = self.driver.find_element_by_css_selector('input#endDate')
        selected_tag_a.click()

        selected_tag_a.send_keys(Keys.END)

        for i in range(1, 12):
            selected_tag_a.send_keys(Keys.BACKSPACE)
        selected_tag_a.send_keys(end.strftime('%Y%m%d'))

    def is_simul_table_exist(self, table_name):
        sql = "select 1 from information_schema.tables where table_schema = '%s' and table_name = '%s'"
        rows = self.db_engine.execute(sql % ("daily_buy_list", table_name)).fetchall()
        if len(rows) == 1:
            return True
        else:
            return False

    # 엑셀파일을 다운 받아서 db에 넣는 함수
    def insert_to(self, file_name, table_name):
        print("insert {} into {}".format(file_name, table_name))
        # kind 검색
        self.driver.find_element_by_xpath('//*[@id="searchForm"]/section/div/div[3]/a[1]').send_keys((Keys.ENTER))
        self.dialog_block_wait()

        # kind 엑셀다운로드
        self.driver.find_element_by_xpath('//*[@id="searchForm"]/section/div/div[3]/a[2]').send_keys((Keys.ENTER))
        sleep(2)  # 파일이 다 다운될 때 까지 대기

        # 엑셀 데이터를 가져온다.
        df = pd.read_html(
            str(self.download_path / file_name),
            header=0,
            converters={'종목코드': str}
        )[0]

        # 촬영 후 코드가 수정 되었지만 영상 후반에 설명이 나옵니다~
        # 엑셀 파일이 비어 있는 경우 code 컬럼에 '결과값이 없습니다' 라는 내용이 들어가 있다. 아래는 이러한 경우를 제외하는 로직
        df = df[df.종목코드 != '결과값이 없습니다.']


        # 만약에 df(데이터프레임)에 '해제일'이라는 컬럼이 있는 경우(투자경고, 투자위험 종목)
        if '해제일' in df.columns:
            # 해제일이 아직 지정되지 않은 경우는 '-' 값이 들어있다. 이럴 때는 해제일 컬럼을 None으로 변경
            df.loc[df.해제일 == '-', '해제일'] = None

        if len(df):
            del df['번호']
            df = df.rename(columns={
                '종목코드': 'code',
                '종목명': 'code_name',
                '공시일': 'post_date',
                '지정일': 'fix_date',
                '유형': 'type',
                '해제일': 'cleared_date'
            })

            df.to_sql(table_name, self.db_engine, if_exists='append',
                      dtype={
                          'code': sqlalchemy.types.VARCHAR(length=6),
                          'post_date': sqlalchemy.types.DATE,
                          'fix_date': sqlalchemy.types.DATE,
                          'cleared_date': sqlalchemy.types.DATE
                      })

        self.clean_excel()

    def get_last_date_from(self, table_name):
        date = self.DEFAULT_START_DATE

        if self.is_simul_table_exist(table_name):
            sql = "select post_date from {} order by post_date desc limit 1".format(table_name)
            try:
                return self.db_engine.execute(sql).fetchall()[0][0]
            except IndexError:
                pass

        return date

    # crawling하고, db에 넣는 함수
    def crawl_and_insert(self, file_name, table_name):
        # 달력이 종목 탭을 가려서 탭(투자위험종목 등) 클릭을 못하는 경우를 방지
        search_bar = self.driver.find_element_by_css_selector('#AKCKwd')
        search_bar.click()

        #title={  .  } 기준으로 문자가 들어간다. ex) ['투자주의종목','xls']
        selected_tab = self.driver.find_element_by_css_selector('a[title="{}"]'.format(file_name.split('.')[0]))
        selected_tab.click()
        self.dialog_block_wait()

        # 마지막 post날짜 가져와서 1일을 더해준다.
        start_date = self.get_last_date_from(table_name) + timedelta(1)
        end_date = start_date + timedelta(self.rotate_period)

        while start_date < self.today:
            self.date_select(start_date, end_date)
            self.dialog_block_wait()
            self.insert_to(file_name, table_name)

            start_date = end_date + timedelta(1)
            end_date = start_date + timedelta(self.rotate_period)

    # 크롤링 시작하는 함수
    def craw(self):
        # 시작전에 디렉토리 한번 정리
        self.clean_excel()

        # 아래 2라인은 촬영 후 variable_setting 함수에 있던 것을 옮겨왔습니다.
        options = webdriver.ChromeOptions()
        self.driver = webdriver.Chrome("C:\chromedriver\chromedriver", options=options)

        # 영상 촬영 후 추가 된 코드
        # stock_invest_warning(투자경고종목), stock_invest_danger(투자위험종목)
        # 의 경우는 항상 테이블을 삭제해준다.
        # 이유는 투자주의 종목과 다르게
        # 투자경고, 투자위험 종목은 엑셀파일에 '해제일' 컬럼이 있다. 따라서 매번 '해제일'을 업데이트 해줘야 하기 때문에
        # db를 삭제하고 다시 받아온다.
        # try, except 의 경우 혹시라도
        # stock_invest_warning, stock_invest_danger 테이블이 없을 경우 sql문을 실행하면 에러가 발생하기 때문에
        # 그럴 때는 그냥 에러로 인식 하지말고 pass 하라는 의미
        try:
            self.clean_database()
        except InternalError:
            pass

        # kind 사이트로 접속
        self.driver.get('http://kind.krx.co.kr/investwarn/investattentwarnrisky.do?method=investattentwarnriskyMain')
        self.dialog_block_wait()

        # 투자주의 / 투자경고 / 투자위험종목 리스트
        # 순서 : 투자주의종목-> 투자경고종목 -> 투자위험종목
        # ( ) 안에는 각각의 엑셀이름과 테이블 이름을 명시
        insert_table_names = [('투자주의종목.xls', 'stock_invest_caution'),
                              ('투자경고종목.xls', 'stock_invest_warning'),
                              ('투자위험종목.xls', 'stock_invest_danger')]
        for names in insert_table_names:
            # 영상 촬영 후 추가 된 코드
            # 투자경고종목, 투자위험 종목의 경우 kind에 데이터가 많지 않아서 6000일 단위로 조회해도 무리가 없음
            # for문의 처음 돌때 names에는 ('투자주의종목.xls', 'stock_invest_caution') 값이 들어가 있다.
            # 이 때 names[0]에는 '투자주의종목.xls', names[1]에는 'stock_invest_caution' 값이 들어가 있다.
            if names[0] != '투자주의종목.xls':
                self.rotate_period = 6000
            # 투자주의종목의 경우는 데이터가 많아서 100일 단위로 끊어서 조회
            else:
                self.rotate_period = 100

            self.crawl_and_insert(*names)

        # chrome 브라우저 닫기
        self.driver.close()

    # 로딩이 끝나는 순간까지 대기
    def dialog_block_wait(self):
        try:
            WebDriverWait(self.driver, 3).until(EC.visibility_of_element_located((By.XPATH, '//*[@role="dialog"]/')))
            WebDriverWait(self.driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@role="dialog"]/')))
        except TimeoutException:
            pass

    def clean_database(self):
        self.db_engine.execute('DROP TABLE stock_invest_warning')
        self.db_engine.execute('DROP TABLE stock_invest_danger')


if __name__ == "__main__":
    client = KINDCrawler()
    client.craw()
