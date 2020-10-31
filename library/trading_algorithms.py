import numpy as np
import pandas as pd


def BBands(df_close, w=20, k=2):
    """
        w: 이동평균선 기간 값 (20)
        k: 승수 (2)
        std 함수는 '표준편차를 구하기 위한' numpy 패키지에 포함되어 있는 내장 함수입니다.
        DATAFRAME[-1:] 마지막 row
        DATAFRAME[:-1] index 0부터 마지막 row 제외한 rows
        DATAFRAME[-20:] 뒤에서부터 20개의 rows
        DATAFRAME[:20] index 0부터 20개의 rows
        DATAFRAME[20:] index 20부터 끝까지 rows
    """
    # 고가, 저가, 종가의 평균을 이용하는 경우 정수로 변환이 필요
    df_close = df_close.astype(int)
    # 표준편차
    std = df_close[:w].std()[0]
    # mean() 함수는 '평균을 구하기 위한' numpy 패키지에 포함되어 있는 내장 함수입니다.
    # 20일 이평선이자 볼린저밴드 중앙선
    mbb = df_close[:w].mean()[0]
    # 종가
    close = df_close[0][0]

    '''
        std (표준편차 값)과 mbb(중앙선)을 이용하여 볼린저밴드
        1. ubb (상한선)
        2. lbb (하한선)
        3. perb (%b: 볼린저밴드에서의 종가 위치)
        4. bw (밴드폭)
        4개의 값을 구하는 수식을 01)볼린저밴드 개념에 안내되어 있는 [볼린저 밴드 계산방법]을 참고하여 구현해주세요.
        변수 이름은 ubb, lbb, perb, bw로 통일하여 주시기 바랍니다.
    '''

    ### blank ###########################
    ubb = mbb + std*2
    lbb = mbb - std*2
    #####################################

    if ubb > lbb:
        ### blank ###########################
        perb = (close - lbb) / (ubb - lbb)
        bw = (ubb - lbb) / mbb
        #####################################
        return mbb, ubb, lbb, perb, bw
    else:
        return False
