import os
import logging
import pathlib
from logging.handlers import TimedRotatingFileHandler

# 목적
# 콜렉터, 시뮬레이터, 봇 모두 logging_pack.py를 import 하고있다.
# jackbot.log 라는 이름으로 로그파일이 만들어진다.

# log파일 위치와 로그 이름을 설정한다. (촬영 후 아래 수정 하였습니다.)
file_path = pathlib.Path(__file__).parent.parent.absolute() / 'log' / 'jackbot.log'

os.makedirs(file_path.parents[0], exist_ok=True)  # 로그 폴더가 존재하는지 확인 후 없으면 생성

# 로그 파일 더블클릭 -> 연결 프로그램 -> 메모장

# logger instance 생성
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# handler 생성( stream, file)
stream_handler = logging.StreamHandler()

file_handler = TimedRotatingFileHandler(file_path, when="midnight")

# formmater 생성
formatter = logging.Formatter('[%(levelname)s|%(filename)s:%(lineno)s] %(asctime)s > %(message)s')
stream_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)
file_handler.suffix = "%Y%m%d"

# logger instance에 handler 설정
logger.addHandler(stream_handler)
logger.addHandler(file_handler)


logger.debug('debug 모드!')
logger.info('info 모드!')
logger.warning('warning 모드!')
logger.error('error 모드!')
logger.critical('critical 모드!')