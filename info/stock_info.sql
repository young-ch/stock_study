SELECT YES_DAILY.* FROM `20201028` YES_DAILY ,naver_min_crawl NAVER, stock_info Stock 
                  WHERE YES_DAILY.code = NAVER.code
                  AND NAVER.code = Stock.code
                  AND NAVER.date = 202010290910
                  AND NAVER.close * NAVER.volume > 100000000
                  AND (NAVER.close-YES_DAILY.close)/YES_DAILY.close*100 < 10 
                  AND YES_DAILY.clo5_diff_rate >= 5 
                  AND YES_DAILY.clo5_diff_rate < 8 
                  AND YES_DAILY.clo5 > YES_DAILY.clo10 
                  AND YES_DAILY.clo20 < YES_DAILY.close 
                  AND Stock.thema_code IS NOT NULL 
                  AND Stock.thema_code = 281
                  AND Stock.thema_code = 550
                  AND Stock.thema_code = 140
                  AND (exists (SELECT null FROM stock_kospi KOSPI WHERE YES_DAILY.code=KOSPI.code) 
                  OR exists (SELECT null FROM stock_kosdaq KOSDAQ WHERE YES_DAILY.code=KOSDAQ.code)) 
                  ORDER BY (NAVER.close-YES_DAILY.close)/YES_DAILY.close*100 DESC 
                  LIMIT 5
                  ;

SELECT * FROM `20201028` where `20201028`.code_name = '';

select * from stock_info where code = 051910;


select thema_code from stock_info where thema_code = 141 and thema_code = 550 group by thema_code;
