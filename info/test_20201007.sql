
select count(*) from all_item_db where sell_date = "";


select * from all_item_db where code_name = "고려아연"
select * from all_item_db where code_name = "SK이노베이션"
select * from all_item_db where code_name = "넷마블"
select * from all_item_db where code_name = "HDC현대산업개발"


;

                    SELECT * FROM `20190403` a  
                    WHERE NOT exists (SELECT null FROM stock_konex b WHERE a.code=b.code) 
                    AND close < 100000 ;
                    
                    
                    
                    
                    SELECT ALLDB.code, ALLDB.code_name , ALLDB.rate, ALLDB.present_price, ALLDB.valuation_profit 
                 FROM all_item_db ALLDB, daily_buy_list.20201005 BEFORE_DAY
                   WHERE ALLDB.code = BEFORE_DAY.code 
                   AND ALLDB.sell_date = 0 
                   AND (ALLDB.present_price - BEFORE_DAY.close) / BEFORE_DAY.close * 100 < -3
                   
                   OR ALLDB.sell_date = 0 and ALLDB.rate <= -15
                   ;
                   
                    SELECT count(*) 
                 FROM all_item_db ALLDB, daily_buy_list.20201005 BEFORE_DAY
                   WHERE ALLDB.code = BEFORE_DAY.code 
                   AND ALLDB.sell_date = 0 
                   AND (ALLDB.present_price - BEFORE_DAY.close) / BEFORE_DAY.close * 100 < -1
                   
                   OR ALLDB.sell_date = 0 and ALLDB.rate <= -7;
                   
                   SELECT ALLDB.buy_date , ALLDB.sell_date ,ALLDB.code, ALLDB.code_name  , ALLDB.present_price, BEFORE_DAY.close
                 FROM all_item_db ALLDB, daily_buy_list.20201005 BEFORE_DAY
                 WHERE ALLDB.code = BEFORE_DAY.code
                 and BEFORE_DAY.code_name = "YG PLUS"
                   ;
                   
                   
                   select * from daily_buy_list.20201005 where code_name = "YG PLUS";
                   
                   SELECT *
                 FROM all_item_db ALLDB, daily_buy_list.`20201005` BEFORE_DAY
                   