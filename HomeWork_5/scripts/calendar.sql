/*
    Данный скрипт послужил основой для формирования запроса DataFrame API
    Можно сравнить результаты выполнения скрипта и API
*/

select
    concat_ws('-', year, lpad(date, 2, '0'), lpad(month, 2, '0')) calendar_dt
from (
    select distinct
        year, date, month
    from
        logs_hotel
    order by year::int, month::int, date::int
)
;
