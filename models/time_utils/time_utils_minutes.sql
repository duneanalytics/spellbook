{{ config( alias='minutes', tags=['static'], materialized = 'table')}}

select explode(sequence(
          TIMESTAMP('2000-01-01')
       ,  TIMESTAMP('2100-01-01')
        ,interval 1 minute )) as minute
order by minute asc