{{ config(
        schema = 'metrics'
        , alias = 'tron_fee_correction'
        , materialized = 'table'
        , file_format = 'delta'
        )
}}

-- api can only return 1000 days a time, this CTE URLs to query in batches of 1000 days
with api_urls as (
  select
    startdate
    -- https://docs.tronscan.org/api-endpoints/statistics#get-energy-consumption-distribution
    ,concat('https://apilist.tronscanapi.com/api/energystatistic?size=1000&type=0',
            '&start_timestamp=',cast(cast(to_unixtime(startdate)*1000 as uint256) as varchar),
            '&end_timestamp=',cast(cast(to_unixtime(startdate + interval '999' day)*1000 as uint256) as varchar)
            ) as api_url
  from unnest(
        sequence(
            cast('2018-10-11' as timestamp) --earliest result in API calls
           ,cast(date_trunc('day', now()) as timestamp)
          ,interval '1000' day)
          ) as foo(startdate)
)
,trx_fee_corrections as (
  SELECT
    -- json_row,
    cast(json_extract_scalar(json_row, '$.day') as timestamp) as day,
    -- cast(json_extract_scalar(json_data, '$.total_energy') as bigint) as total_energy,
    -- cast(json_extract_scalar(json_data, '$.energy') as bigint) as energy,
    -- cast(json_extract_scalar(json_data, '$.contract_supplied') as bigint) as contract_supplied,
    -- cast(json_extract_scalar(json_data, '$.trx') as bigint) as trx,
    -- cast(json_extract_scalar(json_data, '$.energy_ration') as double) as energy_ration,
    -- cast(json_extract_scalar(json_data, '$.contract_supplied_ration') as double) as contract_supplied_ration,
    cast(json_extract_scalar(json_row, '$.trx_ration') as double) as trx_ration
  FROM (
        SELECT cast(json_extract(
                      http_get(api_url),
                      '$.data')
                    as array<json>) as json_data
        FROM api_urls
      ) temp
  CROSS JOIN UNNEST(temp.json_data) as foo(json_row)
)

SELECT * from trx_fee_corrections
union all
-- we hardcord the ~3months where we have no api data for to have no correction.
select * from (
select
  day
  ,1.0 as trx_ration
from unnest(
        sequence(
           cast('2018-06-25' as timestamp) --min(block_date) from tron.transactions
          ,cast('2018-10-10' as timestamp) --day before earliest result in API calls
          ,interval '1' day)
          ) as foo(day)
)
