CREATE SCHEMA IF NOT EXISTS olympus;

BEGIN;
DROP materialized VIEW IF EXISTS olympus.olympus_hourly_rebase;
create materialized view olympus.olympus_hourly_rebase as 
with time as
(
SELECT
generate_series( '2021-06-16', NOW(), '1 hour'::interval) as date
),

staking_address AS
(
--staking v2
    SELECT
    evt_block_time as date,
    e.value as staked_amount
    FROM erc20."ERC20_evt_Transfer" e
    WHERE "contract_address" = '\x383518188c0c6d7730d91b2c03a03c837814a899' -- OHM contract address
    and e."from" = '\xFd31c7d00Ca47653c6Ce64Af53c1571f9C36566a'
UNION ALL
    SELECT
    evt_block_time as date,
    -e.value as staked_amount
    FROM erc20."ERC20_evt_Transfer" e
    WHERE "contract_address" = '\x383518188c0c6d7730d91b2c03a03c837814a899' -- OHM contract address
    and e."to" = '\xFd31c7d00Ca47653c6Ce64Af53c1571f9C36566a'
),

final_staked as
(
    SELECT
    date_trunc('hour',date) as hour,
    sum(-sum(staked_amount)) over (order by 1)/1e9 as ohm_staked_amount
    FROM 
    staking_address
    group by 1
),
staking_tx as (
    select evt_block_time, value/1e9 as ohm_transferred, evt_tx_hash
    from erc20."ERC20_evt_Transfer" e
    where e."from" = '\x383518188c0c6d7730d91b2c03a03c837814a899'
    and e."to"  = '\xFd31c7d00Ca47653c6Ce64Af53c1571f9C36566a' and value != 0
),

rebase as (
    select evt_block_time, ohm_transferred, ohm_staked_amount, (ohm_transferred/(ohm_staked_amount)) as rebase, evt_tx_hash
    from staking_tx
    left join final_staked on final_staked."hour" = date_trunc('hour',evt_block_time)
    order by evt_block_time desc
) 

,init_data as (
select evt_block_time as rebase_time,rebase, (1+rebase)^(1095) as apy, row_number() over (order by evt_block_time asc) as rebase_id
    from rebase
where evt_block_time > '2021-06-16'
order by 1 asc
)

--select * from init_data limit 100
,holey_data as (
select 
    date_trunc('hour', A."date") as rebase_hour
    ,rebase
    ,apy 
    ,rebase_id
from 
    "time" A
    left join init_data B on date_trunc('hour', A."date") =  date_trunc('hour', B.rebase_time)
Group by 1,2,3,4
order by 1 asc
)
--select * from holey data limit 100

--to make things simple, I want to make sure the table always start with a completely filled row. This allows us to find when that is.
,rebase_start AS (
Select 
    min(rebase_hour) rebase_start
from holey_data
    where rebase_id >0    
)

--select * from rebase_start

--this creates a table with the empty holes and a row number for each one (so we can fill them). I added row number for QA purposes
,holey_numbered_data as (
select 
    row_number() over (order by rebase_hour asc) as id
    ,* 
from holey_data A
    join rebase_start B on 1=1
Where
    A.rebase_hour >= B.rebase_start
)

-- group the rows with null values with the first non-null value above it.
,grouped_table as (
    select
        rebase_hour
        ,rebase 
        ,apy
        ,count(rebase) over (order by rebase_hour) as _rebase
        ,count(apy) over (order by rebase_hour) as _apy
    from holey_numbered_data
    )

--return that first value for every row that shares a grouping
,final_table as (
    Select
        rebase_hour
        ,rebase
        ,_rebase
        ,_apy
        ,first_value(rebase) over (partition by _rebase order by rebase_hour) as new_rebase
        ,first_value(apy) over (partition by _rebase order by rebase_hour) as new_apy
    from grouped_table
)

select
    rebase_hour as timestamp
    ,new_rebase as rebase
    ,new_apy as apy
from final_table
order by 1  
; 

CREATE INDEX IF NOT EXISTS "timestamp" ON olympus.olympus_hourly_rebase ("timestamp", rebase, apy);
COMMIT;

INSERT INTO cron.job(schedule, command)
VALUES ('* 1 * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY olympus.olympus_hourly_rebase$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
