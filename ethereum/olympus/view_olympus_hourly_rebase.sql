CREATE SCHEMA IF NOT EXISTS olympus;

BEGIN;
DROP materialized VIEW IF EXISTS olympus.olympus_hourly_rebase;
create materialized view olympus.olympus_hourly_rebase as 

with staking_address AS
(
--staking v2
    SELECT
    evt_block_time as Date,
    value as staked_amount
    FROM olympus."OHM_evt_Transfer" -- OHM contract address
    where "from" = '\xFd31c7d00Ca47653c6Ce64Af53c1571f9C36566a'
UNION ALL
    SELECT
    evt_block_time as Date,
    -value as staked_amount
    FROM olympus."OHM_evt_Transfer" -- OHM contract address
    where "to" = '\xFd31c7d00Ca47653c6Ce64Af53c1571f9C36566a'
),

final_staked as
(
    SELECT
    Date as second,
    sum(-sum(staked_amount)) over (order by Date)/1e9 as OHM_staked_amount
    FROM staking_address
    GROUP BY 1
),

staking_tx as (
    select evt_block_time, (value/1e9) as ohm_transferred, evt_tx_hash
    FROM olympus."OHM_evt_Transfer"
    where "from" = '\x383518188c0c6d7730d91b2c03a03c837814a899'
    and "to"  = '\xFd31c7d00Ca47653c6Ce64Af53c1571f9C36566a'
),

rebase as (select evt_block_time, ohm_transferred, ohm_staked_amount, (ohm_transferred/(ohm_staked_amount)) as rebase, evt_tx_hash
from staking_tx
left join final_staked on final_staked."second" = evt_block_time
order by evt_block_time desc)

select evt_block_time as "date",rebase, (1+rebase)^(1095) as apy
from rebase
where evt_block_time > '2021-06-16'


; 

CREATE INDEX IF NOT EXISTS "date" ON olympus.olympus_hourly_rebase ("date", rebase, apy,version);
COMMIT;

INSERT INTO cron.job(schedule, command)
VALUES ('* 59 * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY olympus.olympus_hourly_rebase$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
