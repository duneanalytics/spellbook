CREATE SCHEMA IF NOT EXISTS olympus;

BEGIN;
DROP materialized VIEW IF EXISTS olympus.olympus_hourly_rebase;
create materialized view olympus.olympus_hourly_rebase as 

with time as
(
SELECT
generate_series('2021-06-16', NOW(), '1 second'::interval) as date
),

staking_address AS
(
--staking v2
    SELECT
    date_trunc('second', block_time) as date,
    COALESCE(sum(e.value/1e9), 0) as staked_amount
    FROM erc20."ERC20_evt_Transfer" e
    LEFT JOIN ethereum."transactions" tx ON evt_tx_hash = tx.hash
    WHERE "contract_address" = '\x383518188c0c6d7730d91b2c03a03c837814a899' -- OHM contract address
    and e."from" = '\xFd31c7d00Ca47653c6Ce64Af53c1571f9C36566a'
    GROUP BY 1
UNION ALL
    SELECT
    date_trunc('second', block_time) as date,
    COALESCE(sum(-e.value/1e9), 0) as staked_amount
    FROM erc20."ERC20_evt_Transfer" e
    LEFT JOIN ethereum."transactions" tx ON evt_tx_hash = tx.hash
    WHERE "contract_address" = '\x383518188c0c6d7730d91b2c03a03c837814a899' -- OHM contract address
    and e."to" = '\xFd31c7d00Ca47653c6Ce64Af53c1571f9C36566a'
    GROUP BY 1
),

final_staked as
(
SELECT
Date as second,
sum(-sum(staked_amount)) over (order by date) as OHM_staked_amount
FROM 
(
SELECT Date, staking_address.staked_amount as staked_amount FROM staking_address UNION ALL
SELECT Date, 0 as staked_amount FROM time
) t
GROUP BY 1
ORDER BY Date DESC
),

staking_tx as (
select evt_block_time, (value/1e9) as ohm_transferred, evt_tx_hash
from erc20."ERC20_evt_Transfer" e
where e."from" = '\x383518188c0c6d7730d91b2c03a03c837814a899'
and e."to"  = '\xFd31c7d00Ca47653c6Ce64Af53c1571f9C36566a' and value != 0
),

rebase as (select evt_block_time as "timestamp", ohm_transferred, ohm_staked_amount, (ohm_transferred/(ohm_staked_amount)) as rebase, evt_tx_hash
from staking_tx
left join final_staked on final_staked."second" = evt_block_time
order by "timestamp" desc)

select "timestamp",rebase, (1+rebase)^(1095) as apy
from rebase
where "timestamp" > '2021-06-16'


; 

CREATE INDEX IF NOT EXISTS "timestamp" ON olympus.olympus_hourly_rebase ("timestamp", rebase, apy);
COMMIT;

INSERT INTO cron.job(schedule, command)
VALUES ('* 1 * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY olympus.olympus_hourly_rebase$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
