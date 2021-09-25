CREATE SCHEMA IF NOT EXISTS olympus;

BEGIN;
DROP materialized VIEW IF EXISTS olympus.runway;
create materialized view olympus.runway as 

with staking_address AS
(
    SELECT
    evt_block_time as Date, -value as staked_amount FROM olympus."OHM_evt_Transfer" -- OHM contract address
    where "from" in ('\x0822F3C03dcc24d200AFF33493Dc08d0e1f274A2','\xFd31c7d00Ca47653c6Ce64Af53c1571f9C36566a')
UNION ALL
    SELECT
    evt_block_time as Date, value as staked_amount FROM olympus."OHM_evt_Transfer" -- OHM contract address
    where "to" in ('\x0822F3C03dcc24d200AFF33493Dc08d0e1f274A2','\xFd31c7d00Ca47653c6Ce64Af53c1571f9C36566a')
),

final_staked as
(
    SELECT
    date_trunc('day',date) as date,
    sum(sum(staked_amount)) over (order by date_trunc('day',date))/1e9 as OHM_staked
    FROM staking_address
    GROUP BY 1
)

select final_staked."date", treasury_rfv, 
(ln(treasury_rfv/OHM_staked)/ln(1+0.00485037114805))/3 as "20k_apy_runway",
(ln(treasury_rfv/OHM_staked)/ln(1+0.00421449096620))/3 as "10k_apy_runway",
(ln(treasury_rfv/OHM_staked)/ln(1+0.00210502990765))/3 as "1k_apy_runway",
(ln(treasury_rfv/OHM_staked)/ln(1+0.00147088700743))/3 as "500_apy_runway"
from final_staked 
left join olympus.treasury_rfv on olympus.view_treasury_rfv."date" = final_staked."date"


; 

CREATE INDEX IF NOT EXISTS "date" ON olympus.runway ("date", treasury_rfv,"20k_apy_runway", "10k_apy_runway","1k_apy_runway", "500_apy_runway");
COMMIT;

INSERT INTO cron.job(schedule, command)
VALUES ('* 59 * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY olympus.runway$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;