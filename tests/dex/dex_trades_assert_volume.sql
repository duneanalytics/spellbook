with volume as (
    SELECT
        project
        , blockchain
        , sum(amount_usd) volume
    from {{ ref('dex_trades') }}
    where block_time >= now() - interval '7' day
    group by 1,2
)

-- Game on to dethone.
, cur_top_project as (
select project,
       blockchain,
       volume as expected_top_volume
from volume
where project = 'uniswap'
and blockchain = 'ethereum')

select
   v.project,
   v.blockchain,
   v.volume,
   ctp.expected_top_volume,
 ctp.expected_top_volume - v.volume as delta_with_current_top_project
from volume v
cross join cur_top_project ctp
where  round(ctp.expected_top_volume - v.volume, 4) < 0