-- only compare aum of stablecoins considering price of others assets may change greatly within a short time
with aum as (
  select date,
         usdc_aum,
         usdc_e_aum
  from (select date_trunc('day', minute) as date,
               -- minute,
               row_number() over (partition by date_trunc('day', minute) order by minute desc) as rn,
               usdc_aum,
               usdc_e_aum
        from {{ ref('gmx_avalanche_c_glp_aum') }}
        where date_trunc('day', minute) in (cast('2022-09-15' as date), cast('2022-09-16' as date), cast('2022-10-02' as date),
                       cast('2022-11-23' as date), cast('2022-11-30' as date)))
  where rn = 1
)
   , examples as (
    select *
    from {{ ref('gmx_glp_aum_seed') }}
)
   , matched as (
    select a.date,
           a.usdc_aum                                                               as a_usdc_aum,
           e.usdc_aum                                                               as e_usdc_aum,
           if(abs(a.usdc_aum - e.usdc_aum) / e.usdc_aum < 0.03, true, false)       as usdc_aum_within_range,
           a.usdc_e_aum                                                             as a_usdc_e_aum,
           e.usdc_e_aum                                                             as e_usdc_e_aum,
           if(abs(a.usdc_e_aum - e.usdc_e_aum) / e.usdc_e_aum < 0.03, true, false) as usdc_e_aum_within_range
    from aum as a
    full outer join examples as e
on a.date=e.date
    )
select *
from matched
where not (usdc_aum_within_range and usdc_e_aum_within_range)
