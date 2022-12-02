with aum as (
    select date_trunc('day', minute) as date,
           *
    from {{ ref('gmx_avalanche_c_glp_aum') }}
    where minute in
          ('2022-09-15 23:59',
           '2022-09-16 23:59',
           '2022-10-02 23:59',
           '2022-11-23 23:59',
           '2022-11-30 23:59')
)
   , examples as (
    select *
    from {{ ref('gmx_glp_aum_seed') }}
)
   , matched as (
    select a.date,
           a.usdc_aum                                                               as a_usdc_aum,
           e.usdc_aum                                                               as e_usdc_aum,
           if(abs(a.usdc_aum - e.usdc_aum) / e.usdc_aum < 0.001, true, false)       as usdc_aum_within_range,
           a.usdc_e_aum                                                             as a_usdc_e_aum,
           e.usdc_e_aum                                                             as e_usdc_e_aum,
           if(abs(a.usdc_e_aum - e.usdc_e_aum) / e.usdc_e_aum < 0.001, true, false) as usdc_e_aum_within_range,
           a.wavax_aum                                                              as a_wavax_aum,
           e.wavax_aum                                                              as e_wavax_aum,
           if(abs(a.wavax_aum - e.wavax_aum) / e.wavax_aum < 0.001, true, false)    as wavax_aum_within_range,
           a.weth_e_aum                                                             as a_weth_e_aum,
           e.weth_e_aum                                                             as e_weth_e_aum,
           if(abs(a.weth_e_aum - e.weth_e_aum) / e.weth_e_aum < 0.001, true, false) as weth_e_aum_within_range,
           a.wbtc_e_aum                                                             as a_wbtc_e_aum,
           e.wbtc_e_aum                                                             as e_wbtc_e_aum,
           if(abs(a.wbtc_e_aum - e.wbtc_e_aum) / e.wbtc_e_aum < 0.001, true, false) as wbtc_e_aum_within_range,
           a.btc_b_aum                                                              as a_btc_b_aum,
           e.btc_b_aum                                                              as e_btc_b_aum,
           if(abs(a.btc_b_aum - e.btc_b_aum) / e.btc_b_aum < 0.001, true, false)    as btc_b_aum_within_range
    from aum as a
    full outer join examples as e
on a.date=e.date
    )
select *
from matched
where not (usdc_aum_within_range and usdc_e_aum_within_range and wavax_aum_within_range and weth_e_aum_within_range and
           wbtc_e_aum_within_range and btc_b_aum_within_range)
