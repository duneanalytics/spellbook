{{ config(
        alias = 'glp_aum',
        materialized = 'view',
        post_hook='{{ expose_spells(\'["arbitrum"]\',
                                    "project",
                                    "gmx",
                                    \'["1chioku"]\') }}'
        )
}}

/*
Stablecoin holings AUM = poolAmounts * current_price
Directional holdings Long Exposure = (available_assets * current_price) + COALESCE(((shorts_outstanding_notional / shorts_entry_price) * current_price),0)
Directional holdings Neutral Exposure = (longs) + ((current_price - shorts_entry_price) * COALESCE((shorts_outstanding_notional / current_price),0)) - COALESCE(((shorts_outstanding_notional / shorts_entry_price) * current_price),0)
*/

SELECT -- This query calculates the AUM of each component of GLP
    minute,
    
    frax_available_assets * frax_current_price AS frax_aum,
    
    usdt_available_assets * usdt_current_price AS usdt_aum,

    (wbtc_available_assets * wbtc_current_price) + COALESCE(((wbtc_shorts_outstanding_notional / wbtc_shorts_entry_price) * wbtc_current_price),0) AS wbtc_long_exposure_aum,
    (wbtc_longs) + ((wbtc_current_price - wbtc_shorts_entry_price) * COALESCE((wbtc_shorts_outstanding_notional / wbtc_current_price),0)) - COALESCE(((wbtc_shorts_outstanding_notional / wbtc_shorts_entry_price) * wbtc_current_price),0) AS wbtc_neutral_exposure_aum,

    usdc_available_assets * usdc_current_price AS usdc_aum,

    (uni_available_assets * uni_current_price) + COALESCE(((uni_shorts_outstanding_notional / uni_shorts_entry_price) * uni_current_price),0) AS uni_long_exposure_aum,
    (uni_longs) + ((uni_current_price - uni_shorts_entry_price) * COALESCE((uni_shorts_outstanding_notional / uni_current_price),0)) - COALESCE(((uni_shorts_outstanding_notional / uni_shorts_entry_price) * uni_current_price),0) AS uni_neutral_exposure_aum,

    (link_available_assets * link_current_price) + COALESCE(((link_shorts_outstanding_notional / link_shorts_entry_price) * link_current_price),0) AS link_long_exposure_aum,
    (link_longs) + ((link_current_price - link_shorts_entry_price) * COALESCE((link_shorts_outstanding_notional / link_current_price),0)) - COALESCE(((link_shorts_outstanding_notional / link_shorts_entry_price) * link_current_price),0) AS link_neutral_exposure_aum,
    
    (weth_available_assets * weth_current_price) + COALESCE(((weth_shorts_outstanding_notional / weth_shorts_entry_price) * weth_current_price),0) AS weth_long_exposure_aum,
    (weth_longs) + ((weth_current_price - weth_shorts_entry_price) * COALESCE((weth_shorts_outstanding_notional / weth_current_price),0)) - COALESCE(((weth_shorts_outstanding_notional / weth_shorts_entry_price) * weth_current_price),0) AS weth_neutral_exposure_aum,
    
    dai_available_assets * dai_current_price AS dai_aum
FROM {{ref('gmx_arbitrum_glp_components')}}