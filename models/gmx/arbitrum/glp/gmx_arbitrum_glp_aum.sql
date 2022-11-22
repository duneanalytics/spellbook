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
Directional holdings AUM = (available_assets * current_price) + (longs) + (current_price - average_short_entry_price) * (shorts_opened_notional / current_price)
*/

SELECT -- This query calculates the AUM of each component of GLP
    minute,
    frax_available_assets * frax_current_price AS frax_aum,
    usdt_available_assets * usdt_current_price AS usdt_aum,
    (wbtc_available_assets * wbtc_current_price) + (wbtc_longs) + (wbtc_current_price - wbtc_shorts_entry_price) * COALESCE((wbtc_shorts_outstanding_notional / wbtc_current_price),0) AS wbtc_aum, -- Removes null values derrived from 0 divided 0
    usdc_available_assets * usdc_current_price AS usdc_aum,
    (uni_available_assets * uni_current_price) + (uni_longs) + (uni_current_price - uni_shorts_entry_price) * COALESCE((uni_shorts_outstanding_notional / uni_current_price),0) AS uni_aum, -- Removes null values derrived from 0 divided 0
    (link_available_assets * link_current_price) + (link_longs) + (link_current_price - link_shorts_entry_price) * COALESCE((link_shorts_outstanding_notional / link_current_price),0) AS link_aum, -- Removes null values derrived from 0 divided 0
    (weth_available_assets * weth_current_price) + (weth_longs) + (weth_current_price - weth_shorts_entry_price) * COALESCE((weth_shorts_outstanding_notional / weth_current_price),0) AS weth_aum, -- Removes null values derrived from 0 divided 0
    dai_available_assets * dai_current_price AS dai_aum
FROM {{ref('gmx_arbitrum_glp_components')}}