{{ config(
        alias = 'glp_exposure',
        materialized = 'view',
        post_hook='{{ expose_spells(\'["arbitrum"]\',
                                    "project",
                                    "gmx",
                                    \'["1chioku"]\') }}'
        )
}}

/*
Exposure = (available_assets * current_price) + ((shorts_opened_notional/average_short_entry_price) * current_price)
*/

SELECT -- This query calculates the underlying directional exposure in GLP, i.e we remove market neutral positions in the GLP AUM (stablecoins and longs)
    minute,
    (wbtc_available_assets * wbtc_current_price) + COALESCE(((wbtc_shorts_outstanding_notional / wbtc_shorts_entry_price) * wbtc_current_price),0) AS wbtc_exposure, -- Removes null values derrived from 0 divided 0
    (uni_available_assets * uni_current_price) + COALESCE(((uni_shorts_outstanding_notional / uni_shorts_entry_price) * uni_current_price),0) AS uni_exposure, -- Removes null values derrived from 0 divided 0
    (link_available_assets * link_current_price) + COALESCE(((link_shorts_outstanding_notional / link_shorts_entry_price) * link_current_price),0) AS link_exposure, -- Removes null values derrived from 0 divided 0
    (weth_available_assets * weth_current_price) + COALESCE(((weth_shorts_outstanding_notional / weth_shorts_entry_price) * weth_current_price),0) AS weth_exposure -- Removes null values derrived from 0 divided 0
FROM {{ref('gmx_arbitrum_glp_components')}}