{{ config(
        alias = alias('glp_aum'),
        partition_by = ['block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'minute'],
        post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                    "project",
                                    "gmx",
                                    \'["theachenyj"]\') }}'
        )
}}

/*
Stablecoin holings AUM = poolAmounts * current_price
Directional holdings AUM = (available_assets * current_price) + (longs) + (current_price - average_short_entry_price) * (shorts_opened_notional / current_price)
*/
{% set project_start_date = '2021-12-22 06:07' %}

SELECT -- This query calculates the AUM of each component of GLP
    minute,
    block_date,
    usdc_available_assets * usdc_current_price AS usdc_aum,
    usdc_e_available_assets * usdc_e_current_price AS usdc_e_aum,
    mim_available_assets * mim_current_price as mim_aum,
    (wavax_available_assets * wavax_current_price) + (wavax_longs) + (wavax_current_price - wavax_shorts_entry_price) * COALESCE((wavax_shorts_outstanding_notional / wavax_current_price),0) AS wavax_aum, -- Removes null values derrived from 0 divided 0
    (weth_e_available_assets * weth_e_current_price) + (weth_e_longs) + (weth_e_current_price - weth_e_shorts_entry_price) * COALESCE((weth_e_shorts_outstanding_notional / weth_e_current_price),0) AS weth_e_aum, -- Removes null values derrived from 0 divided 0
    (wbtc_e_available_assets * wbtc_e_current_price) + (wbtc_e_longs) + (wbtc_e_current_price - wbtc_e_shorts_entry_price) * COALESCE((wbtc_e_shorts_outstanding_notional / wbtc_e_current_price),0) AS wbtc_e_aum, -- Removes null values derrived from 0 divided 0
    (btc_b_available_assets * btc_b_current_price) + (btc_b_longs) + (btc_b_current_price - btc_b_shorts_entry_price) * COALESCE((btc_b_shorts_outstanding_notional / btc_b_current_price),0) AS btc_b_aum -- Removes null values derrived from 0 divided 0
FROM {{ref('gmx_avalanche_c_glp_components')}}
{% if is_incremental() %}
WHERE minute >= date_trunc("day", now() - interval '1 day')
{% endif %}
{% if not is_incremental() %}
WHERE minute >= '{{project_start_date}}'
{% endif %}
