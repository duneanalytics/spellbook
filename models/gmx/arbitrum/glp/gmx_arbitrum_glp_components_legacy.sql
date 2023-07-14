{{ config(
	tags=['legacy'],
	
        alias = alias('glp_components', legacy_model=True),
        partition_by = ['block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'minute'],
        post_hook='{{ expose_spells(\'["arbitrum"]\',
                                        "project",
                                        "gmx",
                                        \'["1chioku"]\') }}'
        )
}}

{% set project_start_date = '2021-08-31 08:13' %}

SELECT
    minute,
    block_date,
    
    frax_available_assets, -- FRAX Pool Amounts - Decimal Places 18
    frax_current_price, -- Current Price as MAX(getMaxPrice,getMinPrice) - Decimal Places 12
    
    usdt_available_assets, -- USDT Pool Amounts - Decimal Places 6
    usdt_current_price, -- Current Price as MAX(getMaxPrice,getMinPrice) - Decimal Places 12
    
    wbtc_available_assets, -- WBTC Available Assets - Decimal Places 8
    wbtc_longs, --USDG Decimal Places 30
    wbtc_current_price, -- Current Price as MAX(getMaxPrice,getMinPrice) - Decimal Places 12
    wbtc_shorts_entry_price, -- Average Short entry price - Decimal Places 30
    wbtc_shorts_outstanding_notional, -- Shorts Opened Notional - Decimal Places 30
    
    usdc_available_assets, -- USDC Pool Amounts - Decimal Places 6
    usdc_current_price, -- Current Price as MAX(getMaxPrice,getMinPrice) - Decimal Places 12
    
    uni_available_assets, -- UNI Available Assets - Decimal Places 8
    uni_longs, --USDG Decimal Places 30
    uni_current_price, -- Current Price as MAX(getMaxPrice,getMinPrice) - Decimal Places 12
    uni_shorts_entry_price, -- Average Short entry price - Decimal Places 30
    uni_shorts_outstanding_notional, -- Shorts Opened Notional - Decimal Places 30
    
    link_available_assets, -- UNI Available Assets - Decimal Places 8
    link_longs, --USDG Decimal Places 30
    link_current_price, -- Current Price as MAX(getMaxPrice,getMinPrice) - Decimal Places 12
    link_shorts_entry_price, -- Average Short entry price - Decimal Places 30
    link_shorts_outstanding_notional, -- Shorts Opened Notional - Decimal Places 30
    
    weth_available_assets, -- WETH Available Assets - Decimal Places 18
    weth_longs, --USDG Decimal Places 30
    weth_current_price, -- Current Price as MAX(getMaxPrice,getMinPrice) - Decimal Places 12
    weth_shorts_entry_price, -- Average Short entry price - Decimal Places 30
    weth_shorts_outstanding_notional, -- Shorts Opened Notional - Decimal Places 30
    
    dai_available_assets, -- DAI Pool Amounts - Decimal Places 18
    dai_current_price
FROM {{ref('gmx_arbitrum_glp_components_base_legacy')}}
{% if is_incremental() %}
WHERE minute >= date_trunc("day", now() - interval '1 week')
{% endif %}
{% if not is_incremental() %}
WHERE minute >= '{{project_start_date}}'
{% endif %}