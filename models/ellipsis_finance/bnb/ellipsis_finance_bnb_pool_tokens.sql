{{ config(
    alias = 'pool_tokens',
    partition_by = ['pool'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool', 'token_id']
    )
}}

{%- set call_coin_sources = [
 source('ellipsis_finance_bnb', '2brl_call_coins')
 ,source('ellipsis_finance_bnb', '2pool_call_coins')
 ,source('ellipsis_finance_bnb', '3brl_call_coins')
 ,source('ellipsis_finance_bnb', '3EPS_call_coins')
 ,source('ellipsis_finance_bnb', 'Ankr_BNB_call_coins')
 ,source('ellipsis_finance_bnb', 'ankr_eth_call_coins')
 ,source('ellipsis_finance_bnb', 'ankr_matic_call_coins')
 ,source('ellipsis_finance_bnb', 'apl_BUSD_call_coins')
 ,source('ellipsis_finance_bnb', 'ARTH_usd_call_coins')
 ,source('ellipsis_finance_bnb', 'AUSD_3EPS_call_coins')
 ,source('ellipsis_finance_bnb', 'axelarUSD_call_coins')
 ,source('ellipsis_finance_bnb', 'bnb_bnbl_call_coins')
 ,source('ellipsis_finance_bnb', 'BNBx_BNB_call_coins')
 ,source('ellipsis_finance_bnb', 'cryptopool_BNBx_BNB_call_coins')
 ,source('ellipsis_finance_bnb', 'cryptopool_BUSD_ARTH_bsc_call_coins')
 ,source('ellipsis_finance_bnb', 'crypto_pool_BUSD_BTCB_call_coins')
 ,source('ellipsis_finance_bnb', 'crypto_pool_BUSD_DDD_call_coins')
 ,source('ellipsis_finance_bnb', 'crypto_pool_BUSD_jCHF_call_coins')
 ,source('ellipsis_finance_bnb', 'cryptopool_dEPX_BUSD_call_coins')
 ,source('ellipsis_finance_bnb', 'crypto_pool_EPX_BNB_call_coins')
 ,source('ellipsis_finance_bnb', 'crypto_pool_ETH_BNB_call_coins')
 ,source('ellipsis_finance_bnb', 'crypto_pool_JRT_BNB_call_coins')
 ,source('ellipsis_finance_bnb', 'crypto_pool_VALAS_BNB_call_coins')
 ,source('ellipsis_finance_bnb', 'CZUSD_BUSD_call_coins')
 ,source('ellipsis_finance_bnb', 'CZUSD_val3EPS_call_coins')
 ,source('ellipsis_finance_bnb', 'deBridge_USD_call_coins')
 ,source('ellipsis_finance_bnb', 'DotDot_dEPX_EPX_call_coins')
 ,source('ellipsis_finance_bnb', 'HAY_BUSD_call_coins')
 ,source('ellipsis_finance_bnb', 'Horizon_protocol_zBNB_BNB_call_coins')
 ,source('ellipsis_finance_bnb', 'jBRL_BUSD_call_coins')
 ,source('ellipsis_finance_bnb', 'jNGN_NGNT_call_coins')
 ,source('ellipsis_finance_bnb', 'MAI_val3EPS_call_coins')
 ,source('ellipsis_finance_bnb', 'nBUSD_val3EPS_call_coins')
 ,source('ellipsis_finance_bnb', 'StableSwap_call_coins')
 ,source('ellipsis_finance_bnb', 'USDD_3EPS_call_coins')
 ,source('ellipsis_finance_bnb', 'USDL_val3EPS_call_coins')
 ,source('ellipsis_finance_bnb', 'USDN_val3EPS_call_coins')
 ,source('ellipsis_finance_bnb', 'USDS_val3EPS_call_coins')
 ,source('ellipsis_finance_bnb', 'val3EPS_call_coins')
 ,source('ellipsis_finance_bnb', 'valBTC_renBTC_call_coins')
 ,source('ellipsis_finance_bnb', 'valDAI_val3EPS_call_coins')
 ,source('ellipsis_finance_bnb', 'valTUSD_val3EPS_call_coins')
] -%}

SELECT
    'bnb' as blockchain,
    '1' as version,
    'ellipsis_finance' as project,
    pool,
    token_id,
    token_address
FROM (
    {%- for src in call_coin_sources %}
    SELECT DISTINCT
        contract_address as pool,
        arg0 as token_id,
        output_0 as token_address
    FROM {{ src }}
    WHERE call_success = true
        {%- if is_incremental() %}
        AND call_block_time >= date_trunc("day", now() - interval '1 week')
        {%- endif %}
    {%- if not loop.last %}
    UNION ALL
    {%- endif %}
    {%- endfor %}
)
