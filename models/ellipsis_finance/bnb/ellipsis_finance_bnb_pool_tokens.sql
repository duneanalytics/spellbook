{{config(alias='pool_tokens')}}

2brl as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', '2brl_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

2pool as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', '2pool_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

3brl as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', '3brl_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

3eps as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', '3EPS_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

ankr_bnb as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', 'Ankr_BNB_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

ankr_eth as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', 'ankr_eth_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

ankr_matic as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', 'ankr_matic_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

apl_busd as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', 'apl_BUSD_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

arth_usd as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', 'ARTH_usd_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

ausd_3eps as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', 'AUSD_3EPS_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

axelar_usd as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', 'axelarUSD_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

bnb_bnbl as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', 'bnb_bnbl_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

bnbx_bnb as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', 'BNBx_BNB_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

cryptopool_bnbx_bnb as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', 'cryptopool_BNBx_BNB_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

cryptopool_busd_arth as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', 'cryptopool_BUSD_ARTH_bsc_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

cryptopool_busd_btcb as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', 'crypto_pool_BUSD_BTCB_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

cryptopool_busd_ddd as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', 'crypto_pool_BUSD_DDD_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

cryptopool_busd_jchf as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', 'crypto_pool_BUSD_jCHF_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

cryptopool_depx_busd as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', 'cryptopool_dEPX_BUSD_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

cryptopool_epx_bnb as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', 'crypto_pool_EPX_BNB_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

cryptopool_eth_bnb as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', 'crypto_pool_ETH_BNB_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

cryptopool_jrt_bnb as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', 'crypto_pool_JRT_BNB_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

cryptopool_valas_bnb as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', 'crypto_pool_VALAS_BNB_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

czusd_busd as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', 'CZUSD_BUSD_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

czusd_val3eps as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', 'CZUSD_val3EPS_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

debridge_usd as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', 'deBridge_USD_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

dotdot_depx_epx as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', 'DotDot_dEPX_EPX_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

hay_busd as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', 'HAY_BUSD_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

horizon_zbnb_bnb as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', 'Horizon_protocol_zBNB_BNB_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

jbrl_busd as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', 'jBRL_BUSD_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

jngn_ngnt as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', 'jNGN_NGNT_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

mai_val3eps as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', 'MAI_val3EPS_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

nbusd_val3eps as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', 'nBUSD_val3EPS_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

hay_stableswap as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', 'StableSwap_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

usdd_3eps as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', 'USDD_3EPS_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

usdl_val3eps as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', 'USDL_val3EPS_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

usdn_val3eps as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', 'USDN_val3EPS_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

usds_val3eps as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', 'USDS_val3EPS_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

val3eps as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', 'val3EPS_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

valbtc_renbtc as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', 'valBTC_renBTC_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

valdai_val3eps as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', 'valDAI_val3EPS_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

valtusd_val3eps as (
    SELECT 
        contract_address as pool, 
        arg0 as token_id, 
        output_0 as token_address,
        COUNT(*) as cnt 
    FROM 
    {{ source('ellipsis_finance_bnb', 'valTUSD_val3EPS_call_coins') }}
    WHERE call_success = true 
    GROUP BY 1, 2, 3 
), 

all as (
SELECT * FROM 2brl 

UNION 

SELECT * FROM 2pool 

UNION 

SELECT * FROM 3brl 

UNION 

SELECT * FROM 3eps 

UNION 

SELECT * FROM ankr_bnb

UNION 

SELECT * FROM ankr_eth 

UNION 

SELECT * FROM ankr_matic

UNION 

SELECT * FROM apl_busd

UNION 

SELECT * FROM arth_usd

UNION 

SELECT * FROM ausd_3eps

UNION 

SELECT * FROM axelar_usd

UNION 

SELECT * FROM bnb_bnbl

UNION 

SELECT * FROM bnbx_bnb

UNION 

SELECT * FROM cryptopool_bnbx_bnb

UNION 

SELECT * FROM cryptopool_busd_arth

UNION 

SELECT * FROM cryptopool_busd_btcb

UNION 

SELECT * FROM cryptopool_busd_ddd

UNION 

SELECT * FROM cryptopool_busd_jchf

UNION 

SELECT * FROM cryptopool_depx_busd

UNION 

SELECT * FROM cryptopool_epx_bnb

UNION 

SELECT * FROM cryptopool_eth_bnb

UNION 

SELECT * FROM cryptopool_jrt_bnb

UNION 

SELECT * FROM cryptopool_valas_bnb

UNION 

SELECT * FROM czusd_busd 

UNION 

SELECT * FROM czusd_val3eps

UNION 

SELECT * FROM debridge_usd

UNION 

SELECT * FROM dotdot_depx_epx

UNION 

SELECT * FROM hay_busd

UNION 

SELECT * FROM horizon_zbnb_bnb

UNION 

SELECT * FROM jbrl_busd

UNION 

SELECT * FROM jngn_ngnt

UNION 

SELECT * FROM mai_val3eps

UNION 

SELECT * FROM nbusd_val3eps

UNION 

SELECT * FROM hay_stableswap

UNION

SELECT * FROM usdd_3eps

UNION 

SELECT * FROM usdl_val3eps

UNION 

SELECT * FROM usdn_val3eps

UNION 

SELECT * FROM usds_val3eps

UNION 

SELECT * FROM val3eps

UNION 

SELECT * FROM valbtc_renbtc

UNION 

SELECT * FROM valdai_val3eps

UNION 

SELECT * FROM valtusd_val3eps
) 

SELECT 
    'bnb' as blockchain, 
    '1' as version, 
    'ellipsis_finance' as project, 
    pool, 
    token_id,
    token_address 
FROM 
all 

