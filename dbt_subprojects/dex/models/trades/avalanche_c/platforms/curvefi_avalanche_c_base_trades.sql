{{
    config(
        schema = 'curvefi_avalanche_c',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set wavax_avalanche_c_token = '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7' %}
{% set ust_wormhole_avalanche_c_token = '0xb599c3590f42f8f995ecfa0f85d2980b76862fc1' %}
{% set usdc_avalanche_c_token = '0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e' %}
{% set usdt_avalanche_c_token = '0x9702230a8ea53601f5cd2dc00fdbc13d4df4a8c7' %}
{% set mim_avalanche_c_token = '0x130966628846bfd36ff31a822705796e8cb8c18d' %}
{% set frax_avalanche_c_token = '0xd24c2ad096400b6fbcd2ad8b24e7acbc21a1da64' %}
{% set renBTC_avalanche_c_token = '0xdbf31df14b66535af65aac99c32e9ea844e14501' %}
{% set mai_avalanche_c_token = '0x5c49b268c9841aff1cc3b0a418ff5c3442ee3f3b' %}
{% set fusd_avalanche_c_token = '0xc6636e205460be6073b65094422910161d767ef2' %}
{% set usds_avalanche_c_token = '0xab05b04743e0aeaf9d2ca81e5d3b8385e4bf961e' %}
{% set usdl_avalanche_c_token = '0x40b393cecf8f7d7fc79b83e8fa40e850511817f6' %}
{% set yusd_avalanche_c_token = '0x111111111111ed1d73f860f57b2798b683f2d325' %}
{% set nxusd_avalanche_c_token = '0xf14f4ce569cb3679e99d5059909e23b07bd2f387' %}
{% set moremoney_avalanche_c_token = '0x0f577433bf59560ef2a79c124e9ff99fca258948' %}
{% set agEUR_avalanche_c_token = '0x6fefd97f328342a8a840546a55fdcfee7542f9a8' %}
{% set jEUR_avalanche_c_token = '0x9fb1d52596c44603198fb0aee434fac3a679f702' %}
{% set eEUR_avalanche_c_token = '0xe1d70994be12b73e76889412b284a8f19b0de56d' %}
{% set arUSD_avalanche_c_token = '0x025ab35ff6abcca56d57475249baaeae08419039' %}
{% set axlUSDC_avalanche_c_token = '0xfab550568c688d5d8a52c7d794cb93edc26ec0ec' %}
{% set dd_avalanche_c_token = '0x210c2e177b34145c64c4eaccd17aca07d2b86164' %}
{% set debridge_usdc_avalanche_c_token = '0x28690ec942671ac8d9bc442b667ec338ede6dfd3' %}
{% set defrost_h20_avalanche_c_token = '0x026187bdbc6b751003517bcb30ac7817d5b766f8' %}
{% set ankr_aAVAXb_avalanche_c_token = '0x6c6f910a79639dcc94b4feef59ff507c2e843929' %}
{% set usdt_e_avalanche_c_token = '0xc7198437980c041c805a1edcba50c1ce5db95118' %}
{% set usdc_e_avalanche_c_token = '0xa7d7079b0fead91f3e65f86e8915cb59c1a4c664' %}
{% set dai_e_avalanche_c_token = '0xd586e7f844cea2f87f50152665bcbc2c279d8d70' %}
{% set wbtc_e_avalanche_c_token = '0x50b7545627a5162f82a992c33b87adc75187b218' %}
{% set aave_dai_avalanche_c_token = '0x47afa96cdc9fab46904a55a6ad4bf6660b53c38a' %}
{% set aave_usdc_avalanche_c_token = '0x46a51127c3ce23fb7ab1de06226147f446e4a857' %}
{% set aave_usdt_avalanche_c_token = '0x532e6537fea298397212f09a61e03311686f548e' %}
{% set av3CRV_avalanche_c_token = '0x1337bedc9d22ecbe766df105c9623922a27963ec' %}
{% set av3CRV_guage_avalanche_c_token = '0x5b5cfe992adac0c9d48e05854b2d91c73a003858' %}
{% set aave_wbtc_avalanche_c_token = '0x686bef2417b6dc32c50a3cbfbcc3bb60e1e9a15d' %}
{% set aave_weth_avalanche_c_token = '0x53f7c5869a859f0aec3d334ee8b4cf01e3492f21' %}
{% set blizz_dai_avalanche_c_token = '0x6807ed4369d9399847f306d7d835538915fa749d' %}
{% set blizz_usdc_avalanche_c_token = '0xc25ff1af397b76252d6975b4d7649b35c0e60f69' %}
{% set blizz_usdt_avalanche_c_token = '0x18cb11c9f2b6f45a7ac0a95efd322ed4cf9eeebf' %}


WITH dexs AS (

    -- 3pool tokenexchange 
    SELECT
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        CAST(NULL as varchar) as version,
        buyer AS taker,
        CAST(NULL as varbinary) as maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        CASE
            WHEN bought_id = INT256 '0' THEN {{ust_wormhole_avalanche_c_token}}
            WHEN bought_id = INT256 '1' THEN {{usdc_avalanche_c_token}}
            WHEN bought_id = INT256 '2' THEN {{usdt_avalanche_c_token}}
        END as token_bought_address,
        CASE
            WHEN sold_id = INT256 '0' THEN {{ust_wormhole_avalanche_c_token}}
            WHEN sold_id = INT256 '1' THEN {{usdc_avalanche_c_token}}
            WHEN sold_id = INT256 '2' THEN {{usdt_avalanche_c_token}}
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('curvefi_avalanche_c', '3pool_evt_TokenExchange') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL 

    -- 3poolV2 tokenexchange 
    SELECT
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        CAST(NULL as varchar) as version,
        buyer AS taker,
        CAST(NULL as varbinary) as maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        CASE
            WHEN bought_id = INT256 '0' THEN {{mim_avalanche_c_token}}
            WHEN bought_id = INT256 '1' THEN {{usdt_e_avalanche_c_token}}
            WHEN bought_id = INT256 '2' THEN {{usdc_e_avalanche_c_token}}
        END as token_bought_address,
        CASE
            WHEN sold_id = INT256 '0' THEN {{mim_avalanche_c_token}}
            WHEN sold_id = INT256 '1' THEN {{usdt_e_avalanche_c_token}}
            WHEN sold_id = INT256 '2' THEN {{usdc_e_avalanche_c_token}}
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('curvefi_avalanche_c', '3poolV2_evt_TokenExchange') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL 

    -- Aavepool tokenexchange 
    SELECT
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        CAST(NULL as varchar) as version,
        buyer AS taker,
        CAST(NULL as varbinary) as maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        CASE
            WHEN bought_id = INT256 '0' THEN {{aave_dai_avalanche_c_token}}
            WHEN bought_id = INT256 '1' THEN {{aave_usdc_avalanche_c_token}}
            WHEN bought_id = INT256 '2' THEN {{aave_usdt_avalanche_c_token}}
        END as token_bought_address,
        CASE
            WHEN sold_id = INT256 '0' THEN {{aave_dai_avalanche_c_token}}
            WHEN sold_id = INT256 '1' THEN {{aave_usdc_avalanche_c_token}}
            WHEN sold_id = INT256 '2' THEN {{aave_usdt_avalanche_c_token}}
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('curvefi_avalanche_c', 'AavePool_evt_TokenExchange') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL 

    -- Aavepool tokenexchange underlying 
    SELECT
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        CAST(NULL as varchar) as version,
        buyer AS taker,
        CAST(NULL as varbinary) as maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        CASE
            WHEN bought_id = INT256 '0' THEN {{dai_e_avalanche_c_token}}
            WHEN bought_id = INT256 '1' THEN {{usdc_e_avalanche_c_token}}
            WHEN bought_id = INT256 '2' THEN {{usdt_e_avalanche_c_token}}
        END as token_bought_address,
        CASE
            WHEN sold_id = INT256 '0' THEN {{dai_e_avalanche_c_token}}
            WHEN sold_id = INT256 '1' THEN {{usdc_e_avalanche_c_token}}
            WHEN sold_id = INT256 '2' THEN {{usdt_e_avalanche_c_token}}
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('curvefi_avalanche_c', 'AavePool_evt_TokenExchangeUnderlying') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL 

    -- Aavev3 tokenexchange underlying 
    SELECT
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        CAST(NULL as varchar) as version,
        buyer AS taker,
        CAST(NULL as varbinary) as maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        CASE
            WHEN bought_id = INT256 '0' THEN {{dai_e_avalanche_c_token}}
            WHEN bought_id = INT256 '1' THEN {{usdc_avalanche_c_token}}
            WHEN bought_id = INT256 '2' THEN {{usdt_avalanche_c_token}}
        END as token_bought_address,
        CASE
            WHEN sold_id = INT256 '0' THEN {{dai_e_avalanche_c_token}}
            WHEN sold_id = INT256 '1' THEN {{usdc_avalanche_c_token}}
            WHEN sold_id = INT256 '2' THEN {{usdt_avalanche_c_token}}
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('curvefi_avalanche_c', 'aave_v3_evt_TokenExchangeUnderlying') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL 

    -- agEURjEUR tokenexchange
    SELECT
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        CAST(NULL as varchar) as version,
        buyer AS taker,
        CAST(NULL as varbinary) as maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        CASE
            WHEN bought_id = INT256 '0' THEN {{agEUR_avalanche_c_token}}
            WHEN bought_id = INT256 '1' THEN {{jEUR_avalanche_c_token}}
        END as token_bought_address,
        CASE
            WHEN sold_id = INT256 '0' THEN {{agEUR_avalanche_c_token}}
            WHEN sold_id = INT256 '1' THEN {{jEUR_avalanche_c_token}}
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('curvefi_avalanche_c', 'agEURjEUR_evt_TokenExchange') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL  

    -- arUSD tokenexchange
    SELECT
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        CAST(NULL as varchar) as version,
        buyer AS taker,
        CAST(NULL as varbinary) as maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        CASE
            WHEN bought_id = INT256 '0' THEN {{arUSD_avalanche_c_token}}
            WHEN bought_id = INT256 '1' THEN {{usdc_avalanche_c_token}}
            WHEN bought_id = INT256 '2' THEN {{usdt_avalanche_c_token}}
        END as token_bought_address,
        CASE
            WHEN sold_id = INT256 '0' THEN {{arUSD_avalanche_c_token}}
            WHEN sold_id = INT256 '1' THEN {{usdc_avalanche_c_token}}
            WHEN sold_id = INT256 '2' THEN {{usdt_avalanche_c_token}}
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('curvefi_avalanche_c', 'arUSD_evt_TokenExchange') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL 

    -- ATricryptopool tokenexchange
    SELECT
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        CAST(NULL as varchar) as version,
        buyer AS taker,
        CAST(NULL as varbinary) as maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        CASE
            WHEN bought_id = UINT256 '0' THEN {{av3CRV_avalanche_c_token}}
            WHEN bought_id = UINT256 '1' THEN {{aave_wbtc_avalanche_c_token}}
            WHEN bought_id = UINT256 '2' THEN {{aave_weth_avalanche_c_token}}
        END as token_bought_address,
        CASE
            WHEN sold_id = UINT256 '0' THEN {{av3CRV_avalanche_c_token}}
            WHEN sold_id = UINT256 '1' THEN {{aave_wbtc_avalanche_c_token}}
            WHEN sold_id = UINT256 '2' THEN {{aave_weth_avalanche_c_token}}
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('curvefi_avalanche_c', 'ATriCryptoPool_evt_TokenExchange') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL 

    -- avax3pool tokenexchange 
    SELECT
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        CAST(NULL as varchar) as version,
        buyer AS taker,
        CAST(NULL as varbinary) as maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        CASE
            WHEN bought_id = INT256 '0' THEN {{dai_e_avalanche_c_token}}
            WHEN bought_id = INT256 '1' THEN {{usdc_avalanche_c_token}}
            WHEN bought_id = INT256 '2' THEN {{usdt_avalanche_c_token}}
        END as token_bought_address,
        CASE
            WHEN sold_id = INT256 '0' THEN {{dai_e_avalanche_c_token}}
            WHEN sold_id = INT256 '1' THEN {{usdc_avalanche_c_token}}
            WHEN sold_id = INT256 '2' THEN {{usdt_avalanche_c_token}}
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('curvefi_avalanche_c', 'avax3pool_evt_TokenExchange') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL 

    -- axlUSDCUSDC tokenexchange 
    SELECT
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        CAST(NULL as varchar) as version,
        buyer AS taker,
        CAST(NULL as varbinary) as maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        CASE
            WHEN bought_id = INT256 '0' THEN {{axlUSDC_avalanche_c_token}}
            WHEN bought_id = INT256 '1' THEN {{usdc_avalanche_c_token}}
        END as token_bought_address,
        CASE
            WHEN sold_id = INT256 '0' THEN {{axlUSDC_avalanche_c_token}}
            WHEN sold_id = INT256 '1' THEN {{usdc_avalanche_c_token}}
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('curvefi_avalanche_c', 'axlUSDCUSDC_evt_TokenExchange') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL 

    -- axlUSDCUSDC_e tokenexchange 
    SELECT
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        CAST(NULL as varchar) as version,
        buyer AS taker,
        CAST(NULL as varbinary) as maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        CASE
            WHEN bought_id = INT256 '0' THEN {{axlUSDC_avalanche_c_token}}
            WHEN bought_id = INT256 '1' THEN {{usdc_e_avalanche_c_token}}
        END as token_bought_address,
        CASE
            WHEN sold_id = INT256 '0' THEN {{axlUSDC_avalanche_c_token}}
            WHEN sold_id = INT256 '1' THEN {{usdc_e_avalanche_c_token}}
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('curvefi_avalanche_c', 'axlUSDC_USDC_e_evt_TokenExchange') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL 

    -- blizz tokenexchange 
    SELECT
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        CAST(NULL as varchar) as version,
        buyer AS taker,
        CAST(NULL as varbinary) as maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        CASE
            WHEN bought_id = INT256 '0' THEN {{blizz_dai_avalanche_c_token}}
            WHEN bought_id = INT256 '1' THEN {{blizz_usdc_avalanche_c_token}}
            WHEN bought_id = INT256 '2' THEN {{blizz_usdt_avalanche_c_token}}
        END as token_bought_address,
        CASE
            WHEN sold_id = INT256 '0' THEN {{blizz_dai_avalanche_c_token}}
            WHEN sold_id = INT256 '1' THEN {{blizz_usdc_avalanche_c_token}}
            WHEN sold_id = INT256 '2' THEN {{blizz_usdt_avalanche_c_token}}
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('curvefi_avalanche_c', 'Blizz_evt_TokenExchange') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL 

    -- Curve_DD2_Pool tokenexchange 
    SELECT
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        CAST(NULL as varchar) as version,
        buyer AS taker,
        CAST(NULL as varbinary) as maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        CASE
            WHEN bought_id = INT256 '0' THEN {{dd_avalanche_c_token}}
            WHEN bought_id = INT256 '1' THEN {{usdc_e_avalanche_c_token}}
        END as token_bought_address,
        CASE
            WHEN sold_id = INT256 '0' THEN {{dd_avalanche_c_token}}
            WHEN sold_id = INT256 '1' THEN {{usdc_e_avalanche_c_token}}
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('curvefi_avalanche_c', 'Curve_DD2_Pool_evt_TokenExchange') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL 

    -- deBridge_USDC tokenexchange underlying 
    SELECT
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        CAST(NULL as varchar) as version,
        buyer AS taker,
        CAST(NULL as varbinary) as maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        CASE
            WHEN bought_id = INT256 '0' THEN {{debridge_usdc_avalanche_c_token}}
            WHEN bought_id = INT256 '1' THEN {{dai_e_avalanche_c_token}}
            WHEN bought_id = INT256 '2' THEN {{usdc_e_avalanche_c_token}}
            WHEN bought_id = INT256 '3' THEN {{usdt_e_avalanche_c_token}}
        END as token_bought_address,
        CASE
            WHEN sold_id = INT256 '0' THEN {{debridge_usdc_avalanche_c_token}}
            WHEN sold_id = INT256 '1' THEN {{av3CRV_avalanche_c_token}}
            WHEN sold_id = INT256 '2' THEN {{av3CRV_avalanche_c_token}}
            WHEN sold_id = INT256 '3' THEN {{av3CRV_avalanche_c_token}}
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('curvefi_avalanche_c', 'deBridge_USDC_evt_TokenExchangeUnderlying') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL 

    -- defrost h20 tokenexchange 
    SELECT
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        CAST(NULL as varchar) as version,
        buyer AS taker,
        CAST(NULL as varbinary) as maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        CASE
            WHEN bought_id = INT256 '0' THEN {{defrost_h20_avalanche_c_token}}
            WHEN bought_id = INT256 '1' THEN {{av3CRV_avalanche_c_token}}
        END as token_bought_address,
        CASE
            WHEN sold_id = INT256 '0' THEN {{defrost_h20_avalanche_c_token}}
            WHEN sold_id = INT256 '1' THEN {{av3CRV_avalanche_c_token}}
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('curvefi_avalanche_c', 'DefrostH2O_evt_TokenExchange') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL 

    -- defrost h20 tokenexchange underlying
    SELECT
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        CAST(NULL as varchar) as version,
        buyer AS taker,
        CAST(NULL as varbinary) as maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        CASE
            WHEN bought_id = INT256 '0' THEN {{defrost_h20_avalanche_c_token}}
            WHEN bought_id = INT256 '1' THEN {{dai_e_avalanche_c_token}}
            WHEN bought_id = INT256 '2' THEN {{usdc_e_avalanche_c_token}}
            WHEN bought_id = INT256 '3' THEN {{usdt_e_avalanche_c_token}}
        END as token_bought_address,
        CASE
            WHEN sold_id = INT256 '0' THEN {{defrost_h20_avalanche_c_token}}
            WHEN sold_id = INT256 '1' THEN {{av3CRV_avalanche_c_token}}
            WHEN sold_id = INT256 '2' THEN {{av3CRV_avalanche_c_token}}
            WHEN sold_id = INT256 '3' THEN {{av3CRV_avalanche_c_token}}
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('curvefi_avalanche_c', 'DefrostH2O_evt_TokenExchangeUnderlying') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL 

    -- eEURjEUR tokenexchange
    SELECT
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        CAST(NULL as varchar) as version,
        buyer AS taker,
        CAST(NULL as varbinary) as maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        CASE
            WHEN bought_id = INT256 '0' THEN {{eEUR_avalanche_c_token}}
            WHEN bought_id = INT256 '1' THEN {{jEUR_avalanche_c_token}}
        END as token_bought_address,
        CASE
            WHEN sold_id = INT256 '0' THEN {{eEUR_avalanche_c_token}}
            WHEN sold_id = INT256 '1' THEN {{jEUR_avalanche_c_token}}
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('curvefi_avalanche_c', 'eEURjEUR_evt_TokenExchange') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL 

    -- frax tokenexchange 
    SELECT
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        CAST(NULL as varchar) as version,
        buyer AS taker,
        CAST(NULL as varbinary) as maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        CASE
            WHEN bought_id = INT256 '0' THEN {{frax_avalanche_c_token}}
            WHEN bought_id = INT256 '1' THEN {{av3CRV_guage_avalanche_c_token}}
        END as token_bought_address,
        CASE
            WHEN sold_id = INT256 '0' THEN {{frax_avalanche_c_token}}
            WHEN sold_id = INT256 '1' THEN {{av3CRV_guage_avalanche_c_token}}
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('curvefi_avalanche_c', 'frax_evt_TokenExchange') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL 

    -- frax tokenexchange  underlying
    SELECT
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        CAST(NULL as varchar) as version,
        buyer AS taker,
        CAST(NULL as varbinary) as maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        CASE
            WHEN bought_id = INT256 '0' THEN {{frax_avalanche_c_token}}
            WHEN bought_id = INT256 '1' THEN {{dai_e_avalanche_c_token}}
            WHEN bought_id = INT256 '2' THEN {{usdc_e_avalanche_c_token}}
            WHEN bought_id = INT256 '3' THEN {{usdt_e_avalanche_c_token}}
        END as token_bought_address,
        CASE
            WHEN sold_id = INT256 '0' THEN {{frax_avalanche_c_token}}
            WHEN sold_id = INT256 '1' THEN {{av3CRV_avalanche_c_token}}
            WHEN sold_id = INT256 '2' THEN {{av3CRV_avalanche_c_token}}
            WHEN sold_id = INT256 '3' THEN {{av3CRV_avalanche_c_token}}
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('curvefi_avalanche_c', 'frax_evt_TokenExchangeUnderlying') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL 

    -- FUSD_MIM_Factory_Pool tokenexchange 
    SELECT
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        CAST(NULL as varchar) as version,
        buyer AS taker,
        CAST(NULL as varbinary) as maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        CASE
            WHEN bought_id = INT256 '0' THEN {{fusd_avalanche_c_token}}
            WHEN bought_id = INT256 '1' THEN {{mim_avalanche_c_token}}
        END as token_bought_address,
        CASE
            WHEN sold_id = INT256 '0' THEN {{fusd_avalanche_c_token}}
            WHEN sold_id = INT256 '1' THEN {{mim_avalanche_c_token}}
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('curvefi_avalanche_c', 'FUSD_MIM_Factory_Pool_evt_TokenExchange') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL 

    -- FUSDUSDC tokenexchange 
    SELECT
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        CAST(NULL as varchar) as version,
        buyer AS taker,
        CAST(NULL as varbinary) as maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        CASE
            WHEN bought_id = INT256 '0' THEN {{fusd_avalanche_c_token}}
            WHEN bought_id = INT256 '1' THEN {{usdc_e_avalanche_c_token}}
        END as token_bought_address,
        CASE
            WHEN sold_id = INT256 '0' THEN {{fusd_avalanche_c_token}}
            WHEN sold_id = INT256 '1' THEN {{usdc_e_avalanche_c_token}}
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('curvefi_avalanche_c', 'FUSDUSDC_evt_TokenExchange') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL 

    -- l2stableswap tokenexchange 
    SELECT
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        CAST(NULL as varchar) as version,
        buyer AS taker,
        CAST(NULL as varbinary) as maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        CASE
            WHEN bought_id = INT256 '0' THEN {{wavax_avalanche_c_token}}
            WHEN bought_id = INT256 '1' THEN {{ankr_aAVAXb_avalanche_c_token}}
        END as token_bought_address,
        CASE
            WHEN sold_id = INT256 '0' THEN {{wavax_avalanche_c_token}}
            WHEN sold_id = INT256 '1' THEN {{ankr_aAVAXb_avalanche_c_token}}
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('curvefi_avalanche_c', 'L2StableSwap_evt_TokenExchange') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL 

    -- mai tokenexchange 
    SELECT
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        CAST(NULL as varchar) as version,
        buyer AS taker,
        CAST(NULL as varbinary) as maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        CASE
            WHEN bought_id = INT256 '0' THEN {{mai_avalanche_c_token}}
            WHEN bought_id = INT256 '1' THEN {{av3CRV_avalanche_c_token}}
        END as token_bought_address,
        CASE
            WHEN sold_id = INT256 '0' THEN {{mai_avalanche_c_token}}
            WHEN sold_id = INT256 '1' THEN {{av3CRV_avalanche_c_token}}
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('curvefi_avalanche_c', 'MAI_evt_TokenExchange') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}
    
    UNION ALL 

    -- mai tokenexchange  underlying
    SELECT
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        CAST(NULL as varchar) as version,
        buyer AS taker,
        CAST(NULL as varbinary) as maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        CASE
            WHEN bought_id = INT256 '0' THEN {{mai_avalanche_c_token}}
            WHEN bought_id = INT256 '1' THEN {{dai_e_avalanche_c_token}}
            WHEN bought_id = INT256 '2' THEN {{usdc_e_avalanche_c_token}}
            WHEN bought_id = INT256 '3' THEN {{usdt_e_avalanche_c_token}}
        END as token_bought_address,
        CASE
            WHEN sold_id = INT256 '0' THEN {{mai_avalanche_c_token}}
            WHEN sold_id = INT256 '1' THEN {{av3CRV_avalanche_c_token}}
            WHEN sold_id = INT256 '2' THEN {{av3CRV_avalanche_c_token}}
            WHEN sold_id = INT256 '3' THEN {{av3CRV_avalanche_c_token}}
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('curvefi_avalanche_c', 'MAI_evt_TokenExchangeUnderlying') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL 

    -- mim tokenexchange 
    SELECT
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        CAST(NULL as varchar) as version,
        buyer AS taker,
        CAST(NULL as varbinary) as maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        CASE
            WHEN bought_id = INT256 '0' THEN {{mim_avalanche_c_token}}
            WHEN bought_id = INT256 '1' THEN {{av3CRV_avalanche_c_token}}
        END as token_bought_address,
        CASE
            WHEN sold_id = INT256 '0' THEN {{mim_avalanche_c_token}}
            WHEN sold_id = INT256 '1' THEN {{av3CRV_avalanche_c_token}}
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('curvefi_avalanche_c', 'MIM_evt_TokenExchange') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL 

    -- mim tokenexchange  underlying
    SELECT
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        CAST(NULL as varchar) as version,
        buyer AS taker,
        CAST(NULL as varbinary) as maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        CASE
            WHEN bought_id = INT256 '0' THEN {{mim_avalanche_c_token}}
            WHEN bought_id = INT256 '1' THEN {{dai_e_avalanche_c_token}}
            WHEN bought_id = INT256 '2' THEN {{usdc_e_avalanche_c_token}}
            WHEN bought_id = INT256 '3' THEN {{usdt_e_avalanche_c_token}}
        END as token_bought_address,
        CASE
            WHEN sold_id = INT256 '0' THEN {{mim_avalanche_c_token}}
            WHEN sold_id = INT256 '1' THEN {{av3CRV_avalanche_c_token}}
            WHEN sold_id = INT256 '2' THEN {{av3CRV_avalanche_c_token}}
            WHEN sold_id = INT256 '3' THEN {{av3CRV_avalanche_c_token}}
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('curvefi_avalanche_c', 'MIM_evt_TokenExchangeUnderlying') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL 

    -- MoreMoney_USD tokenexchange 
    SELECT
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        CAST(NULL as varchar) as version,
        buyer AS taker,
        CAST(NULL as varbinary) as maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        CASE
            WHEN bought_id = INT256 '0' THEN {{moremoney_avalanche_c_token}}
            WHEN bought_id = INT256 '1' THEN {{av3CRV_avalanche_c_token}}
        END as token_bought_address,
        CASE
            WHEN sold_id = INT256 '0' THEN {{moremoney_avalanche_c_token}}
            WHEN sold_id = INT256 '1' THEN {{av3CRV_avalanche_c_token}}
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('curvefi_avalanche_c', 'MoreMoney_USD_evt_TokenExchange') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL  

    -- MoreMoney_USD tokenexchange  underlying
    SELECT
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        CAST(NULL as varchar) as version,
        buyer AS taker,
        CAST(NULL as varbinary) as maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        CASE
            WHEN bought_id = INT256 '0' THEN {{moremoney_avalanche_c_token}}
            WHEN bought_id = INT256 '1' THEN {{dai_e_avalanche_c_token}}
            WHEN bought_id = INT256 '2' THEN {{usdc_e_avalanche_c_token}}
            WHEN bought_id = INT256 '3' THEN {{usdt_e_avalanche_c_token}}
        END as token_bought_address,
        CASE
            WHEN sold_id = INT256 '0' THEN {{moremoney_avalanche_c_token}}
            WHEN sold_id = INT256 '1' THEN {{av3CRV_avalanche_c_token}}
            WHEN sold_id = INT256 '2' THEN {{av3CRV_avalanche_c_token}}
            WHEN sold_id = INT256 '3' THEN {{av3CRV_avalanche_c_token}}
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('curvefi_avalanche_c', 'MoreMoney_USD_evt_TokenExchangeUnderlying') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL  

    -- NXUSDaV3CRV tokenexchange  underlying
    SELECT
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        CAST(NULL as varchar) as version,
        buyer AS taker,
        CAST(NULL as varbinary) as maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        CASE
            WHEN bought_id = INT256 '0' THEN {{nxusd_avalanche_c_token}}
            WHEN bought_id = INT256 '1' THEN {{dai_e_avalanche_c_token}}
            WHEN bought_id = INT256 '2' THEN {{usdc_e_avalanche_c_token}}
            WHEN bought_id = INT256 '3' THEN {{usdt_e_avalanche_c_token}}
        END as token_bought_address,
        CASE
            WHEN sold_id = INT256 '0' THEN {{nxusd_avalanche_c_token}}
            WHEN sold_id = INT256 '1' THEN {{av3CRV_avalanche_c_token}}
            WHEN sold_id = INT256 '2' THEN {{av3CRV_avalanche_c_token}}
            WHEN sold_id = INT256 '3' THEN {{av3CRV_avalanche_c_token}}
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('curvefi_avalanche_c', 'NXUSDaV3CRV_evt_TokenExchangeUnderlying') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL 

    -- ren tokenexchange 
    SELECT
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        CAST(NULL as varchar) as version,
        buyer AS taker,
        CAST(NULL as varbinary) as maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        CASE
            WHEN bought_id = INT256 '0' THEN {{aave_wbtc_avalanche_c_token}}
            WHEN bought_id = INT256 '1' THEN {{renBTC_avalanche_c_token}}
        END as token_bought_address,
        CASE
            WHEN sold_id = INT256 '0' THEN {{aave_wbtc_avalanche_c_token}}
            WHEN sold_id = INT256 '1' THEN {{renBTC_avalanche_c_token}}
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('curvefi_avalanche_c', 'ren_evt_TokenExchange') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL 

    -- ren tokenexchange underlying 
    SELECT
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        CAST(NULL as varchar) as version,
        buyer AS taker,
        CAST(NULL as varbinary) as maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        CASE
            WHEN bought_id = INT256 '0' THEN {{wbtc_e_avalanche_c_token}}
            WHEN bought_id = INT256 '1' THEN {{renBTC_avalanche_c_token}}
        END as token_bought_address,
        CASE
            WHEN sold_id = INT256 '0' THEN {{wbtc_e_avalanche_c_token}}
            WHEN sold_id = INT256 '1' THEN {{renBTC_avalanche_c_token}}
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('curvefi_avalanche_c', 'ren_evt_TokenExchangeUnderlying') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL 

    -- Topshelf_USDL tokenexchange  underlying
    SELECT
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        CAST(NULL as varchar) as version,
        buyer AS taker,
        CAST(NULL as varbinary) as maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        CASE
            WHEN bought_id = INT256 '0' THEN {{usdl_avalanche_c_token}}
            WHEN bought_id = INT256 '1' THEN {{dai_e_avalanche_c_token}}
            WHEN bought_id = INT256 '2' THEN {{usdc_e_avalanche_c_token}}
            WHEN bought_id = INT256 '3' THEN {{usdt_e_avalanche_c_token}}
        END as token_bought_address,
        CASE
            WHEN sold_id = INT256 '0' THEN {{usdl_avalanche_c_token}}
            WHEN sold_id = INT256 '1' THEN {{av3CRV_avalanche_c_token}}
            WHEN sold_id = INT256 '2' THEN {{av3CRV_avalanche_c_token}}
            WHEN sold_id = INT256 '3' THEN {{av3CRV_avalanche_c_token}}
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('curvefi_avalanche_c', 'Topshelf_USDL_evt_TokenExchangeUnderlying') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL 

    -- USDCe_UST tokenexchange 
    SELECT
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        CAST(NULL as varchar) as version,
        buyer AS taker,
        CAST(NULL as varbinary) as maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        CASE
            WHEN bought_id = INT256 '0' THEN {{usdc_e_avalanche_c_token}}
            WHEN bought_id = INT256 '1' THEN {{ust_wormhole_avalanche_c_token}}
        END as token_bought_address,
        CASE
            WHEN sold_id = INT256 '0' THEN {{usdc_e_avalanche_c_token}}
            WHEN sold_id = INT256 '1' THEN {{ust_wormhole_avalanche_c_token}}
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('curvefi_avalanche_c', 'USDCe_UST_evt_TokenExchange') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL 

    -- USD coin tokenexchange 
    SELECT
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        CAST(NULL as varchar) as version,
        buyer AS taker,
        CAST(NULL as varbinary) as maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        CASE
            WHEN bought_id = INT256 '0' THEN {{usdc_e_avalanche_c_token}}
            WHEN bought_id = INT256 '1' THEN {{usdc_avalanche_c_token}}
        END as token_bought_address,
        CASE
            WHEN sold_id = INT256 '0' THEN {{usdc_e_avalanche_c_token}}
            WHEN sold_id = INT256 '1' THEN {{usdc_avalanche_c_token}}
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('curvefi_avalanche_c', 'USD_coin_evt_TokenExchange') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL 

    -- usds tokenexchange  underlying
    SELECT
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        CAST(NULL as varchar) as version,
        buyer AS taker,
        CAST(NULL as varbinary) as maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        CASE
            WHEN bought_id = INT256 '0' THEN {{usds_avalanche_c_token}}
            WHEN bought_id = INT256 '1' THEN {{dai_e_avalanche_c_token}}
            WHEN bought_id = INT256 '2' THEN {{usdc_e_avalanche_c_token}}
            WHEN bought_id = INT256 '3' THEN {{usdt_e_avalanche_c_token}}
        END as token_bought_address,
        CASE
            WHEN sold_id = INT256 '0' THEN {{usds_avalanche_c_token}}
            WHEN sold_id = INT256 '1' THEN {{av3CRV_avalanche_c_token}}
            WHEN sold_id = INT256 '2' THEN {{av3CRV_avalanche_c_token}}
            WHEN sold_id = INT256 '3' THEN {{av3CRV_avalanche_c_token}}
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('curvefi_avalanche_c', 'USDS_evt_TokenExchangeUnderlying') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL 

    -- yusdpool tokenexchange 
    SELECT
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        CAST(NULL as varchar) as version,
        buyer AS taker,
        CAST(NULL as varbinary) as maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        CASE
            WHEN bought_id = INT256 '0' THEN {{yusd_avalanche_c_token}}
            WHEN bought_id = INT256 '1' THEN {{usdc_avalanche_c_token}}
            WHEN bought_id = INT256 '2' THEN {{usdt_avalanche_c_token}}
        END as token_bought_address,
        CASE
            WHEN sold_id = INT256 '0' THEN {{yusd_avalanche_c_token}}
            WHEN sold_id = INT256 '1' THEN {{usdc_avalanche_c_token}}
            WHEN sold_id = INT256 '2' THEN {{usdt_avalanche_c_token}}
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('curvefi_avalanche_c', 'YUSDPOOL_evt_TokenExchange') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}
)

SELECT
    'avalanche_c' AS blockchain,
    'curve' AS project,
    '2' AS version,
    CAST(date_trunc('month', dexs.block_time) AS date) AS block_month,
    CAST(date_trunc('day', dexs.block_time) AS date) AS block_date,
    dexs.block_time,
    dexs.block_number,
    dexs.token_bought_amount_raw,
    dexs.token_sold_amount_raw,
    dexs.token_bought_address,
    dexs.token_sold_address,
    dexs.taker,
    dexs.maker,
    dexs.project_contract_address,
    dexs.tx_hash,
    dexs.evt_index
FROM dexs
