{{ config(
    alias = 'quotation_trades',
    
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date',  'evt_tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "nexusmutual",
                                \'["sharkxff"]\') }}'
    )
}}

{% set project_start_date = '2019-07-12' %}

WITH quo_evt AS (
    SELECT cid,
           contract_address,
           evt_block_number,
           evt_block_time,
           evt_index,
           evt_tx_hash,
           curr,
           expiry,
           premium,
           premiumNXM,
           scAdd,
           sumAssured,
           0xd7c49cee7e9188cca6ad8ff264c1da2e69d4cf3b as token
    FROM
        {{ source('nexusmutual_ethereum', 'QuotationData_evt_CoverDetailsEvent') }}
    {% if not is_incremental() %}
    WHERE evt_block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
)

SELECT quo_evt.cid,
       quo_evt.contract_address,
       quo_evt.token                                                AS token_address,
       erc20.symbol,
       quo_evt.evt_index,
       quo_evt.evt_tx_hash,
       quo_evt.curr,
       quo_evt.premium,
       quo_evt.premium * power(10, erc20.decimals)                  AS pre_amount,
       quo_evt.premiumNXM                                           AS premium_nxm,
       quo_evt.premiumNXM * power(10, erc20.decimals)               AS pre_nxm_amount,
       quo_evt.scAdd                                                AS sc_add,
       quo_evt.sumAssured                                           AS sum_assured,
       tx.block_hash,
       tx.nonce,
       tx.gas_limit,
       tx.gas_price,
       tx.gas_used,
       tx.max_fee_per_gas,
       tx.max_priority_fee_per_gas,
       tx.priority_fee_per_gas,
       tx.success,
       tx.type                                                     AS tx_type,
       tx.value                                                    AS tx_value,
       quo_evt.evt_block_number                                    AS evt_block_number,
       quo_evt.evt_block_time                                      AS evt_block_time,
       quo_evt.expiry                                              AS evt_expiry,
       from_unixtime(TRY_CAST(quo_evt.expiry as double) ) AS evt_expiry_date,
       TRY_CAST(date_trunc('DAY', quo_evt.evt_block_time) AS date) AS block_date,
       TRY_CAST(date_trunc('month', quo_evt.evt_block_time) AS date) AS block_month
FROM quo_evt
INNER JOIN {{ source('ethereum','transactions') }} tx
    ON quo_evt.evt_tx_hash = tx.hash
    AND tx.success is NOT NULL
    {% if not is_incremental() %}
    AND tx.block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ source('tokens', 'erc20') }} erc20
    ON quo_evt.token = erc20.contract_address
    AND erc20.blockchain = 'ethereum'