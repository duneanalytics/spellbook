{{ config(
    
    alias = 'donations',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "gitcoin",
                                \'["hildobby"]\') }}'
    )
}}

{% set eth_contract = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' %}
{% set weth_contract = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' %}

WITH gitcoin_donations AS (
    SELECT gd.evt_block_number AS block_number
    , gd.evt_block_time AS block_time
    , gd.amount AS amount_raw
    , CASE WHEN gd.token = {{eth_contract}}
        THEN gd.amount/POWER(10, 18)
        ELSE gd.amount/POWER(10, tok.decimals)
        END AS amount_original
    , gd.donor AS donor
    , gd.dest AS recipient
    , CASE WHEN gd.token = {{eth_contract}}
        THEN {{weth_contract}}
        ELSE gd.token
        END AS currency_contract
    , CASE WHEN gd.token = {{eth_contract}}
        THEN 'ETH'
        ELSE tok.symbol
        END AS currency_symbol
    , gd.evt_index
    , gd.contract_address
    , gd.evt_tx_hash AS tx_hash
    FROM {{ source('gitcoin_ethereum', 'BulkCheckout_evt_DonationSent') }} gd
    LEFT JOIN {{ source('tokens_ethereum', 'erc20') }} tok
        ON tok.contract_address=gd.token
    {% if is_incremental() %}
    WHERE gd.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    )


SELECT 'ethereum' AS blockchain
, 'gitcoin' AS project
, 'v1' AS version
, grd.round_name AS grant_round
, date_trunc('day', gd.block_time) AS block_date
, CAST(date_trunc('month', gd.block_time) AS DATE) AS block_month
, gd.block_time
, gd.block_number
, gd.amount_raw
, gd.amount_original
, gd.amount_original*pu.price AS amount_usd
, gd.donor
, gd.recipient
, gd.currency_contract
, gd.currency_symbol
, gd.evt_index
, gd.contract_address
, gd.tx_hash
FROM gitcoin_donations gd
LEFT JOIN {{ ref('gitcoin_grant_round_dates') }} grd ON grd.start_date <= gd.block_time
    AND gd.block_time < grd.end_date
LEFT JOIN {{ source('prices', 'usd') }} pu ON pu.blockchain='ethereum'
    AND pu.contract_address=gd.currency_contract
    AND pu.minute=date_trunc('minute', gd.block_time)