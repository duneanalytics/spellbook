{{ config(
    schema = 'bridges_evms'
    , alias = 'withdrawals'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy='merge'
    , unique_key = ['deposit_chain','withdrawal_chain','bridge_name','bridge_version','bridge_transfer_id']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
)
}}

{% set chains = [
    'ethereum'
    , 'base'
    , 'arbitrum'
    , 'avalanche_c'
    , 'optimism'
    , 'polygon'
    , 'unichain'
] %}

WITH grouped_withdrawals AS (
SELECT *
FROM (
        {% for chain in chains %}
        SELECT deposit_chain
        , withdrawal_chain
        , bridge_name
        , bridge_version
        , block_date
        , block_time
        , block_number
        , withdrawal_amount_raw
        , sender
        , recipient
        , withdrawal_token_standard
        , withdrawal_token_address
        , tx_from
        , tx_hash
        , evt_index
        , contract_address
        , bridge_transfer_id
        FROM {{ ref('bridges_'~chain~'_withdrawals') }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        )
    )

SELECT w.deposit_chain
, w.withdrawal_chain
, w.bridge_name
, w.bridge_version
, w.block_date
, w.block_time
, w.block_number
, w.withdrawal_amount_raw
, w.withdrawal_amount_raw/POWER(10, p.decimals) AS withdrawal_amount
, p.price*w.withdrawal_amount_raw/POWER(10, p.decimals) AS withdrawal_amount_usd
, w.sender
, w.recipient
, w.withdrawal_token_standard
, w.withdrawal_token_address
, w.tx_from
, w.tx_hash
, w.evt_index
, w.contract_address
, w.bridge_transfer_id
FROM grouped_withdrawals w
INNER JOIN {{ source('prices', 'usd') }} p ON p.blockchain=w.withdrawal_chain
    AND p.contract_address=w.withdrawal_token_address
    AND p.minute=date_trunc('minute', w.block_time)
    {% if is_incremental() %}
    AND {{ incremental_predicate('p.minute') }}
    {% endif %}