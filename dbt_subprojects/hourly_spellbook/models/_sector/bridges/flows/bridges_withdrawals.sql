{{ config(
    schema = 'bridges'
    , alias = 'withdrawals'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy='merge'
    , unique_key = ['deposit_chain','tx_hash','evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
)
}}

{% set chains = [
    'ethereum'
    , 'base'
] %}

WITH grouped_finalised_events AS (
SELECT *
FROM (
        {% for chain in chains %}
        SELECT deposit_chain
        , withdrawal_chain
        , bridge_name
        , bridge_version
        , canonical_bridge
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
        , transfer_id
        FROM {{ ref('bridges_'~chain~'_withdrawals') }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        )
    )

SELECT deposit_chain
, withdrawal_chain
, bridge_name
, bridge_version
, canonical_bridge
, block_date
, block_time
, block_number
, withdrawal_amount_raw
, withdrawal_amount_raw/POWER(10, pus.decimals) AS withdrawal_amount
, pus.price*withdrawal_amount_raw/POWER(10, pus.decimals) AS withdrawal_amount_usd
, sender
, recipient
, withdrawal_token_standard
, withdrawal_token_address
, tx_from
, tx_hash
, evt_index
, contract_address
, transfer_id
FROM grouped_finalised_events i
INNER JOIN {{ source('prices', 'usd') }} pus ON pus.blockchain=i.withdrawal_chain
    AND pus.contract_address=i.deposit_token_address
    AND pus.minute=date_trunc('minute', block_time)
    {% if is_incremental() %}
    AND  {{ incremental_predicate('pus.minute') }}
    {% endif %}