{{ config(
    schema = 'bridges_evms'
    , alias = 'deposits_raw'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy='merge'
    , unique_key = ['deposit_chain','withdrawal_chain','bridge_name','bridge_version','bridge_transfer_id', 'duplicate_index']
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

WITH raw_deposits AS (
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
        , deposit_amount_raw
        , sender
        , recipient
        , deposit_token_standard
        , deposit_token_address
        , tx_from
        , tx_hash
        , evt_index
        , contract_address
        , bridge_transfer_id
        FROM {{ ref('bridges_'~chain~'_deposits') }}
        {% if is_incremental() %}
        WHERE  {{ incremental_predicate('block_time') }}
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %} 
        )
    )

{% if is_incremental() %}
WITH check_dupes AS (
    SELECT deposit_chain
    , withdrawal_chain
    , bridge_name
    , bridge_version
    , bridge_transfer_id
    , MAX(duplicate_index) AS duplicate_index
    FROM raw_deposits rd
    INNER JOIN {{ this }} t USING (deposit_chain, withdrawal_chain, bridge_name, bridge_version, bridge_transfer_id)
    WHERE {{ incremental_predicate('rd.block_time') }}
    GROUP BY 1, 2, 3, 4, 5
    )
{% endif %}

    SELECT deposit_chain
, withdrawal_chain
, bridge_name
, bridge_version
, bridge_transfer_id
{% if is_incremental() %}
, COALESCE(cd.duplicate_index, 0) + ROW_NUMBER() OVER (PARTITION BY d.deposit_chain, d.withdrawal_chain, d.bridge_name, d.bridge_version, d.bridge_transfer_id ORDER BY d.block_number, d.evt_index ) AS duplicate_index
{% else %}
, ROW_NUMBER() OVER (PARTITION BY d.deposit_chain, d.withdrawal_chain, d.bridge_name, d.bridge_version, d.bridge_transfer_id ORDER BY d.block_number, d.evt_index ) AS duplicate_index
{% endif %}
, ROW_NUMBER() OVER (PARTITION BY d.deposit_chain, d.withdrawal_chain, d.bridge_name, d.bridge_version, d.bridge_transfer_id ORDER BY d.block_number, d.evt_index) AS duplicate_index
FROM raw_deposits rd
{% if is_incremental() %}
INNER JOIN check_dupes cd ON rd.deposit_chain = cd.deposit_chain
    AND rd.withdrawal_chain = cd.withdrawal_chain
    AND rd.bridge_name = cd.bridge_name
    AND rd.bridge_version = cd.bridge_version
    AND rd.bridge_transfer_id = cd.bridge_transfer_id
{% endif %}
