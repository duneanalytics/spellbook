{{ config(
    schema = 'bridges_evms'
    , alias = 'deposits'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy='merge'
    , unique_key = ['deposit_chain','withdrawal_chain','bridge_name','bridge_version','bridge_transfer_id', 'duplicate_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
)
}}

SELECT d.deposit_chain
, d.withdrawal_chain
, d.bridge_name
, d.bridge_version
, d.block_date
, d.block_time
, d.block_number
, d.deposit_amount_raw
, d.deposit_amount_raw/POWER(10, p.decimals) AS deposit_amount
, p.price*d.deposit_amount_raw/POWER(10, p.decimals) AS deposit_amount_usd
, d.sender
, d.recipient
, d.deposit_token_standard
, d.deposit_token_address
, d.tx_from
, d.tx_hash
, d.evt_index
, d.contract_address
, d.bridge_transfer_id
{% if is_incremental() %}
, COALESCE(cd.duplicate_index, 0) + ROW_NUMBER() OVER (PARTITION BY d.deposit_chain, d.withdrawal_chain, d.bridge_name, d.bridge_version, d.bridge_transfer_id ORDER BY d.block_number, d.evt_index ) AS duplicate_index
{% else %}
, ROW_NUMBER() OVER (PARTITION BY d.deposit_chain, d.withdrawal_chain, d.bridge_name, d.bridge_version, d.bridge_transfer_id ORDER BY d.block_number, d.evt_index ) AS duplicate_index
{% endif %}
FROM {{ ref('bridges_evms_deposits_raw') }} d
INNER JOIN {{ source('prices', 'usd') }} p ON p.blockchain=d.deposit_chain
    AND p.contract_address=d.deposit_token_address
    AND p.minute=date_trunc('minute', d.block_time)
    {% if is_incremental() %}
    AND {{ incremental_predicate('p.minute') }}
    {% endif %}
{% if is_incremental() %}
{% endif %}
{% if is_incremental() %}
WHERE {{ incremental_predicate('d.block_time') }}
{% endif %}
