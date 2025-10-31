{{ config(
    schema = 'bridges_evms'
    , alias = 'deposits'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy='append'
    , unique_key = ['deposit_chain','withdrawal_chain','withdrawal_chain_id','bridge_name','bridge_version','bridge_transfer_id', 'duplicate_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
)
}}

{% if is_incremental() %}
WITH new_raw_keys AS (
    SELECT DISTINCT deposit_chain
    , withdrawal_chain
    , withdrawal_chain_id
    , bridge_name
    , bridge_version
    , bridge_transfer_id
    FROM {{ ref('bridges_evms_deposits_raw') }} rw
    WHERE {{ incremental_predicate('rw.block_time') }}
    )
    
, check_dupes AS (
    SELECT n.deposit_chain
    , n.withdrawal_chain_id
    , n.withdrawal_chain
    , n.bridge_name
    , n.bridge_version
    , n.bridge_transfer_id
    , MAX(t.duplicate_index) AS duplicate_index
    FROM new_raw_keys n
    INNER JOIN {{ this }} t ON n.deposit_chain=t.deposit_chain
        AND n.withdrawal_chain=t.withdrawal_chain
        AND n.withdrawal_chain_id=t.withdrawal_chain_id
        AND n.bridge_name=t.bridge_name
        AND n.bridge_version=t.bridge_version
        AND n.bridge_transfer_id=t.bridge_transfer_id
    GROUP BY 1, 2, 3, 4, 5, 6
    )
{% endif %}

SELECT d.deposit_chain
, d.withdrawal_chain_id
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
, COALESCE(cd.duplicate_index, 0)+ROW_NUMBER() OVER (PARTITION BY d.deposit_chain, d.withdrawal_chain, d.withdrawal_chain_id, d.bridge_name, d.bridge_version, d.bridge_transfer_id ORDER BY d.block_number, d.evt_index ) AS duplicate_index
{% else %}
, ROW_NUMBER() OVER (PARTITION BY d.deposit_chain, d.withdrawal_chain, d.withdrawal_chain_id, d.bridge_name, d.bridge_version, d.bridge_transfer_id ORDER BY d.block_number, d.evt_index ) AS duplicate_index
{% endif %}
FROM {{ ref('bridges_evms_deposits_raw') }} d
INNER JOIN {{ source('prices', 'usd') }} p ON p.blockchain=d.deposit_chain
    AND p.contract_address=d.deposit_token_address
    AND p.minute=date_trunc('minute', d.block_time)
    {% if is_incremental() %}
    AND {{ incremental_predicate('p.minute') }}
    {% endif %}
{% if is_incremental() %}
LEFT JOIN {{ this }} t ON d.deposit_chain = t.deposit_chain
    AND d.tx_hash = t.tx_hash
    AND d.evt_index = t.evt_index
LEFT JOIN check_dupes cd ON d.deposit_chain = cd.deposit_chain
    AND d.withdrawal_chain = cd.withdrawal_chain
    AND d.withdrawal_chain_id = cd.withdrawal_chain_id
    AND d.bridge_name = cd.bridge_name
    AND d.bridge_version = cd.bridge_version
    AND d.bridge_transfer_id = cd.bridge_transfer_id
WHERE {{ incremental_predicate('d.block_time') }}
AND t.bridge_transfer_id IS NULL
{% endif %}
