{{ config(
    schema = 'bridges_evms'
    , alias = 'withdrawals'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy='append'
    , unique_key = ['deposit_chain','deposit_chain_id','withdrawal_chain','bridge_name','bridge_version','bridge_transfer_id','duplicate_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
)
}}

{% if is_incremental() %}
WITH new_raw_keys AS (
    SELECT DISTINCT deposit_chain
    , deposit_chain_id
    , withdrawal_chain
    , bridge_name
    , bridge_version
    , bridge_transfer_id
    FROM {{ ref('bridges_evms_withdrawals_raw') }} rw
    WHERE {{ incremental_predicate('rw.block_time') }}
    )
    
, check_dupes AS (
    SELECT n.deposit_chain
    , n.deposit_chain_id
    , n.withdrawal_chain
    , n.bridge_name
    , n.bridge_version
    , n.bridge_transfer_id
    , MAX(t.duplicate_index) AS duplicate_index
    FROM new_raw_keys n
    INNER JOIN {{ this }} t ON n.deposit_chain=t.deposit_chain
        AND n.deposit_chain_id=t.deposit_chain_id
        AND n.withdrawal_chain=t.withdrawal_chain
        AND n.bridge_name=t.bridge_name
        AND n.bridge_version=t.bridge_version
        AND n.bridge_transfer_id=t.bridge_transfer_id
    GROUP BY 1, 2, 3, 4, 5, 6
    )
{% endif %}

SELECT w.deposit_chain_id
, w.deposit_chain
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
{% if is_incremental() %}
, COALESCE(cd.duplicate_index, 0)+ROW_NUMBER() OVER (PARTITION BY w.deposit_chain, w.deposit_chain_id, w.withdrawal_chain, w.bridge_name, w.bridge_version, w.bridge_transfer_id ORDER BY w.block_number, w.evt_index ) AS duplicate_index
{% else %}
, ROW_NUMBER() OVER (PARTITION BY w.deposit_chain, w.deposit_chain_id, w.withdrawal_chain, w.bridge_name, w.bridge_version, w.bridge_transfer_id ORDER BY w.block_number, w.evt_index ) AS duplicate_index
{% endif %}
FROM {{ ref('bridges_evms_withdrawals_raw') }} w
INNER JOIN {{ source('prices', 'usd') }} p ON p.blockchain=w.withdrawal_chain
    AND p.contract_address=w.withdrawal_token_address
    AND p.minute=date_trunc('minute', w.block_time)
    {% if is_incremental() %}
    AND {{ incremental_predicate('p.minute') }}
    {% endif %}
{% if is_incremental() %}
LEFT JOIN {{ this }} t ON d.withdrawal_chain = t.withdrawal_chain
    AND d.tx_hash = t.tx_hash
    AND d.evt_index = t.evt_index
LEFT JOIN check_dupes cd ON w.deposit_chain = cd.deposit_chain
    AND w.deposit_chain_id = cd.deposit_chain_id
    AND w.withdrawal_chain = cd.withdrawal_chain
    AND w.bridge_name = cd.bridge_name
    AND w.bridge_version = cd.bridge_version
    AND w.bridge_transfer_id = cd.bridge_transfer_id
WHERE {{ incremental_predicate('w.block_time') }}
AND t.bridge_transfer_id IS NULL
{% endif %}
    