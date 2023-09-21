{% macro balances_fungible_suicide(blockchain) %}
WITH

suicide_creations as (
    SELECT 
        "from" as suicide_contract, 
        tx_index,
        address as contract_address, 
        tx_hash
    FROM 
    {{ source( blockchain , 'traces') }}
    WHERE type = 'create'
    AND call_type IS NULL -- helpful filter
    AND refund_address IS NULL -- filter
    AND value = UINT256 '0'
    {% if is_incremental() %}
    AND block_time >= date_trunc('day', now() - interval '3' Day)
    {% endif %}
),

suicide_join_logs as (
    SELECT 
        el.tx_hash, 
        el.tx_index, 
        el.block_time, 
        CAST(date_trunc('month', el.block_time) as date) as block_month,
        bytearray_substring(el.data, 13, 20) as suicide_contract_address, 
        bytearray_substring(el.data, 45, 20) as suicide_refund_address, 
        bytearray_to_uint256(bytearray_substring(el.data,65,32)) as amount_raw 
    FROM 
    {{ source( blockchain , 'logs') }} el 
    INNER JOIN 
    suicide_creations sc 
        ON el.contract_address = sc.suicide_contract
        AND bytearray_substring(el.data, 13, 20) = sc.contract_address
        AND el.tx_hash = sc.tx_hash 
        AND el.tx_index = sc.tx_index
    {% if is_incremental() %}
    WHERE el.block_time >= date_trunc('day', now() - interval '3' Day)
    {% else %}
    WHERE 1 = 1 -- consider removing this incremental if no case of these contracts show up
    {% endif %}
    AND el.topic0 IS NULL 
    AND bytearray_to_uint256(bytearray_substring(el.data,65,32)) > UINT256 '0'
)

SELECT 
    suicide_contract_address as address,
    block_month
FROM 
suicide_join_logs

UNION ALL 

SELECT 
    suicide_refund_address as address,  -- these would cause duplicated entries in the table but since we're doing a left join to filter then we can leave it instead of trying to find a distinct
    block_month
FROM 
suicide_join_logs
{% endmacro %}