
-- applicable for multiple L2s that expose this in their tx receipt
{% macro has_l1_fee(blockchain) %}
    {{ return(
        blockchain in all_op_chains() + ['scroll']
    ) }}
{% endmacro %}

-- only possible if we've built out the blob_submissions model
{% macro has_blob_fee(blockchain) %}
    {{ return(
        blockchain in ['ethereum'])
    ) }}
{% endmacro %}

-- applicable for arbitrum stack
{% macro has_effective_gas_price(blockchain) %}
    {{ return(
        blockchain in ['arbitrum'])
    ) }}
{% endmacro %}

{% macro gas_fees(blockchain) %}
WITH base_model as (
    SELECT
        txns.block_time
        ,txns.block_number
        ,txns.hash AS tx_hash
        ,txns."from" AS tx_from
        ,txns.to AS tx_to
        ,{%- if has_effective_gas_price(blockchain) -%}
          txns.effective_gas_price {%- else -%} txns.gas_price
        {%- endif -%} as gas_price
        ,txns.gas_used as gas_used
        ,{%- if has_l1_fee(blockchain) -%}
        cast(l1_fee as uint256) +
        {%- endif -%}
        {%- if has_blob_fee(blockchain) -%}
        cast(blob.blob_base_fee as uint256) * cast(blob.blob_gas_used as uint256) +
        {%- endif -%}
        cast(
            {%- if has_effective_gas_price(blockchain) -%} txns.effective_gas_price {%- else -%} txns.gas_price {%- endif -%}
        as uint256) * cast(txns.gas_used as uint256)
        as tx_fee_raw
        ,blocks.miner AS block_proposer
        ,txns.max_fee_per_gas
        ,txns.priority_fee_per_gas
        ,txns.max_priority_fee_per_gas
        ,blocks.base_fee_per_gas
        ,txns.gas_limit
        ,CASE  --safe divide-by-zero
            WHEN txns.gas_limit = 0 THEN NULL
            WHEN txns.gas_limit != 0 THEN cast(txns.gas_used as double) / cast(txns.gas_limit as double)
        END AS gas_limit_usage
    FROM {{ source( blockchain, 'transactions') }} txns
    INNER JOIN {{ source( blockchain, 'blocks') }} blocks
        ON txns.block_number = blocks.number
        {% if is_incremental() %}
        AND {{ incremental_predicate('blocks.time') }}
        {% endif %}
    {%- if has_blob_fee(blockchain) -%}
    LEFT JOIN {{ source( blockchain, 'blobs_submissions') }} blob
        ON txns.hash = blob.tx_hash
        AND txns.block_number = blob.block_number
        {% if is_incremental() %}
        AND {{ incremental_predicate('blob.block_time') }}
        {% endif %}
    {%- endif -%}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('txns.block_time') }}
    {% else %}
    WHERE txns.block_time >= date '2024-08-01'
    OR txns.hash in (select tx_hash from {{ref('evm_gas_fees')}})
    {% endif %}
    )

SELECT
    '{{blockchain}}' as blockchain
    ,CAST(date_trunc('month', block_time) AS DATE) AS block_month
    ,CAST(date_trunc('day', block_time) AS DATE) AS block_date
    ,block_time
    ,block_number
    ,tx_hash
    ,tx_from
    ,tx_to
    ,gas_price
    ,gas_used
    ,p.symbol as token_symbol
    ,tx_fee_raw
    ,tx_fee_raw / pow(10,p.decimals) as tx_fee
    ,tx_fee_raw / pow(10,p.decimals) * p.price as tx_fee_usd
    ,block_proposer
    ,max_fee_per_gas
    ,priority_fee_per_gas
    ,max_priority_fee_per_gas
    ,base_fee_per_gas
    ,gas_limit
    ,gas_limit_usage
FROM base_model
LEFT JOIN {{ref('prices_usd_native')}} p
    ON p.blockchain = '{{blockchain}}'
    AND p.contract_address = {{var('ETH_ERC20_ADDRESS')}}
    AND p.minute = date_trunc('minute', block_time)
{% endmacro %}
