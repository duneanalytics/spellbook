
-- applicable for multiple L2s that expose this in their tx receipt
{% macro has_l1_fee(blockchain) %}
    {{ return(
        blockchain in all_op_chains() + ('scroll','blast','mantle'))
    }}
{% endmacro %}

-- only possible if we've built out the blob_submissions model
{% macro has_blob_fee(blockchain) %}
    {{ return(
        blockchain in ['ethereum'])
    }}
{% endmacro %}

-- applicable for arbitrum stack and celo
{% macro select_gas_price(blockchain) %}
    {%- if blockchain in ['arbitrum']-%}
    txns.effective_gas_price
    {%- elif blockchain in ['celo'] -%}
    case when txns.gas_price = 0 then txns.priority_fee_per_gas else txns.gas_price end
    {%- else -%}
    txns.gas_price
    {%- endif -%}
{% endmacro %}

-- applicable on Celo
{% macro has_fee_currency(blockchain) %}
    {{ return(
        blockchain in ['celo'])
    }}
{% endmacro %}

{% macro gas_fees(blockchain) %}
WITH base_model as (
    SELECT
        txns.block_time
        ,txns.block_number
        ,txns.hash AS tx_hash
        ,txns."from" AS tx_from
        ,txns.to AS tx_to
        ,cast({{ select_gas_price(blockchain) }} as uint256) as gas_price
        ,txns.gas_used as gas_used
        ,{%- if has_l1_fee(blockchain) -%}
          cast(coalesce(l1_fee,0) as uint256) +
        {%- endif -%}
        {%- if has_blob_fee(blockchain) -%}
          cast(coalesce(blob.blob_base_fee,0) as uint256) * cast(coalesce(blob.blob_gas_used,0) as uint256) +
        {%- endif -%}
          cast({{ select_gas_price(blockchain) }} as uint256) * cast(txns.gas_used as uint256)
        as tx_fee_raw
        ,map_concat(map()
            {%- if has_l1_fee(blockchain) %}
              ,map(array['l1_fee'], array[cast(coalesce(l1_fee,0) as uint256)])
            {%- endif -%}
            {%- if has_blob_fee(blockchain) %}
              ,map(array['blob_fee'],array[cast(coalesce(blob.blob_base_fee,0) as uint256) * cast(coalesce(blob.blob_gas_used,0) as uint256)])
            {%- endif %}
              ,case when txns.priority_fee_per_gas is null
                    then map(array['base_fee'], array[(cast(base_fee_per_gas as uint256) * cast(txns.gas_used as uint256))])
                    else map(array['base_fee','priority_fee'],
                             array[(cast(base_fee_per_gas as uint256) * cast(txns.gas_used as uint256))
                                    ,(cast(priority_fee_per_gas as uint256) * cast(txns.gas_used as uint256))]
                             )
                    ) end
        as tx_fee_breakdown_raw
        ,{%- if has_fee_currency(blockchain) -%}
          coalesce(txns.fee_currency, {{var('ETH_ERC20_ADDRESS')}}) {%- else -%} {{var('ETH_ERC20_ADDRESS')}}
        {%- endif %} as tx_fee_currency
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
    ,p.symbol as currency_symbol
    ,coalesce(tx_fee_raw, 0) as tx_fee_raw
    ,coalesce(tx_fee_raw, 0) / pow(10,p.decimals) as tx_fee
    ,coalesce(tx_fee_raw, 0) / pow(10,p.decimals) * p.price as tx_fee_usd
    ,transform_values(tx_fee_breakdown_raw,
            (k,v) -> coalesce(v,0)) as tx_fee_breakdown_raw
    ,transform_values(tx_fee_breakdown_raw,
            (k,v) -> coalesce(v, 0) / pow(10,p.decimals) ) as tx_fee_breakdown
    ,transform_values(tx_fee_breakdown_raw,
            (k,v) -> coalesce(v, 0) / pow(10,p.decimals) * p.price) as tx_fee_breakdown_usd
    ,tx_fee_currency
    ,block_proposer
    ,max_fee_per_gas
    ,priority_fee_per_gas
    ,max_priority_fee_per_gas
    ,base_fee_per_gas
    ,gas_limit
    ,gas_limit_usage
FROM base_model
LEFT JOIN {{ref('prices_usd_with_native')}} p
    ON p.blockchain = '{{blockchain}}'
    AND p.contract_address = tx_fee_currency
    AND p.minute = date_trunc('minute', block_time)
{% endmacro %}
