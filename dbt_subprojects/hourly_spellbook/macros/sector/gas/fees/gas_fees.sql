-- any modifications needed for getting the correct gas price
{% macro gas_price(blockchain) %}
    {%- if blockchain in ['arbitrum', 'nova', 'corn']-%}
    effective_gas_price
    {%- elif blockchain in ['abstract']-%}
    gas_price  -- L2 gas price for abstract
    {%- elif blockchain in ['boba']-%}
    gas_price  -- Uses standard gas_price but updates every 10 minutes based on L1 prices
    {%- elif blockchain in ['ink', 'worldchain']-%}
    gas_price  -- Standard gas price for OP Stack L2s
    {%- elif blockchain in ['flare']-%}
    gas_price  -- Standard Ethereum-compatible gas price model
    {%- else -%}
    gas_price
    {%- endif -%}
{% endmacro %}

-- include chain specific logic here
{% macro tx_fee_raw(blockchain) %}
    {%- if blockchain in all_op_chains() + ('scroll','blast','mantle') -%}
    cast(coalesce(l1_fee,0) as uint256) +
    {%- endif -%}
    {%- if blockchain in ('ethereum',) -%}
    cast(coalesce(blob.blob_base_fee,0) as uint256) * cast(coalesce(blob.blob_gas_used,0) as uint256) +
    {%- endif -%}
    {%- if blockchain in ('abstract',) -%}
    -- For Abstract, the raw transaction fee consists of both L2 execution gas and batch overhead
    -- Since we might not have direct access to batch overhead data from the chain
    -- we'll use the gas_price (L2 gas price) and gas_used similar to other chains,
    -- but recognize that this might underestimate the actual fee that includes the batch overhead component
    cast({{ gas_price(blockchain) }} as uint256) * cast(txns.gas_used as uint256)
    {%- elif blockchain in ('boba',) -%}
    -- For Boba, we use the standard gas price calculation similar to other Optimistic Rollups
    -- Gas price updates every 10 minutes based on L1 with maximium 5% change per update
    -- Boba may also have L1 data costs which would ideally be included, but the l1_fee data might not be directly accessible
    cast({{ gas_price(blockchain) }} as uint256) * cast(txns.gas_used as uint256)
    {%- elif blockchain in ('ink', 'worldchain') -%}
    -- For Ink and Worldchain, as OP Stack chains, they follow the standard Optimistic Rollup fee structure
    -- with L1 data fees and L2 execution fees. The l1_fee is already included above.
    cast({{ gas_price(blockchain) }} as uint256) * cast(txns.gas_used as uint256)
    {%- elif blockchain in ('nova',) -%}
    -- For Arbitrum Nova (AnyTrust), gas fees are significantly lower than Arbitrum One
    -- due to the Data Availability Committee (DAC) which reduces L1 data costs
    -- Nova fees still include both L1 and L2 components, but L1 costs are much lower
    cast({{ gas_price(blockchain) }} as uint256) * cast(txns.gas_used as uint256)
    {%- elif blockchain in ('corn',) -%}
    -- For Arbitrum Corn (Orbit), fees follow the Arbitrum Orbit model with four components:
    -- l2BaseFee, l2SurplusFee, l1BaseFee, and l1SurplusFee
    -- Since these components may not be directly accessible in the data, we use the effective gas price
    cast({{ gas_price(blockchain) }} as uint256) * cast(txns.gas_used as uint256)
    {%- elif blockchain in ('flare',) -%}
    -- For Flare, we use the standard Ethereum-compatible gas fee calculation
    -- Flare uses a similar gas model to Ethereum but with lower base costs
    cast({{ gas_price(blockchain) }} as uint256) * cast(txns.gas_used as uint256)
    {%- else -%}
    cast({{ gas_price(blockchain) }} as uint256) * cast(txns.gas_used as uint256)
    {%- endif -%}
{% endmacro %}

-- include chain specific logic here
-- arbitrum is a bit special as they have eip1559 but ignore tips (priority fees)
-- https://docs.arbitrum.io/how-arbitrum-works/gas-fees#tips-in-l2
-- zksync doesn't provide an easy way to track which part of the fee is L1 fee and which is the L2 base fee
{% macro tx_fee_breakdown_raw(blockchain) %}
    map_concat(
    map()
    {%- if blockchain in ('arbitrum',) %}
      ,map(array['l1_fee','base_fee']
        , array[cast(coalesce(gas_used_for_l1,0) * {{gas_price(blockchain)}} as uint256)
                ,cast((txns.gas_used - coalesce(gas_used_for_l1,0)) * {{gas_price(blockchain)}} as uint256)])
    {%- elif blockchain in ('nova',) %}
      -- For Arbitrum Nova (AnyTrust), the fee breakdown is similar to Arbitrum One
      -- but with reduced L1 costs due to the Data Availability Committee
      ,map(array['l1_fee','base_fee']
        , array[cast(coalesce(gas_used_for_l1,0) * {{gas_price(blockchain)}} as uint256)
                ,cast((txns.gas_used - coalesce(gas_used_for_l1,0)) * {{gas_price(blockchain)}} as uint256)])
    {%- elif blockchain in ('corn',) %}
      -- For Arbitrum Corn (Orbit), the fee breakdown ideally would include the four components:
      -- l2BaseFee, l2SurplusFee, l1BaseFee, and l1SurplusFee
      -- Since these components may not be directly accessible, we use a simplified breakdown similar to Arbitrum
      ,map(array['l1_fee','base_fee']
        , array[cast(coalesce(gas_used_for_l1,0) * {{gas_price(blockchain)}} as uint256)
                ,cast((txns.gas_used - coalesce(gas_used_for_l1,0)) * {{gas_price(blockchain)}} as uint256)])
    {%- elif blockchain in ('zksync',) %}
      ,map(array['base_fee'], array[(cast({{gas_price(blockchain)}} as uint256) * cast(txns.gas_used as uint256))])
    {%- elif blockchain in ('celo',) %}
        ,case when txns.priority_fee_per_gas is null or txns.priority_fee_per_gas < 0
                then map(array['base_fee'], array[(cast({{gas_price(blockchain)}} as uint256) * cast(txns.gas_used as uint256))])
                else map(array['base_fee','priority_fee'],
                         array[(cast(gas_price - priority_fee_per_gas as uint256) * cast(txns.gas_used as uint256))
                                ,(cast(priority_fee_per_gas as uint256) * cast(txns.gas_used as uint256))]
                         )
                end
    {%- elif blockchain in ('abstract',) %}
      -- For Abstract, we ideally would break down into L2 execution gas and batch overhead
      -- Since we don't have direct access to these components, we'll approximate with base_fee (L2 execution)
      ,map(array['base_fee'], array[(cast({{gas_price(blockchain)}} as uint256) * cast(txns.gas_used as uint256))])
    {%- elif blockchain in ('boba',) %}
      -- For Boba, we ideally would break down into L2 execution and L1 data costs
      -- Based on Optimism design which Boba is built on
      -- We simplify the breakdown to a base_fee component
      ,case when txns.priority_fee_per_gas is null or txns.priority_fee_per_gas < 0
              then map(array['base_fee'], array[(cast({{gas_price(blockchain)}} as uint256) * cast(txns.gas_used as uint256))])
              else map(array['base_fee','priority_fee'],
                       array[(cast(base_fee_per_gas as uint256) * cast(txns.gas_used as uint256))
                              ,(cast(priority_fee_per_gas as uint256) * cast(txns.gas_used as uint256))]
                       )
              end
    {%- elif blockchain in ('ink', 'worldchain') %}
      -- For Ink and Worldchain as OP Stack chains
      -- We include l1_fee from above, and handle base_fee/priority_fee breakdown similar to other EIP-1559 chains
      ,case when txns.priority_fee_per_gas is null or txns.priority_fee_per_gas < 0
              then map(array['base_fee'], array[(cast({{gas_price(blockchain)}} as uint256) * cast(txns.gas_used as uint256))])
              else map(array['base_fee','priority_fee'],
                       array[(cast(base_fee_per_gas as uint256) * cast(txns.gas_used as uint256))
                              ,(cast(priority_fee_per_gas as uint256) * cast(txns.gas_used as uint256))]
                       )
              end
    {%- elif blockchain in ('flare',) %}
      -- For Flare, use standard Ethereum-compatible fee breakdown
      -- Supporting both legacy transactions and EIP-1559 style transactions
      ,case when txns.priority_fee_per_gas is null or txns.priority_fee_per_gas < 0
              then map(array['base_fee'], array[(cast({{gas_price(blockchain)}} as uint256) * cast(txns.gas_used as uint256))])
              else map(array['base_fee','priority_fee'],
                       array[(cast(base_fee_per_gas as uint256) * cast(txns.gas_used as uint256))
                              ,(cast(priority_fee_per_gas as uint256) * cast(txns.gas_used as uint256))]
                       )
              end
    {%- else -%}
        {%- if blockchain in all_op_chains() + ('scroll','blast','mantle') %}
          ,map(array['l1_fee'], array[cast(coalesce(l1_fee,0) as uint256)])
        {%- endif -%}
        {%- if blockchain in ('ethereum',) %}
          ,map(array['blob_fee'],array[cast(coalesce(blob.blob_base_fee,0) as uint256) * cast(coalesce(blob.blob_gas_used,0) as uint256)])
        {%- endif %}
          ,case when txns.priority_fee_per_gas is null or txns.priority_fee_per_gas < 0
                then map(array['base_fee'], array[(cast({{gas_price(blockchain)}} as uint256) * cast(txns.gas_used as uint256))])
                else map(array['base_fee','priority_fee'],
                         array[(cast(base_fee_per_gas as uint256) * cast(txns.gas_used as uint256))
                                ,(cast(priority_fee_per_gas as uint256) * cast(txns.gas_used as uint256))]
                         )
                end
    {%- endif -%}
    )
{% endmacro %}

-- include chain specific columns here
{% macro select_extra_columns(blockchain, include_ref = true) %}
    {%- if blockchain in ('scroll') %}
      ,l1_fee
    {%- endif %}
    {%- if blockchain in all_op_chains() + ('blast','mantle') %}
      ,l1_fee
      ,l1_gas_used
      ,l1_gas_price
      ,l1_fee_scalar
    {%- endif %}
    {%- if blockchain in ('arbitrum',) %}
      ,effective_gas_price
      ,gas_used_for_l1
    {%- endif %}
    {%- if blockchain in ('ethereum',) %}
      ,{%- if include_ref %}blob.{%- endif %}blob_base_fee
      ,{%- if include_ref %}blob.{%- endif %}blob_gas_used
      ,{%- if include_ref %}blob.{%- endif %}max_fee_per_blob_gas
    {%- endif %}
{% endmacro %}

-- applicable on Celo
{% macro fee_currency(blockchain) %}
    {%- if blockchain in ('celo',) -%}
    coalesce(fee_currency, {{var('ETH_ERC20_ADDRESS')}})
    {%- else -%}
    {{var('ETH_ERC20_ADDRESS')}}
    {%- endif %}
{% endmacro %}

{% macro gas_fees(blockchain) %}
-- Used to run the models only on incremental timeframe + seed transactions (for tests)
{% set test_short_ci = false %}
WITH base_model as (
    SELECT
        txns.block_time
        ,txns.block_number
        ,txns.hash AS tx_hash
        ,txns."from" AS tx_from
        ,txns.to AS tx_to
        ,cast({{ gas_price(blockchain) }} as uint256) as gas_price
        ,txns.gas_used as gas_used
        ,{{tx_fee_raw(blockchain) }} as tx_fee_raw
        ,{{tx_fee_breakdown_raw(blockchain)}} as tx_fee_breakdown_raw
        ,{{fee_currency(blockchain)}} as tx_fee_currency
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
        {{ select_extra_columns(blockchain) }}
    FROM {{ source( blockchain, 'transactions') }} txns
    INNER JOIN {{ source( blockchain, 'blocks') }} blocks
        ON txns.block_number = blocks.number
        {% if is_incremental() %}
        AND {{ incremental_predicate('blocks.time') }}
        {% endif %}
    {%- if blockchain in ('ethereum',)-%}
    LEFT JOIN {{ source( blockchain, 'blobs_submissions') }} blob
        ON txns.hash = blob.tx_hash
        AND txns.block_number = blob.block_number
        {% if is_incremental() %}
        AND {{ incremental_predicate('blob.block_time') }}
        {% endif %}
    {%- endif -%}
    {% if test_short_ci %}
    WHERE {{ incremental_predicate('txns.block_time') }}
    OR txns.hash in (select tx_hash from {{ref('evm_gas_fees')}})
    {% elif is_incremental() %}
    WHERE {{ incremental_predicate('txns.block_time') }}
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
    {{ select_extra_columns(blockchain=blockchain, include_ref=false) }}
FROM base_model
LEFT JOIN {{ref('prices_usd_with_native')}} p
    ON p.blockchain = '{{blockchain}}'
    AND p.contract_address = tx_fee_currency
    AND p.minute = date_trunc('minute', block_time)
{% endmacro %}
