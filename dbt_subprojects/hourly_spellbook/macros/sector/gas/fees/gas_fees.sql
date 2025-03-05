-- any modifications needed for getting the correct gas price
{% macro gas_price(blockchain) %}
    {%- if blockchain in ['arbitrum']-%}
    effective_gas_price
    {%- elif blockchain in ['nova', 'corn']-%}
    gas_price
    {%- elif blockchain in ['abstract']-%}
    gas_price  -- L2 gas price for abstract
    {%- elif blockchain in ['boba']-%}
    gas_price  -- Uses standard gas_price but updates every 10 minutes based on L1 prices
    {%- elif blockchain in ['ink', 'worldchain']-%}
    gas_price  -- Standard gas price for OP Stack L2s
    {%- elif blockchain in ['flare']-%}
    gas_price  -- Standard Ethereum-compatible gas price model
    {%- elif blockchain in ['zksync']-%}
    gas_price  -- zkSync uses a standard gas price model, but with unique L1/L2 component costs
    {%- elif blockchain in ['linea']-%}
    gas_price  -- Linea uses a standard gas price model with L1/L2 component costs (ZK rollup)
    {%- elif blockchain in ['sei']-%}
    gas_price  -- Sei uses a dual gas price system optimized for trading applications
    {%- elif blockchain in ['kaia']-%}
    gas_price  -- Kaia uses an EIP-1559 style dynamic fee model with base fee and priority fee
    {%- elif blockchain in ['sonic']-%}
    gas_price  -- Sonic uses a standard gas price model with a specialized fee distribution system
    {%- elif blockchain in ['viction']-%}
    gas_price  -- Viction uses an innovative Zero-Gas mechanism with VRC25 token standard
    {%- elif blockchain in ['sophon']-%}
    gas_price  -- Sophon is a Validium-based L2 with ZK rollup characteristics
    {%- elif blockchain in ['apechain']-%}
    gas_price  -- Apechain is a Layer-3 blockchain using APE token for gas
    {%- elif blockchain in ['berachain']-%}
    gas_price  -- Berachain uses a standard EVM-compatible gas price model
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
      -- For Arbitrum Nova (AnyTrust), the fee breakdown is simplified since we don't have access to specific components
      -- Nova's actual fee structure includes:
      -- 1. L1 Data Costs: Lower fees due to the DAC model that only posts small proofs to L1
      -- 2. L2 Execution Costs: Fees for computation on Nova chain itself
      -- 3. Fallback Mechanism: For when DAC fails to make data available
      -- Since we don't have direct access to these components, we use a simplified approach
      ,map(array['base_fee'], array[(cast({{gas_price(blockchain)}} as uint256) * cast(txns.gas_used as uint256))])
    {%- elif blockchain in ('corn',) -%}
      -- For Arbitrum Corn (Orbit), the fee breakdown should ideally include four components:
      -- 1. l2BaseFee: Fees for execution on the Orbit chain itself
      -- 2. l2SurplusFee: Additional fees during network congestion
      -- 3. l1BaseFee: Fees for posting transactions to the parent chain
      -- 4. l1SurplusFee: Extra fees for batch posters
      -- Since we don't have direct access to these components, we use a simplified approach
      ,map(array['base_fee'], array[(cast({{gas_price(blockchain)}} as uint256) * cast(txns.gas_used as uint256))])
    {%- elif blockchain in ('flare',) -%}
    -- For Flare, we use the standard Ethereum-compatible gas fee calculation
    -- Flare uses a similar gas model to Ethereum but with lower base costs
    cast({{ gas_price(blockchain) }} as uint256) * cast(txns.gas_used as uint256)
    {%- elif blockchain in ('zksync',) -%}
    -- For zkSync, the transaction fee consists of two main components:
    -- 1. L1 Data Availability Costs: Fees for posting data to Ethereum
    -- 2. L2 Computation Costs: Fees for execution on zkSync itself
    -- The total gas cost combines these components, but we might not have direct access
    -- to the separate L1/L2 components in raw transaction data
    cast({{ gas_price(blockchain) }} as uint256) * cast(txns.gas_used as uint256)
    {%- elif blockchain in ('linea',) -%}
    -- For Linea, as a Type 2 ZK rollup, the transaction fee consists of two main components:
    -- 1. L1 Data Availability Costs: Fees for posting transaction data and proofs to Ethereum
    -- 2. L2 Execution Costs: Fees for computation on the Linea network
    -- The gas price model follows EIP-1559 with base fee and optional priority fee
    -- L1 costs are amortized across batches through efficient ZK proving systems
    cast({{ gas_price(blockchain) }} as uint256) * cast(txns.gas_used as uint256)
    {%- elif blockchain in ('sei',) -%}
    -- For Sei, the transaction fee uses a dual gas price system:
    -- 1. Base Gas Price: Set by the protocol and adjusted based on network demand
    -- 2. Priority Gas Price: Optional additional fee for transaction prioritization
    -- Sei's parallel execution architecture enables higher throughput and generally lower gas costs
    -- compared to sequential execution blockchains
    cast({{ gas_price(blockchain) }} as uint256) * cast(txns.gas_used as uint256)
    {%- elif blockchain in ('kaia',) -%}
    -- For Kaia, the transaction fee consists of two main components:
    -- 1. Base Fee: Dynamically adjusted based on network congestion (KIP-71)
    -- 2. Priority Fee: Optional fee for transaction prioritization (KIP-162)
    -- Half of the gas fee is burned since Magma hardfork, and most is burned since Kore hardfork
    cast({{ gas_price(blockchain) }} as uint256) * cast(txns.gas_used as uint256)
    {%- elif blockchain in ('sonic',) -%}
    -- For Sonic, the transaction fee uses a standard gas price model but with a unique distribution:
    -- - 50% is burned (for non-FeeM transactions)
    -- - 45% goes to validators (10% for FeeM transactions)
    -- - 5% goes to Ecosystem Vault (0% for FeeM transactions)
    -- - 0-90% can go to app developers (FeeM program)
    cast({{ gas_price(blockchain) }} as uint256) * cast(txns.gas_used as uint256)
    {%- elif blockchain in ('viction',) -%}
    -- For Viction, the transaction fee can be zero for users through the innovative Zero-Gas mechanism:
    -- - VRC25 token standard enables developers to cover gas fees for their users
    -- - Gas fees are paid by token owners who deposit VIC to the VRC25Issuer contract
    -- - Internal calls to VRC25 tokens are not gas-sponsored
    cast({{ gas_price(blockchain) }} as uint256) * cast(txns.gas_used as uint256)
    {%- elif blockchain in ('sophon',) -%}
    -- For Sophon, as a Validium-based L2 using ZKsync's Elastic Chain:
    -- - Gas fees are paid in SOPH token
    -- - Fees may include costs for computation and data availability
    -- - Uses ZK proofs for security and efficiency
    cast({{ gas_price(blockchain) }} as uint256) * cast(txns.gas_used as uint256)
    {%- elif blockchain in ('apechain',) -%}
    -- For Apechain, a Layer-3 blockchain:
    -- - Gas fees are paid in APE token
    -- - Part of transaction fees are burned through Timeboost mechanism
    -- - Designed for entertainment use cases with optimized performance
    cast({{ gas_price(blockchain) }} as uint256) * cast(txns.gas_used as uint256)
    {%- elif blockchain in ('berachain',) -%}
    -- For Berachain:
    -- - Gas fees follow standard EVM calculations
    -- - Based on Cosmos SDK with potential for unique consensus mechanisms
    -- - May include validator incentives in fee structure
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
      -- For Arbitrum Nova (AnyTrust), the fee breakdown is simplified since we don't have access to specific components
      -- Nova's actual fee structure includes:
      -- 1. L1 Data Costs: Lower fees due to the DAC model that only posts small proofs to L1
      -- 2. L2 Execution Costs: Fees for computation on Nova chain itself
      -- 3. Fallback Mechanism: For when DAC fails to make data available
      -- Since we don't have direct access to these components, we use a simplified approach
      ,map(array['base_fee'], array[(cast({{gas_price(blockchain)}} as uint256) * cast(txns.gas_used as uint256))])
    {%- elif blockchain in ('corn',) %}
      -- For Arbitrum Corn (Orbit), the fee breakdown should ideally include four components:
      -- 1. l2BaseFee: Fees for execution on the Orbit chain itself
      -- 2. l2SurplusFee: Additional fees during network congestion
      -- 3. l1BaseFee: Fees for posting transactions to the parent chain
      -- 4. l1SurplusFee: Extra fees for batch posters
      -- Since we don't have direct access to these components, we use a simplified approach
      ,map(array['base_fee'], array[(cast({{gas_price(blockchain)}} as uint256) * cast(txns.gas_used as uint256))])
    {%- elif blockchain in ('zksync',) %}
      -- For zkSync, ideally we would break down the fee into L1 data costs and L2 execution costs
      -- Since we don't have direct access to these components in the transaction data,
      -- we'll use a simple base_fee component for the total fee
      -- This is a limitation of the current implementation
      ,map(array['base_fee'], array[(cast({{gas_price(blockchain)}} as uint256) * cast(txns.gas_used as uint256))])
    {%- elif blockchain in ('linea',) %}
      -- For Linea, the fee breakdown ideally includes L1 data costs and L2 execution costs
      -- Since we don't have direct access to the separate components in the transaction data, 
      -- we use the EIP-1559 style breakdown for the total fee
      ,case when txns.priority_fee_per_gas is null or txns.priority_fee_per_gas < 0
              then map(array['base_fee'], array[(cast({{gas_price(blockchain)}} as uint256) * cast(txns.gas_used as uint256))])
              else map(array['base_fee','priority_fee'],
                       array[(cast(base_fee_per_gas as uint256) * cast(txns.gas_used as uint256))
                              ,(cast(priority_fee_per_gas as uint256) * cast(txns.gas_used as uint256))]
                       )
              end
    {%- elif blockchain in ('sei',) %}
        -- For Sei, the fee breakdown includes base gas price and optional priority gas price
        -- The parallel execution model optimizes for trading operations
        ,case when txns.priority_fee_per_gas is null or txns.priority_fee_per_gas < 0
                then map(array['base_fee'], array[(cast({{gas_price(blockchain)}} as uint256) * cast(txns.gas_used as uint256))])
                else map(array['base_fee','priority_fee'],
                         array[(cast(base_fee_per_gas as uint256) * cast(txns.gas_used as uint256))
                                ,(cast(priority_fee_per_gas as uint256) * cast(txns.gas_used as uint256))]
                         )
                end
    {%- elif blockchain in ('kaia',) %}
        -- For Kaia, the fee breakdown includes base fee and optional priority fee
        -- The network follows EIP-1559 style pricing with partial fee burning
        ,case when txns.priority_fee_per_gas is null or txns.priority_fee_per_gas < 0
                then map(array['base_fee'], array[(cast({{gas_price(blockchain)}} as uint256) * cast(txns.gas_used as uint256))])
                else map(array['base_fee','priority_fee'],
                         array[(cast(base_fee_per_gas as uint256) * cast(txns.gas_used as uint256))
                                ,(cast(priority_fee_per_gas as uint256) * cast(txns.gas_used as uint256))]
                         )
                end
    {%- elif blockchain in ('sonic',) %}
        -- For Sonic, the fee breakdown includes standard components with a specialized distribution system
        ,case when txns.priority_fee_per_gas is null or txns.priority_fee_per_gas < 0
                then map(array['base_fee'], array[(cast({{gas_price(blockchain)}} as uint256) * cast(txns.gas_used as uint256))])
                else map(array['base_fee','priority_fee'],
                         array[(cast(base_fee_per_gas as uint256) * cast(txns.gas_used as uint256))
                                ,(cast(priority_fee_per_gas as uint256) * cast(txns.gas_used as uint256))]
                         )
                end
    {%- elif blockchain in ('viction',) %}
        -- For Viction, the Zero-Gas mechanism may result in no fees for end users
        -- but the gas is still paid by token owners
        ,case when txns.priority_fee_per_gas is null or txns.priority_fee_per_gas < 0
                then map(array['base_fee'], array[(cast({{gas_price(blockchain)}} as uint256) * cast(txns.gas_used as uint256))])
                else map(array['base_fee','priority_fee'],
                         array[(cast(base_fee_per_gas as uint256) * cast(txns.gas_used as uint256))
                                ,(cast(priority_fee_per_gas as uint256) * cast(txns.gas_used as uint256))]
                         )
                end
    {%- elif blockchain in ('sophon',) %}
        -- For Sophon, the fee structure is based on the ZK rollup model
        -- with potential components for L1 data and L2 computation
        ,case when txns.priority_fee_per_gas is null or txns.priority_fee_per_gas < 0
                then map(array['base_fee'], array[(cast({{gas_price(blockchain)}} as uint256) * cast(txns.gas_used as uint256))])
                else map(array['base_fee','priority_fee'],
                         array[(cast(base_fee_per_gas as uint256) * cast(txns.gas_used as uint256))
                                ,(cast(priority_fee_per_gas as uint256) * cast(txns.gas_used as uint256))]
                         )
                end
    {%- elif blockchain in ('apechain',) %}
        -- For Apechain, the standard gas fee model with possible Timeboost burn mechanism
        ,case when txns.priority_fee_per_gas is null or txns.priority_fee_per_gas < 0
                then map(array['base_fee'], array[(cast({{gas_price(blockchain)}} as uint256) * cast(txns.gas_used as uint256))])
                else map(array['base_fee','priority_fee'],
                         array[(cast(base_fee_per_gas as uint256) * cast(txns.gas_used as uint256))
                                ,(cast(priority_fee_per_gas as uint256) * cast(txns.gas_used as uint256))]
                         )
                end
    {%- elif blockchain in ('berachain',) %}
        -- For Berachain, standard fee breakdown with potential Cosmos SDK influences
        ,case when txns.priority_fee_per_gas is null or txns.priority_fee_per_gas < 0
                then map(array['base_fee'], array[(cast({{gas_price(blockchain)}} as uint256) * cast(txns.gas_used as uint256))])
                else map(array['base_fee','priority_fee'],
                         array[(cast(base_fee_per_gas as uint256) * cast(txns.gas_used as uint256))
                                ,(cast(priority_fee_per_gas as uint256) * cast(txns.gas_used as uint256))]
                         )
                end
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
    {%- if blockchain in ('arbitrum') %}
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
    {% else %}
    WHERE txns.block_time > now() - interval '30' day
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
