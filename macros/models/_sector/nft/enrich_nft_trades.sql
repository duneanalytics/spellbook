{% macro enrich_nft_trades(base_trades) %}
-- Macro to apply the NFT trades enrichment(s) to base models
-- 1. add NFT token information
-- 2. add ERC20 token information + handle ERC20 decimals
-- 3. handle USD columns
-- 4. add aggregator columns
-- 5. fix buyer or seller for aggregator txs
-- 6. calculate platform and royalty rates

-- TODO: We should remove this CTE and include ETH into the general prices table once everything is migrated
WITH prices_patch as (
    SELECT
        contract_address
        ,blockchain
        ,decimals
        ,minute
        ,price
        ,symbol
    FROM {{ ref('prices_usd_forward_fill') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('minute')}}
    {% endif %}
    UNION ALL
    SELECT
        {{ var("ETH_ERC20_ADDRESS") }} as contract_address
        ,'ethereum' as blockchain
        ,18 as decimals
        ,minute
        ,price
        ,'ETH' as symbol
    FROM {{ ref('prices_usd_forward_fill') }}
    WHERE blockchain is null AND symbol = 'ETH'
    {% if is_incremental() %}
    AND {{incremental_predicate('minute')}}
    {% endif %}
    UNION ALL
    SELECT
        {{ var("ETH_ERC20_ADDRESS") }} as contract_address
        ,'base' as blockchain
        ,18 as decimals
        ,minute
        ,price
        ,'ETH' as symbol
    FROM {{ ref('prices_usd_forward_fill') }}
    WHERE blockchain is null AND symbol = 'ETH'
    {% if is_incremental() %}
    AND {{incremental_predicate('minute')}}
    {% endif %}
    UNION ALL
    SELECT
        {{ var("ETH_ERC20_ADDRESS") }} as contract_address
        ,'arbitrum' as blockchain
        ,18 as decimals
        ,minute
        ,price
        ,'ETH' as symbol
    FROM {{ ref('prices_usd_forward_fill') }}
    WHERE blockchain is null AND symbol = 'ETH'
    {% if is_incremental() %}
    AND {{incremental_predicate('minute')}}
    {% endif %}
),
enrichments as (
SELECT
    base.blockchain,
    base.project,
    base.project_version,
    base.block_date,
    base.block_month,
    base.block_time,
    base.block_number,
    base.tx_hash,
    base.sub_tx_trade_id,
    base.project_contract_address,
    base.trade_category,
    base.trade_type,
    case when base.buyer = coalesce(agg1.contract_address,agg2.contract_address) then base.tx_from else base.buyer end as buyer,
    case when base.seller = coalesce(agg1.contract_address,agg2.contract_address) then base.tx_from else base.seller end as seller,
    base.nft_contract_address,
    base.nft_token_id,
    base.nft_amount,
    base.price_raw,
    base.currency_contract,
    base.platform_fee_amount_raw,
    base.royalty_fee_amount_raw,
    base.platform_fee_address,
    base.royalty_fee_address,
    base.tx_from as tx_from,
    base.tx_to as tx_to,
    nft.name as nft_collection,
    nft.standard as nft_standard,
    coalesce(erc20.symbol,p.symbol) as currency_symbol,
    base.price_raw/pow(10,coalesce(erc20.decimals,p.decimals,18)) as price,
    base.platform_fee_amount_raw/pow(10,coalesce(erc20.decimals,p.decimals,18)) as platform_fee_amount,
    base.royalty_fee_amount_raw/pow(10,coalesce(erc20.decimals,p.decimals,18)) as royalty_fee_amount,
    base.price_raw/pow(10,coalesce(erc20.decimals,p.decimals,18))*p.price as price_usd,
    base.platform_fee_amount_raw/pow(10,coalesce(erc20.decimals,p.decimals,18))*p.price as platform_fee_amount_usd,
    base.royalty_fee_amount_raw/pow(10,coalesce(erc20.decimals,p.decimals,18))*p.price as royalty_fee_amount_usd,
    case when base.price_raw > uint256 '0' then cast(100*base.platform_fee_amount_raw/base.price_raw as double) else double '0' end as platform_fee_percentage,
    case when base.price_raw > uint256 '0' then cast(100*base.royalty_fee_amount_raw/base.price_raw as double) else double '0' end as royalty_fee_percentage,
    coalesce(agg1.contract_address,agg2.contract_address) as aggregator_address,
    {% if aggregator_markers != null %}
    CASE WHEN coalesce(agg_mark.aggregator_name, agg1.name, agg2.name)='Gem' AND base.block_number >= 16971894 THEN 'OpenSea Pro' -- 16971894 is the first block of 2023-04-04 which is when Gem rebranded to OpenSea Pro
        ELSE coalesce(agg_mark.aggregator_name, agg1.name, agg2.name)
        END as aggregator_name
    {% else %}
    coalesce(agg1.name,agg2.name) as aggregator_name
    {% endif %}
FROM {{base_trades}} base
LEFT JOIN {{ref('tokens_nft')}} nft
    ON nft.blockchain = base.blockchain
    AND nft.contract_address = base.nft_contract_address
LEFT JOIN {{ source('tokens', 'erc20') }} erc20
    ON erc20.blockchain = base.blockchain
    AND erc20.contract_address = base.currency_contract
LEFT JOIN prices_patch p
    ON p.blockchain = base.blockchain
    AND p.contract_address = base.currency_contract
    AND p.minute = date_trunc('minute',base.block_time)
LEFT JOIN {{ ref('nft_aggregators') }} agg1
    ON agg1.blockchain = base.blockchain
    AND (base.buyer = agg1.contract_address
        OR base.seller = agg1.contract_address)
LEFT JOIN {{ ref('nft_aggregators') }} agg2
    ON agg1.contract_address is null    -- only match if agg1 produces no matches, this prevents duplicates
    AND agg2.blockchain = base.blockchain
    AND tx_to = agg2.contract_address
LEFT JOIN {{ ref('nft_ethereum_aggregators_markers') }} agg_mark
    ON bytearray_starts_with(bytearray_reverse(base.tx_data_marker), bytearray_reverse(agg_mark.hash_marker)) -- eq to end_with()

{% if is_incremental() %}
WHERE {{incremental_predicate('base.block_time')}}
{% endif %}
)

select * from enrichments
{% endmacro %}
