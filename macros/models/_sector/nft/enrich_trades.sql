{% macro enrich_trades(blockchain='', models=[], transactions_model=null, tokens_nft_model=null, tokens_erc20_model=null, prices_model=null, aggregators=null, aggregator_markers=null) %}
-- Macro to apply the NFT trades enrichment(s) to base models
-- 1. add transaction information
-- 2. add NFT token information
-- 3. add ERC20 token information + handle ERC20 decimals
-- 4. handle USD columns
-- 5. add aggregator columns
-- 6. fix buyer or seller for aggregator txs
-- 7. calculate platform and royalty rates
-- 8. deduplicate based on sub_tx_trade_id

WITH base_union as (
    {% for nft_model in models %}
    SELECT
        '{{ blockchain }}' as blockchain,
        '{{ nft_model[0] }}' as project,
        '{{ nft_model[1] }}' as project_version,
        block_date,
        cast(date_trunc('month', block_time) as date) as block_month,
        block_time,
        block_number,
        tx_hash,
        project_contract_address,
        trade_category,                 --buy/sell/swap
        trade_type,                     --primary/secondary
        buyer,
        seller,
        nft_contract_address,
        nft_token_id,
        nft_amount,                -- always 1 for erc721
        price_raw,
        currency_contract,
        platform_fee_amount_raw,
        royalty_fee_amount_raw,
        platform_fee_address,   -- optional
        royalty_fee_address,    -- optional
        sub_tx_trade_id,
        row_number() over (partition by tx_hash, sub_tx_trade_id order by tx_hash) as duplicates_rank
    FROM {{ nft_model[2] }}
    {% if is_incremental() %}
    WHERE block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
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
    case when base.buyer = coalesce(agg1.contract_address,agg2.contract_address) then tx."from" else base.buyer end as buyer,
    case when base.seller = coalesce(agg1.contract_address,agg2.contract_address) then tx."from" else base.seller end as seller,
    base.nft_contract_address,
    base.nft_token_id,
    base.nft_amount,
    base.price_raw,
    base.currency_contract,
    base.platform_fee_amount_raw,
    base.royalty_fee_amount_raw,
    base.platform_fee_address,
    base.royalty_fee_address,
    tx."from" as tx_from,
    tx.to as tx_to,
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
FROM base_union base
INNER JOIN {{ transactions_model }} tx
ON tx.block_number = base.block_number
    AND tx.hash = base.tx_hash
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ tokens_nft_model }} nft
ON nft.contract_address = base.nft_contract_address
LEFT JOIN {{ tokens_erc20_model }} erc20
ON erc20.contract_address = base.currency_contract
LEFT JOIN {{ prices_model }} p
ON p.blockchain = base.blockchain
    AND p.contract_address = base.currency_contract
    AND p.minute = date_trunc('minute',base.block_time)
    {% if is_incremental() %}
    AND p.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ aggregators }} agg1
ON (base.buyer = agg1.contract_address
    OR base.seller = agg1.contract_address)
LEFT JOIN {{ aggregators }} agg2
ON agg1.contract_address is null    -- only match if agg1 produces no matches, this prevents duplicates
    AND tx.to = agg2.contract_address
{% if aggregator_markers != null %}
LEFT JOIN {{ aggregator_markers }} agg_mark
ON bytearray_starts_with(bytearray_reverse(tx.data), bytearray_reverse(agg_mark.hash_marker)) -- eq to end_with()
WHERE base.duplicates_rank = 1
{% endif %}
)


select * from enrichments
{% endmacro %}
