{% macro enrichments(blockchain='', models=[], transactions_model=null, tokens_nft_model=null, tokens_erc20_model=null, prices_model=null, aggregators=null, aggregator_markers=null) %}
-- Macro to apply the NFT trades enrichment(s) to base models
-- 1. add transaction information
-- 2. add NFT token information
-- 3. add ERC20 token information + handle ERC20 decimals
-- 4. handle USD columns
-- 5. add aggregator columns
-- 6. fix buyer or seller for aggregator tx
-- 7. calculate platform and royalty rates

WITH base_union as (
    {% for nft_model in models %}
    SELECT
        '{{ blockchain }}' as blockchain,
        '{{ nft_model[0] }}' as project,
        '{{ nft_model[1] }}' as project_version,
        block_date,
        block_number,
        tx_hash,
        project_contract_address,
        trade_category,                 --buy/sell/swap
        trade_type,                     --primary/secondary
        buyer,
        seller,
        nft_contract_address,
        nft_token_id,
        number_of_items,                -- always 1 for erc721
        amount_raw,
        currency_contract,
        platform_fee_amount_raw,
        royalty_fee_amount_raw,
        platform_fee_receive_address,   -- optional
        royalty_fee_receive_address,    -- optional
        sub_tx_trade_id
    FROM {{ nft_model[2] }}
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
    base.block_number,
    base.tx_hash,
    base.sub_tx_trade_id,
    base.project_contract_address,
    base.trade_category,
    base.trade_type,
    case when base.buyer != agg.contract_address then base.buyer else tx.from end as buyer,
    case when base.seller != agg.contract_address then base.seller else tx.from end as seller,
    base.nft_contract_address,
    base.nft_token_id,
    base.number_of_items,
    base.amount_raw,
    base.currency_contract,
    base.platform_fee_amount_raw,
    base.royalty_fee_amount_raw,
    base.platform_fee_receive_address,
    base.royalty_fee_receive_address,
    tx.block_time,
    tx.from as tx_from,
    tx.to as tx_to,
    nft.name as collection,
    nft.standard as token_standard,
    erc20.symbol as currency_symbol,
    base.amount_raw/pow(10,coalesce(erc20.decimals,18)) as amount_original,
    base.platform_fee_amount_raw/pow(10,coalesce(erc20.decimals,18)) as platform_fee_amount,
    base.royalty_fee_amount_raw/pow(10,coalesce(erc20.decimals,18)) as royalty_fee_amount,
    base.amount_raw/pow(10,coalesce(erc20.decimals,18))*p.price as amount_usd,
    base.platform_fee_amount_raw/pow(10,coalesce(erc20.decimals,18))*p.price as platform_fee_amount_usd,
    base.royalty_fee_amount_raw/pow(10,coalesce(erc20.decimals,18))*p.price as royalty_fee_amount_usd,
    agg.contract_address as aggregator_address,
    {% if aggregator_markers != null %}
    coalesce(agg_mark.aggregator_name, agg.name) as aggregator_name
    {% else %}
    agg.name as aggregator_name
    {% endif %}
FROM base_union base
INNER JOIN {{ transactions_model }} tx
ON tx.block_number = base.block_number
    AND tx.hash = base.tx_hash
INNER JOIN {{ tokens_nft_model }} nft
ON nft.contract_address = base.nft_contract_address
LEFT JOIN {{ tokens_erc20_model }} erc20
ON erc20.contract_address = base.currency_contract
LEFT JOIN {{ prices_model }} p
ON p.blockchain = base.blockchain
    AND p.contract_address = base.currency_contract
    AND p.minute = date_trunc('minute',tx.block_time)
LEFT JOIN {{ aggregators }} agg
ON tx.to = agg.contract_address
    OR base.buyer = agg.contract_address
    OR base.seller = agg.contract_address
{% if aggregator_markers != null %}
LEFT JOIN {{ aggregator_markers }} agg_mark
ON RIGHT(tx.data, agg_mark.hash_marker_size) = agg_mark.hash_marker
{% endif %}
)
select * from enrichments
{% endmacro %}
