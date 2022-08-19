{{ config(
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_trade_id']
    )
}}

WITH looks_rare AS (
        SELECT
        ask.evt_block_time AS block_time,
        ask.tokenId::string AS token_id,
        ask.amount AS number_of_items,
        taker AS seller,
        maker AS buyer,
        price AS price,
        roy.amount AS royalty_fee,
        roy.royaltyRecipient AS royalty_fee_receive_address,
        roy.currency AS royalty_fee_currency_symbol,
        CASE -- REPLACE `ETH` WITH `WETH` for ERC20 lookup later
            WHEN ask.currency = '0x0000000000000000000000000000000000000000' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE ask.currency
        END AS currency_contract,
        ask.currency AS currency_contract_original,
        ask.collection AS nft_contract_address,
        ask.contract_address AS contract_address,
        ask.evt_tx_hash AS tx_hash,
        ask.evt_block_number AS block_number,
        ask.evt_index AS evt_index,
        roy.evt_index as roy_event_index,
        CASE -- CATEGORIZE Collection Wide Offers Accepted 
            WHEN strategy = '0x86f909f70813cdb1bc733f4d97dc6b03b8e7e8f3' THEN 'Collection Offer Accepted'
            ELSE 'Offer Accepted' 
            END AS category    
    FROM {{ source('looksrare_ethereum','looksrareexchange_evt_takerask') }} ask
    LEFT JOIN {{ source('looksrare_ethereum','looksrareexchange_evt_royaltypayment') }} roy ON roy.evt_tx_hash = ask.evt_tx_hash
    AND ask.evt_index - 2 = roy.evt_index
     {% if is_incremental() %} -- this filter will only be applied on an incremental run 
     WHERE ask.evt_block_time >= (select max(block_time) from {{ this }}) 
     {% endif %}
                            UNION
    SELECT 
        bid.evt_block_time AS block_time,
        bid.tokenId::string AS token_id,
        bid.amount AS number_of_items,
        maker AS seller,
        taker AS buyer,
        price AS price,
        roy.amount AS royalty_fee,
        roy.royaltyRecipient AS royalty_fee_receive_address,
        roy.currency AS royalty_fee_currency_symbol,
       CASE -- REPLACE `ETH` WITH `WETH` for ERC20 lookup later
            WHEN bid.currency = '0x0000000000000000000000000000000000000000' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE bid.currency
        END AS currency_contract,
        bid.currency AS currency_contract_original,
        bid.collection AS nft_contract_address,
        bid.contract_address AS contract_address,
        bid.evt_tx_hash AS tx_hash,
        bid.evt_block_number AS block_number,
        bid.evt_index AS evt_index,
        roy.evt_index as roy_event_index,
        'Buy' as category
    FROM {{ source('looksrare_ethereum','looksrareexchange_evt_takerbid') }} bid
    LEFT JOIN {{ source('looksrare_ethereum','looksrareexchange_evt_royaltypayment') }} roy ON roy.evt_tx_hash = bid.evt_tx_hash
    AND roy.evt_index = bid.evt_index - 4
     {% if is_incremental() %} -- this filter will only be applied on an incremental run 
     WHERE bid.evt_block_time >= (select max(block_time) from {{ this }}) 
     {% endif %}
    ),

-- Get ERC721 AND ERC1155 transfer data for every trade TRANSACTION
erc_transfers as
(SELECT evt_tx_hash,
        contract_address,
        id::string as token_id_erc,
        cardinality(collect_list(value)) as count_erc,
        value as value_unique,
        CASE WHEN erc1155.from = '0x0000000000000000000000000000000000000000' THEN 'Mint'
        WHEN erc1155.to = '0x0000000000000000000000000000000000000000' 
        OR erc1155.to = '0x000000000000000000000000000000000000dead' THEN 'Burn' 
        ELSE 'Trade' END AS evt_type,
        evt_index
        FROM {{ source('erc1155_ethereum','evt_transfersingle') }} erc1155
        WHERE erc1155.evt_block_time > '2022-01-01'
        GROUP BY evt_tx_hash,value,id,evt_index, erc1155.from, erc1155.to, erc1155.contract_address
            UNION
SELECT evt_tx_hash,
        contract_address,
        tokenId::string as token_id_erc,
        COUNT(tokenId) as count_erc,
        NULL as value_unique,
        CASE WHEN erc721.from = '0x0000000000000000000000000000000000000000' THEN 'Mint'
        WHEN erc721.to = '0x0000000000000000000000000000000000000000' 
        OR erc721.to = '0x000000000000000000000000000000000000dead' THEN 'Burn' 
        ELSE 'Trade' END AS evt_type,
        evt_index
        FROM {{ source('erc721_ethereum','evt_transfer') }} erc721
        WHERE erc721.evt_block_time > '2022-01-01'
        GROUP BY evt_tx_hash,tokenId,evt_index, erc721.from, erc721.to, erc721.contract_address)

SELECT DISTINCT
    'ethereum' as blockchain,
    'looksrare' as project,
    'v1' as version,
    TRY_CAST(date_trunc('DAY', looks_rare.block_time) AS date) AS block_date,
    looks_rare.block_time,
    token_id,
    tokens.name AS collection,
    looks_rare.price / power(10,erc20.decimals) * p.price AS amount_usd,
    tokens.standard AS token_standard,
    CASE 
        WHEN agg.name is NULL AND erc.value_unique = 1 OR erc.count_erc = 1 THEN 'Single Item Trade'
        WHEN agg.name is NULL AND erc.value_unique > 1 OR erc.count_erc > 1 THEN 'Bundle Trade'
    ELSE 'Single Item Trade' END AS trade_type,
    -- Count number of items traded for different trade types and erc standards
    CASE WHEN agg.name is NULL AND erc.value_unique > 1 THEN erc.value_unique
        WHEN agg.name is NULL AND erc.value_unique is NULL AND erc.count_erc > 1 THEN erc.count_erc
        WHEN tokens.standard = 'erc1155' THEN erc.value_unique
        WHEN tokens.standard = 'erc721' THEN erc.count_erc
        ELSE COALESCE((SELECT
                count(1)::bigint cnt
            FROM {{ source('erc721_ethereum','evt_transfer') }} erc721
            WHERE erc721.evt_tx_hash = tx_hash
            ) +    
            (SELECT
                count(1)::bigint cnt
            FROM {{ source('erc1155_ethereum','evt_transfersingle') }} erc1155
            WHERE erc1155.evt_tx_hash = tx_hash
            ), 0) END AS number_of_items,
    looks_rare.category as trade_category,
    CASE WHEN evt_type is NULL THEN 'Other' ELSE evt_type END as evt_type,
    seller,
    buyer,
    looks_rare.price / power(10,erc20.decimals) AS amount_original,
    looks_rare.price AS amount_raw,
    CASE WHEN looks_rare.currency_contract_original = '0x0000000000000000000000000000000000000000' THEN 'ETH' ELSE erc20.symbol END AS currency_symbol,
    currency_contract,
    COALESCE(erc.contract_address, nft_contract_address) AS nft_contract_address,
    looks_rare.contract_address AS project_contract_address,
    agg.name AS aggregator_name,
    agg.contract_address AS aggregator_address,
    looks_rare.block_number,
    tx_hash,
    tx.from AS tx_from,
    tx.to AS tx_to,
    ROUND((2*(looks_rare.price)/100),7) as platform_fee_amount_raw,
    ROUND((2*(looks_rare.price / power(10,erc20.decimals))/100),7) platform_fee_amount,
    ROUND((2*(looks_rare.price / power(10,erc20.decimals) * p.price)/100),7) as  platform_fee_amount_usd,
    '2' as platform_fee_percentage,
    royalty_fee as royalty_fee_amount_raw,
    royalty_fee / power(10,erc20.decimals) as royalty_fee_amount,
    royalty_fee * p.price/ power(10,erc20.decimals) as royalty_fee_amount_usd,
    royalty_fee / looks_rare.price * 100 as royalty_fee_percentage,
    royalty_fee_receive_address,
    royalty_fee_currency_symbol,
    'looksrare' || '-' || tx_hash || '-' ||  token_id::string || '-' ||  seller::string || '-' || COALESCE(erc.contract_address, nft_contract_address) || '-' || looks_rare.evt_index::string || '-' || COALESCE(evt_type::string, 'Other')  || '-' || COALESCE(case when erc.value_unique::string is null then '0' ELSE '1' end, '1') as unique_trade_id
FROM looks_rare
INNER JOIN {{ source('ethereum','transactions') }} tx ON tx_hash = tx.hash
    {% if not is_incremental() %}
    AND tx.block_time > '2022-01-01'
    {% endif %}
    {% if is_incremental() %}
    AND TRY_CAST(date_trunc('DAY', tx.block_time) AS date) = TRY_CAST(date_trunc('DAY', looks_rare.block_time) AS date)
    {% endif %}
LEFT JOIN erc_transfers erc ON erc.evt_tx_hash = tx_hash AND erc.token_id_erc = token_id
LEFT JOIN {{ ref('tokens_ethereum_nft') }} tokens ON tokens.contract_address =  nft_contract_address
LEFT JOIN  {{ ref('nft_ethereum_aggregators') }} agg ON agg.contract_address = tx.to
LEFT JOIN {{ source('prices', 'usd') }} p ON p.minute = date_trunc('minute', looks_rare.block_time)
    AND p.contract_address = currency_contract
    AND p.blockchain ='ethereum'
LEFT JOIN {{ ref('tokens_ethereum_erc20') }} erc20 ON erc20.contract_address = currency_contract
WHERE number_of_items >= 1
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
AND looks_rare.block_time >= (select max(block_time) from {{ this }})
{% endif %} 