{{ config(
        alias ='events'
)
}}

SELECT *
FROM
(
        SELECT
                blockchain,
                project,
                version,
                block_time,
                token_id,
                collection,
                amount_usd,
                token_standard,
                trade_type,
                number_of_items,
                trade_category,
                evt_type,
                seller,
                buyer,
                amount_original,
                amount_raw,
                currency_symbol,
                currency_contract,
                nft_contract_address,
                project_contract_address,
                aggregator_name,
                aggregator_address,
                tx_hash,
                block_number,
                tx_from,
                tx_to,
                platform_fee_amount_raw,
                platform_fee_amount,
                platform_fee_amount_usd,
                platform_fee_percentage,
                royalty_fee_amount_raw,
                royalty_fee_amount,
                royalty_fee_amount_usd,
                royalty_fee_percentage,
                royalty_fee_receive_address,
                royalty_fee_currency_symbol,
                unique_trade_id
        FROM {{ ref('opensea_v1_ethereum_events') }}
        UNION ALL
        select   a.blockchain
                ,a.project
                ,a.version
                ,a.block_time
                ,a.token_id
                ,a.collection
                ,a.amount_usd
                ,a.token_standard
                ,a.trade_type
                ,a.number_of_items
                ,a.trade_category
                ,a.evt_type
                ,a.seller
                ,a.buyer
                ,a.amount_original
                ,a.amount_raw
                ,a.currency_symbol
                ,a.currency_contract
                ,a.nft_contract_address
                ,a.project_contract_address
                ,a.aggregator_name
                ,a.aggregator_address
                ,a.tx_hash
                ,b.block_number
                ,a.tx_from
                ,a.tx_to
                ,a.platform_fee_amount_raw
                ,a.platform_fee_amount
                ,a.platform_fee_amount_usd
                ,case when a.amount_raw > 0 then a.platform_fee_amount_raw / a.amount_raw * 100 end platform_fee_percentage
                ,a.royalty_fee_amount_raw
                ,a.royalty_fee_amount
                ,a.royalty_fee_amount_usd
                ,case when a.amount_raw > 0 then a.royalty_fee_amount_raw / a.amount_raw * 100 end royalty_fee_percentage
                ,a.royalty_fee_receive_address
                ,a.currency_symbol as royalty_fee_currency_symbol
                ,a.unique_trade_id
          from {{ ref('opensea_v3_ethereum_events') }}
                 inner join ethereum.transactions b on b.hash = a.tx_hash  -- oh dear. forgot to derive block_number
)