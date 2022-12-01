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
                CAST(number_of_items AS DECIMAL(38,0)) number_of_items,
                trade_category,
                evt_type,
                seller,
                buyer,
                amount_original,
                CAST(amount_raw AS DECIMAL(38,0)) amount_raw,
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
                CAST(platform_fee_percentage AS DOUBLE) platform_fee_percentage,
                royalty_fee_amount_raw,
                royalty_fee_amount,
                royalty_fee_amount_usd,
                CAST(royalty_fee_percentage AS DOUBLE) royalty_fee_percentage,
                royalty_fee_receive_address,
                royalty_fee_currency_symbol,
                unique_trade_id
        FROM {{ ref('opensea_v1_ethereum_events') }}
        UNION ALL
        SELECT   blockchain
                ,project
                ,version
                ,block_time
                ,token_id
                ,collection
                ,amount_usd
                ,token_standard
                ,case when trade_type <> 'Bundle Trade' and count(1) over (partition by tx_hash) > 1 then 'Bulk Purchase'
                      else trade_type
                 end as trade_type
                ,CAST(number_of_items AS DECIMAL(38,0)) number_of_items
                ,case when is_private then 'Private Sale' else trade_category end as trade_category -- Private sale can be purchasd by Buy/Offer accepted, but we surpress when it is Private sale here 
                ,evt_type
                ,seller
                ,buyer
                ,amount_original
                ,CAST(amount_raw AS DECIMAL(38,0)) amount_raw
                ,currency_symbol
                ,currency_contract
                ,nft_contract_address
                ,project_contract_address
                ,aggregator_name
                ,aggregator_address
                ,tx_hash
                ,block_number
                ,tx_from
                ,tx_to
                ,platform_fee_amount_raw
                ,platform_fee_amount
                ,platform_fee_amount_usd
                ,case when amount_raw > 0 then CAST ((platform_fee_amount_raw / amount_raw * 100) AS DOUBLE) end platform_fee_percentage
                ,royalty_fee_amount_raw
                ,royalty_fee_amount
                ,royalty_fee_amount_usd
                ,case when amount_raw > 0 then CAST((royalty_fee_amount_raw / amount_raw * 100) AS DOUBLE) end royalty_fee_percentage
                ,royalty_fee_receive_address
                ,currency_symbol as royalty_fee_currency_symbol
                ,unique_trade_id
          FROM {{ ref('opensea_v3_ethereum_events') }}
)