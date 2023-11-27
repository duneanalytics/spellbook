{{ config(
    schema = 'collectionswap_ethereum',

    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}

{%- set project_start_date = "2023-03-29" %}

WITH
raw_trades as (
    select *
    , row_number() over (partition by tx_hash order by evt_index asc, sub_order_id asc) as sub_tx_trade_id
    from(
        select
            block_number, block_time, evt_index, tx_hash, buyer, seller,
            uint256 '1' as nft_amount,
            price_raw/number_of_items as price_raw,
            platform_fee_amount_raw/number_of_items as platform_fee_amount_raw,
            royalty_fee_amount_raw/number_of_items as royalty_fee_amount_raw,
            trade_fee_amount_raw/number_of_items as trade_fee_amount_raw,
            royalty_fee_address,
            project_contract_address,
            number_of_items,
            'secondary' as trade_type,
            trade_category,
            nft_token_id,
            sub_order_id
        from(
            select
                 evt_block_number as block_number
                ,evt_block_time as block_time
                ,evt_index
                ,evt_tx_hash as tx_hash
                ,cast(null as varbinary) as buyer
                ,contract_address as seller
                ,'Buy' as trade_category
                ,nftIds as nft_id_array
                ,cast(cardinality(nftIds) as uint256) as number_of_items
                ,cast(outputAmount as uint256) as price_raw
                ,cast(protocolFee as uint256) as platform_fee_amount_raw
                ,cast(JSON_EXTRACT_SCALAR(royaltyDue[1], '$.amount') as uint256) as royalty_fee_amount_raw
                ,from_hex(JSON_EXTRACT_SCALAR(royaltyDue[1], '$.recipient')) as royalty_fee_address
                ,cast(tradeFee as uint256) as trade_fee_amount_raw
                ,contract_address as project_contract_address
            from {{ source('collectionswap_ethereum','CollectionPool_evt_SwapNFTOutPool') }} e
            {% if is_incremental() %}
            WHERE {{incremental_predicate('evt_block_time')}}
            {% else %}
            WHERE evt_block_time >= timestamp '{{project_start_date}}'
            {% endif %}
            union all
            select
                evt_block_number AS block_number,
                evt_block_time AS block_time,
                evt_index,
                evt_tx_hash AS tx_hash,
                contract_address AS buyer,
                cast(null as varbinary) AS seller,
                'Sell' AS trade_category,
                nftIds AS nft_id_array,
                cast(cardinality(nftIds) as uint256) AS number_of_items,
                TRY_CAST(
                inputAmount + protocolFee + TRY_CAST(
                  JSON_EXTRACT_SCALAR(royaltyDue[1], '$.amount') AS UINT256
                ) AS UINT256
                ) AS price_raw,
                TRY_CAST(protocolFee AS UINT256) AS platform_fee_amount_raw,
                TRY_CAST(JSON_EXTRACT_SCALAR(royaltyDue[1], '$.amount')  AS UINT256) AS royalty_fee_amount_raw,
                from_hex(JSON_EXTRACT_SCALAR(royaltyDue[1], '$.recipient')) AS royalty_fee_address,
                TRY_CAST(tradeFee AS UINT256) AS trade_fee_amount_raw,
                contract_address AS project_contract_address
            from {{ source('collectionswap_ethereum','CollectionPool_evt_SwapNFTInPool') }} e
            {% if is_incremental() %}
            WHERE {{incremental_predicate('evt_block_time')}}
            {% else %}
            WHERE evt_block_time >= timestamp '{{project_start_date}}'
            {% endif %}
            )
        CROSS JOIN UNNEST(nft_id_array)
        WITH ORDINALITY AS foo(nft_token_id, sub_order_id)
    )
),

base_trades as (
    select
    t.*,
    p.nft_contract_address,
    p.token_address as currency_contract
    from raw_trades t
    left join {{ ref('collectionswap_ethereum_pools') }} p
    on t.project_contract_address = p.pool_address
)

-- results
SELECT
 'ethereum' as blockchain
, 'collectionswap' as project
, 'v1' as project_version
, block_time
, block_number
, tx_hash
, project_contract_address
, buyer
, seller
, nft_contract_address
, nft_token_id
, nft_amount
, trade_type
, trade_category
, currency_contract
, cast(price_raw as uint256) as price_raw
, cast(platform_fee_amount_raw as uint256) as platform_fee_amount_raw
, cast(royalty_fee_amount_raw as uint256) as royalty_fee_amount_raw
, cast(null as varbinary) as platform_fee_address
, royalty_fee_address
, sub_tx_trade_id
FROM base_trades


