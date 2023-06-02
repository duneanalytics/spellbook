{{ config(
    schema = 'collectionswap_ethereum',
    alias ='base_trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}

{%- set project_start_date = '2023-03-29' %}

WITH
raw_trades as (
    select *
    , row_number() over (partition by tx_hash order by evt_index asc, sub_order_id asc) as sub_tx_trade_id
    from(
        select
            block_number, block_time, evt_index, tx_hash, buyer, seller,
            posexplode(nft_id_array) as (sub_order_id, nft_token_id),
            cast(1 as DECIMAL(38,0)) as nft_amount,
            price_raw/number_of_items as price_raw,
            platform_fee_amount_raw/number_of_items as platform_fee_amount_raw,
            royalty_fee_amount_raw/number_of_items as royalty_fee_amount_raw,
            trade_fee_amount_raw/number_of_items as trade_fee_amount_raw,
            royalty_fee_address,
            project_contract_address,
            number_of_items,
            'secondary' as trade_type,
            trade_category
        from(
            select
                 evt_block_number as block_number
                ,evt_block_time as block_time
                ,evt_index
                ,evt_tx_hash as tx_hash
                ,null as buyer
                ,contract_address as seller
                ,'Buy' as trade_category
                ,nftIds as nft_id_array
                ,cardinality(nftIds) as number_of_items
                ,cast(outputAmount as decimal(38)) as price_raw
                ,cast(protocolFee as decimal(38)) as platform_fee_amount_raw
                ,get_json_object(royaltyDue[0], '$.amount') as royalty_fee_amount_raw
                ,get_json_object(royaltyDue[0], '$.recipient') as royalty_fee_address
                ,cast(tradeFee as decimal(38)) as trade_fee_amount_raw
                ,contract_address as project_contract_address
            from {{ source('collectionswap_ethereum','CollectionPool_evt_SwapNFTOutPool') }} e
            {% if is_incremental() %}
            WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
            {% else %}
            WHERE evt_block_time >= '{{project_start_date}}'
            {% endif %}
            union all
            select
                evt_block_number as block_number
                ,evt_block_time as block_time
                ,evt_index
                ,evt_tx_hash as tx_hash
                ,contract_address as buyer
                ,null as seller
                ,'Sell' as trade_category
                ,nftIds as nft_id_array
                ,cardinality(nftIds) as number_of_items
                ,cast(inputAmount + protocolFee + cast(get_json_object(royaltyDue[0], '$.amount') as decimal(38)) as decimal(38)) as price_raw
                ,cast(protocolFee as decimal(38)) as platform_fee_amount_raw
                ,get_json_object(royaltyDue[0], '$.amount') as royalty_fee_amount_raw
                ,get_json_object(royaltyDue[0], '$.recipient') as royalty_fee_address
                ,cast(tradeFee as decimal(38)) as trade_fee_amount_raw
                ,contract_address as project_contract_address
            from {{ source('collectionswap_ethereum','CollectionPool_evt_SwapNFTInPool') }} e
            {% if is_incremental() %}
            WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
            {% else %}
            WHERE evt_block_time >= '{{project_start_date}}'
            {% endif %}
            )
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
  date_trunc('day',block_time ) as block_date
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
, cast(price_raw as decimal(38)) as price_raw
, cast(platform_fee_amount_raw as decimal(38)) as platform_fee_amount_raw
, cast(royalty_fee_amount_raw as decimal(38)) as royalty_fee_amount_raw
, cast(null as varchar(1)) as platform_fee_address
, royalty_fee_address
, sub_tx_trade_id
FROM base_trades


