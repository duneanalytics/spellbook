{{ config(
        schema='collectionswap_ethereum',
        alias = 'events',
        partition_by = ['block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'unique_trade_id']
        )
}}

{%- set project_start_date = '2023-03-29' %}

WITH
raw_trades as (
    select *
    , row_number() over (partition by tx_hash order by evt_index asc, sub_order_id asc) as sub_tx_id
    from(
        select
            block_number, block_time, evt_index, tx_hash, buyer, seller,
            posexplode(nft_id_array) as (sub_order_id, token_id),
            amount_raw/number_of_items as amount_raw,
            platform_fee_amount_raw/number_of_items as platform_fee_amount_raw,
            royalty_fee_amount_raw/number_of_items as royalty_fee_amount_raw,
            trade_fee_amount_raw/number_of_items as trade_fee_amount_raw,
            royalty_fee_receive_address,
            project_contract_address,
            number_of_items,
            CASE WHEN number_of_items > 1 THEN 'Bundle Trade'
                    ELSE 'Single Item Trade'
                   END as trade_type,
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
                ,cast(outputAmount as decimal(38)) as amount_raw
                ,cast(protocolFee as decimal(38)) as platform_fee_amount_raw
                ,get_json_object(royaltyDue[0], '$.amount') as royalty_fee_amount_raw
                ,get_json_object(royaltyDue[0], '$.recipient') as royalty_fee_receive_address
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
                ,cast(inputAmount + protocolFee + cast(get_json_object(royaltyDue[0], '$.amount') as decimal(38)) as decimal(38)) as amount_raw
                ,cast(protocolFee as decimal(38)) as platform_fee_amount_raw
                ,get_json_object(royaltyDue[0], '$.amount') as royalty_fee_amount_raw
                ,get_json_object(royaltyDue[0], '$.recipient') as royalty_fee_receive_address
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
    case when p.token_address = '0x0000000000000000000000000000000000000000'
        then '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        else p.token_address END as currency_contract
    from raw_trades t
    left join {{ ref('collectionswap_ethereum_pools') }} p
    on t.project_contract_address = p.pool_address
)



select
    'ethereum' as blockchain
    ,'collectionswap' as project
    ,'v1' as version
    ,date_trunc('day',t.block_time ) as block_date
    ,t.block_time
    ,t.block_number
    ,t.evt_index
    ,t.tx_hash
    ,t.project_contract_address
    ,t.nft_contract_address
    ,t.token_id
    ,coalesce(t.buyer, tx.`from`) as buyer
    ,coalesce(t.seller, tx.`from`) as seller
    ,nft.name as collection
    ,nft.standard as token_standard
    ,t.currency_contract
    ,erc20.symbol as currency_symbol
    ,erc20.symbol as royalty_fee_currency_symbol
    ,cast(amount_raw as decimal(38)) as amount_raw
    ,cast(platform_fee_amount_raw as decimal(38)) as platform_fee_amount_raw
    ,cast(royalty_fee_amount_raw as decimal(38)) as royalty_fee_amount_raw
    ,amount_raw/pow(10,coalesce(erc20.decimals,18)) as amount_original
    ,platform_fee_amount_raw/pow(10,coalesce(erc20.decimals,18)) as platform_fee_amount
    ,royalty_fee_amount_raw/pow(10,coalesce(erc20.decimals,18)) as royalty_fee_amount
    ,amount_raw/pow(10,coalesce(erc20.decimals,18))*p.price as amount_usd
    ,platform_fee_amount_raw/pow(10,coalesce(erc20.decimals,18))*p.price as platform_fee_amount_usd
    ,royalty_fee_amount_raw/pow(10,coalesce(erc20.decimals,18))*p.price as royalty_fee_amount_usd
    ,case when amount_raw > 0 then 100.0*cast(platform_fee_amount_raw as DOUBLE)/cast(amount_raw as DOUBLE) end as platform_fee_percentage
    ,case when amount_raw > 0 then 100.0*cast(royalty_fee_amount_raw as DOUBLE)/cast(amount_raw as DOUBLE) end as royalty_fee_percentage
    ,cast(trade_fee_amount_raw as double)/pow(10,coalesce(erc20.decimals,18)) as trade_fee_amount
    ,cast(trade_fee_amount_raw as double)/pow(10,coalesce(erc20.decimals,18))*p.price as trade_fee_amount_usd
    ,t.trade_category
    ,t.number_of_items
    ,trade_type
    ,cast(null as varchar(1)) as aggregator_name
    ,cast(null as varchar(1)) as aggregator_address
    ,t.royalty_fee_receive_address
    ,cast(null as varchar(1)) as platform_fee_receive_address
    ,tx.`from` as tx_from
    ,tx.`to` as tx_to
    ,'Trade' as evt_type
    ,concat(t.block_number,t.tx_hash, t.sub_tx_id) as unique_trade_id
from base_trades t
left join {{ ref('tokens_ethereum_nft') }} nft
    ON nft.contract_address = t.nft_contract_address
left join {{ ref('tokens_ethereum_erc20') }} erc20
    ON erc20.contract_address = t.currency_contract
left join {{ source('prices', 'usd') }} p
    ON p.blockchain = 'ethereum' and p.minute = date_trunc('minute', t.block_time)
    AND p.contract_address = t.currency_contract
    {% if is_incremental() %}
    AND p.minute >= date_trunc("day", now() - interval '1 week')
    {% else %}
    AND p.minute >= '{{project_start_date}}'
    {% endif %}
inner join {{ source('ethereum','transactions') }} tx
    ON tx.block_number = t.block_number and tx.hash =  t.tx_hash
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc("day", now() - interval '1 week')
    {% else %}
    AND tx.block_time >= '{{project_start_date}}'
    {% endif %}




