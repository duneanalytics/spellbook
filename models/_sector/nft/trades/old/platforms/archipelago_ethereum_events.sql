{{ config(
        schema = 'archipelago_ethereum',
        alias = 'events',
        partition_by = ['block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'unique_trade_id']
        )
}}

WITH
    trade_events as (
        SELECT
            contract_address as project_contract_address
            , evt_block_number as block_number
            , evt_block_time as block_time
            , evt_tx_hash as tx_hash
            , buyer
            , seller
            , cost as amount_raw
            , currency as currency_contract
            , tradeId as unique_trade_id
        FROM {{ source('archipelago_ethereum','ArchipelagoMarket_evt_Trade') }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        {% if not is_incremental() %}
        WHERE evt_block_time >= '2022-6-20'
        {% endif %}

    ),

    token_events as (
        SELECT
             evt_block_number as block_number
            , evt_block_time as block_time
            , evt_tx_hash as tx_hash
            , tokenAddress as nft_contract_address
            , tokenId as token_id
            , tradeId as unique_trade_id
        FROM {{ source('archipelago_ethereum','ArchipelagoMarket_evt_TokenTrade') }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        {% if not is_incremental() %}
        WHERE evt_block_time >= '2022-6-20'
        {% endif %}

    ),

    fee_events as (
        SELECT
             evt_block_number as block_number
            , evt_block_time as block_time
            , evt_tx_hash as tx_hash
            , amount as fee_amount_raw
            , currency as fee_currency_contract
            , recipient as fee_receive_address
            , micros / pow(10,4) as fee_percentage
            , tradeId as unique_trade_id
            , case when (
                upper(recipient) = upper('0xA76456bb6aBC50FB38e17c042026bc27a95C3314')
                or upper(recipient) = upper('0x1fC12C9f68A6B0633Ba5897A40A8e61ed9274dC9')
                ) then true else false end
                as is_protocol_fee
        FROM {{ source('archipelago_ethereum','ArchipelagoMarket_evt_RoyaltyPayment') }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        {% if not is_incremental() %}
        WHERE evt_block_time >= '2022-6-20'
        {% endif %}

    ),

    tokens_ethereum_nft as (
        SELECT
            *
        FROM {{ ref('tokens_nft') }}
        WHERE blockchain = 'ethereum'
    ),

    nft_ethereum_aggregators as (
        SELECT
            *
        FROM {{ ref('nft_aggregators') }}
        WHERE blockchain = 'ethereum'
    ),

    -- enrichments

    trades_with_nft_and_tx as (
        select
            e.*
            , t.nft_contract_address
            , t.token_id
            , tx.from as tx_from
            , tx.to  as tx_to
        from trade_events e
        inner join token_events t
            ON e.block_number = t.block_number and e.unique_trade_id = t.unique_trade_id
        inner join {{ source('ethereum', 'transactions') }} tx
            ON e.block_number = tx.block_number and e.tx_hash = tx.hash
            {% if is_incremental() %}
            AND tx.block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
            {% if not is_incremental() %}
            AND tx.block_time >= '2022-6-20'
            {% endif %}
    ),

    platform_fees as (
        select
            block_number
            ,sum(fee_amount_raw) as platform_fee_amount_raw
            ,sum(fee_percentage) as platform_fee_percentage
            ,unique_trade_id
        from fee_events
            where is_protocol_fee
        group by block_number,unique_trade_id
    ),

    royalty_fees as (
        select
            block_number
            ,sum(fee_amount_raw) as royalty_fee_amount_raw
            ,sum(fee_percentage) as royalty_fee_percentage
            ,CAST(null AS VARCHAR(5)) as royalty_fee_receive_address -- we have multiple address so have to null this field
            ,unique_trade_id
        from fee_events
            where not is_protocol_fee
        group by block_number,unique_trade_id
    ),


    trades_with_fees as (
        select
            t.*
            , pf.platform_fee_amount_raw
            , pf.platform_fee_percentage
            , rf.royalty_fee_amount_raw
            , rf.royalty_fee_percentage
            , rf.royalty_fee_receive_address
        from trades_with_nft_and_tx t
        left join platform_fees pf
            ON t.block_number = pf.block_number and t.unique_trade_id = pf.unique_trade_id
        left join royalty_fees rf
            ON t.block_number = rf.block_number and t.unique_trade_id = rf.unique_trade_id

    ),

    trades_with_price as (
        select
            t.*
            , p.symbol as currency_symbol
            , amount_raw/pow(10, p.decimals) as amount_original
            , amount_raw/pow(10, p.decimals)*p.price as amount_usd
            , platform_fee_amount_raw/pow(10, p.decimals) as platform_fee_amount
            , platform_fee_amount_raw/pow(10, p.decimals)*p.price as platform_fee_amount_usd
            , royalty_fee_amount_raw/pow(10, p.decimals) as royalty_fee_amount
            , royalty_fee_amount_raw/pow(10, p.decimals)*p.price as royalty_fee_amount_usd
            , p.symbol as royalty_fee_currency_symbol
        from trades_with_fees t
        left join {{ source('prices', 'usd') }} p ON p.blockchain='ethereum'
            AND p.symbol = 'WETH' -- currently we only have ETH trades
            AND date_trunc('minute', p.minute)=date_trunc('minute', t.block_time)
            {% if is_incremental() %}
            AND p.minute >= date_trunc("day", now() - interval '1 week')
            {% endif %}
            {% if not is_incremental() %}
            AND p.minute >= '2022-4-1'
            {% endif %}
    ),

    trades_enhanced as (
        select
            t.*
            , nft.standard as token_standard
            , nft.name as collection
            , agg.contract_address as aggregator_address
            , agg.name as aggregator_name
        from trades_with_price t
        left join tokens_ethereum_nft nft
            ON nft_contract_address = nft.contract_address
        left join nft_ethereum_aggregators agg
            ON tx_to = agg.contract_address
    )


SELECT
    'ethereum' as blockchain
    , 'archipelago' as project
    , 'v1' as version
    , TRY_CAST(date_trunc('DAY', te.block_time) AS date) AS block_date
    , te.block_time
    , te.block_number
    , te.token_id
    , te.token_standard
    , CAST(1 AS DECIMAL(38,0)) as number_of_items
    , 'Single Item Trade' as trade_type
    , case when te.tx_from = COALESCE(seller_fix.from, te.seller) then 'Offer Accepted' else 'Buy' end as trade_category
    , 'Trade' as evt_type
    , COALESCE(seller_fix.from, te.seller) AS seller
    , COALESCE(buyer_fix.to, te.buyer) AS buyer
    , CAST(te.amount_raw AS DECIMAL(38,0)) AS amount_raw
    , te.amount_original
    , te.amount_usd
    , te.currency_symbol
    , te.currency_contract
    , te.project_contract_address
    , te.nft_contract_address
    , te.collection
    , te.tx_hash
    , te.tx_from
    , te.tx_to
    , te.aggregator_address
    , te.aggregator_name
    , te.platform_fee_amount
    , te.platform_fee_amount_raw
    , te.platform_fee_amount_usd
    , CAST(te.platform_fee_percentage AS DOUBLE) AS platform_fee_percentage
    , te.royalty_fee_amount
    , te.royalty_fee_amount_usd
    , te.royalty_fee_amount_raw
    , te.royalty_fee_currency_symbol
    , te.royalty_fee_receive_address -- null here
    , CAST(te.royalty_fee_percentage AS DOUBLE) AS royalty_fee_percentage
    , te.unique_trade_id
from trades_enhanced te
left join {{ ref('nft_ethereum_transfers') }} buyer_fix on buyer_fix.block_time=te.block_time
    and te.nft_contract_address=buyer_fix.contract_address
    and buyer_fix.tx_hash=te.tx_hash
    and te.token_id=buyer_fix.token_id
    and te.buyer=te.aggregator_address
    and buyer_fix.from=te.aggregator_address
    {% if is_incremental() %}
    and buyer_fix.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
left join {{ ref('nft_ethereum_transfers') }} seller_fix on seller_fix.block_time=te.block_time
    and te.nft_contract_address=seller_fix.contract_address
    and seller_fix.tx_hash=te.tx_hash
    and te.token_id=seller_fix.token_id
    and te.seller=te.aggregator_address
    and seller_fix.to=te.aggregator_address
    {% if is_incremental() %}
    and seller_fix.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
