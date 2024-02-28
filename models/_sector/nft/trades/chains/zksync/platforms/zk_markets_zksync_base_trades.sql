{{
    config(
        schema = 'zk_markets_zksync',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    )
}}

with

base_trades_combined as (
    select
        s.call_block_time as block_time,
        s.call_block_number as block_number,
        s.nftAddress as nft_contract_address,
        s.tokenId as nft_token_id,
        uint256 '1' as nft_amount,
        cast(null as varbinary) as seller, -- todo
        cast(null as varbinary) as buyer, -- todo
        cast(null as uint256) as price_raw, -- todo
        0x5AEa5775959fBC2557Cc8789bC1bf90A239D9a91 as currency_contract, -- WETH
        s.contract_address as project_contract_address,
        s.call_tx_hash as tx_hash,
        cast(null as uint256) as platform_fee_amount_raw, -- todo
        cast(null as uint256) as royalty_fee_amount_raw, -- todo
        cast(null as varbinary) as royalty_fee_address,
        cast(null as varbinary) as platform_fee_address,
        s.call_index as sub_tx_trade_id
    from {{ source('zk_markets_zksync', 'AANftMarketplace_call_buyItem') }} s
    where s.call_success
    {% if is_incremental() %}
    and {{incremental_predicate('s.call_block_time')}}
    {% endif %}

    union all

    select
        s.evt_block_time as block_time,
        s.evt_block_number as block_number,
        s.nftAddress as nft_contract_address,
        s.tokenId as nft_token_id,
        uint256 '1' as nft_amount,
        s.seller,
        s.buyer,
        s.price as price_raw,
        0x5AEa5775959fBC2557Cc8789bC1bf90A239D9a91 as currency_contract, -- WETH
        s.contract_address as project_contract_address,
        s.evt_tx_hash as tx_hash,
        cast(null as uint256) as platform_fee_amount_raw,
        cast(null as uint256) as royalty_fee_amount_raw,
        cast(null as varbinary) as royalty_fee_address,
        cast(null as varbinary) as platform_fee_address,
        s.evt_index as sub_tx_trade_id
    from {{ source('zk_markets_zksync', 'AANFTMarketplace_evt_ItemBought') }} s
    {% if is_incremental() %}
    where {{incremental_predicate('s.evt_block_time')}}
    {% endif %}

    union all

    select
        s.evt_block_time as block_time,
        s.evt_block_number as block_number,
        s.nftAddress as nft_contract_address,
        s.tokenId as nft_token_id,
        uint256 '1' as nft_amount,
        s.seller,
        s.buyer,
        s.price as price_raw,
        0x5AEa5775959fBC2557Cc8789bC1bf90A239D9a91 as currency_contract, -- WETH
        s.contract_address as project_contract_address,
        s.evt_tx_hash as tx_hash,
        cast(null as uint256) as platform_fee_amount_raw,
        cast(null as uint256) as royalty_fee_amount_raw,
        cast(null as varbinary) as royalty_fee_address,
        cast(null as varbinary) as platform_fee_address,
        s.evt_index as sub_tx_trade_id
    from {{ source('zk_markets_zksync', 'NftMarketplace_evt_ItemBought') }} s
    {% if is_incremental() %}
    where {{incremental_predicate('s.evt_block_time')}}
    {% endif %}
),

base_trades_combined as (
    select
        'zksync' as blockchain,
        'zk_markets' as project,
        '1' as project_version,
        t.block_time,
        cast(date_trunc('day', t.block_time) as date) as block_date,
        cast(date_trunc('month', t.block_time) as date) as block_month,
        t.block_number,
        t.nft_contract_address,
        t.nft_token_id,
        t.nft_amount,
        t.seller,
        t.buyer,
        'Buy' as trade_category,
        'secondary' as trade_type,
        t.price_raw,
        t.currency_contract,
        t.project_contract_address,
        t.tx_hash,
        t.platform_fee_amount_raw,
        t.royalty_fee_amount_raw,
        t.royalty_fee_address,
        t.platform_fee_address,
        t.sub_tx_trade_id
    from base_trades_combined t
)

-- this will be removed once tx_from and tx_to are available in the base event tables
{{ add_nft_tx_data('base_trades', 'zksync') }}
