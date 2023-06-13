{{ config(
    schema = 'nft',
    alias = 'trades_unstable',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['project','project_version','tx_hash','sub_tx_trade_id']
    )
}}

with trades as (
        select
        blockchain,
        project,
        project_version,
        block_date,
        block_time,
        nft_token_id,
        nft_collection,
        price_usd,
        nft_standard,
        trade_type,
        nft_amount,
        trade_category,
        seller,
        buyer,
        price,
        price_raw,
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
        royalty_fee_address,
        royalty_fee_currency_symbol,
        royalty_fee_amount_raw,
        royalty_fee_amount,
        royalty_fee_amount_usd,
        royalty_fee_percentage,
        unique_trade_id
    from {{ref('nft_trades_forward_ported')}}
    {% if is_incremental() %}
    where block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    )

, mints as (
    select
        blockchain,
        project,
        version as project_version,
        block_date,
        block_time,
        token_id as nft_token_id,
        collection as nft_collection,
        amount_usd as price_usd,
        token_standard as nft_standard,
        'primary' as trade_type,
        number_of_items as nft_amount,
        trade_category,
        seller,
        buyer,
        amount_original as price,
        amount_raw as price_raw,
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
        0 as platform_fee_amount_raw,
        0 as platform_fee_amount,
        0 as platform_fee_amount_usd,
        0 as platform_fee_percentage,
        0 as royalty_fee_address,
        0 as royalty_fee_currency_symbol,
        0 as royalty_fee_amount_raw,
        0 as royalty_fee_amount,
        0 as royalty_fee_amount_usd,
        0 as royalty_fee_percentage,
        unique_trade_id
    from {{ref('nft_mints')}}
    {% if is_incremental() %}
    where block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    )

, events as (
    select * from trades
    union all
    select * from mints
    )

select
    e.blockchain,
    e.project,
    e.project_version,
    e.block_date,
    e.block_time,
    e.nft_token_id,
    e.nft_collection,
    e.price_usd,
    e.nft_standard,
    e.trade_type,
    e.nft_amount,
    e.trade_category,
    e.seller,
    e.buyer,
    e.price as price_original_currency,
    e.currency_symbol,
    e.currency_contract,
    e.nft_contract_address,
    e.project_contract_address,
    e.aggregator_name,
    e.aggregator_address,
    e.tx_hash,
    e.block_number,
    e.tx_from,
    e.tx_to,
    e.platform_fee_amount,
    e.platform_fee_amount_usd,
    e.platform_fee_percentage,
    e.royalty_fee_address,
    e.royalty_fee_currency_symbol,
    e.royalty_fee_amount,
    e.royalty_fee_amount_usd,
    e.royalty_fee_percentage,
    e.unique_trade_id,
    rt.description as token_description,
    rt.name as token_name,
    rt.owner as current_token_owner,
    rc.description as collection_description,
    rc.name as collection_name,
    rc.community as collection_community,
    rc.all_time_volume as collection_all_time_volume,
    rc.all_time_rank as collection_all_time_rank,
    rc.token_count as total_supply
  from events e
  left join {{source('reservoir', 'tokens') }} rt
  on e.nft_contract_address = rt.contract
  and e.nft_token_id = rt.token_id
  left join {{source('reservoir', 'collections') }} rc
  on e.nft_contract_address = rc.contract