{{ config(
       schema = 'dns_ton'
       , alias = 'wallet_delegation_updates'
       , materialized = 'incremental'
       , file_format = 'delta'
       , incremental_strategy = 'merge'
       , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
       , unique_key = ['tx_hash', 'block_time']
   )
 }}

with 
_config as (
    select
        upper(ton_address_user_friendly_to_raw('EQC3dNlesgVD8YbAazcauIrXBPfiVhMMr5YYk2in0Mtsz0Bz')) as collection_address
),
_dns_mint_events as (
    select
        *
    from
        ton.nft_events
    where
        collection_address = (select collection_address from _config)
        and "type" = 'mint'
),
_dns_items_address as (
    select
        nft_item_address
    from
        _dns_mint_events
    group by
        1
),
_latest_metadata as (
    select
        address,
        max_by(name, update_time_onchain) as name,
        max_by(content_onchain, update_time_onchain) as content_onchain,
        max_by(description, update_time_onchain) as description,
        max_by(image, update_time_onchain) as image
    from
        ton.nft_metadata NM
    group by
        1
),
_change_dns_msgs as (
    select
        m.block_time,
        m.tx_hash,
        m.trace_id,
        m.body_boc,
        m.source as nft_item_owner,
        m.direction as msg_direction,
        m.destination as nft_item_address,
        (cast(json_extract(metadata.content_onchain, '$.domain') as varchar) || '.ton') as domain
    from
        ton.messages m
    join
        _dns_items_address dns_items on m.destination = dns_items.nft_item_address
    join
        _latest_metadata metadata on m.destination = metadata.address
    where
        m.opcode = from_base(substr('0x4eb1f0f9', 3), 16)
        and m.direction = 'out'
        {% if is_incremental() %}
        AND {{ incremental_predicate('m.block_date') }}
        {% endif %}
),
_dns_ton_change_record_messages_key as (
    select {{ ton_from_boc('body_boc', [
        ton_begin_parse(),
        ton_load_uint(32, 'op_id'),
        ton_load_uint(64, 'query_id'),
        ton_load_uint(256, 'key'),
        ]) }} as pre_result, * 
    from 
        _change_dns_msgs
),
_result_update as (
    select {{ ton_from_boc('body_boc', [
        ton_begin_parse(),
        ton_load_uint(32, 'op_id'),
        ton_load_uint(64, 'query_id'),
        ton_load_uint(256, 'key'),
        ton_load_ref(),
        ton_begin_parse(),
        ton_load_uint(8, 'byte0'),
        ton_load_uint(8, 'byte1'),
        ton_load_address('wallet', false)
        ]) }} as result, * 
    from 
        _dns_ton_change_record_messages_key
    where 
        to_hex(cast(pre_result.key as varbinary)) = 'E8D44050873DBA865AA7C170AB4CCE64D90839A34DCFD6CF71D14E0205443B1B'
),
_result as (
    select 
        block_time, 
        tx_hash, 
        trace_id, 
        domain,
        nft_item_address as dns_nft_item_address,
        nft_item_owner as dns_nft_item_owner,
        case
            when r.result.wallet = 'addr_none' then null
            else r.result.wallet
        end as delegated_to_wallet
    from 
        _result_update r
    WHERE
        r.result.wallet != 'address format is not supported'
)
select 
    *
from 
    _result