{{ config(
       schema = 'dns_ton'
       , alias = 'delegation_updates'
       , materialized = 'incremental'
       , file_format = 'delta'
       , incremental_strategy = 'merge'
       , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
       , unique_key = ['tx_hash', 'block_time']
   )
 }}

{#
https://github.com/ton-blockchain/dns-contract/blob/main/func/nft-item.fc#L204 - implementation of op::change_dns_record
#}
with 
_config as (
    select
        '0:B774D95EB20543F186C06B371AB88AD704F7E256130CAF96189368A7D0CB6CCF' as collection_address
),
_dns_mint_events as (
    select
        *
    from
        {{ source('ton', 'nft_events') }}
    where
        collection_address = (select collection_address from _config)
        and "type" = 'mint'
),
_dns_items_address as (
    select
        nft_item_address, max(cast(json_extract(content_onchain, '$.domain') as varchar) || '.ton') as domain
    from
        _dns_mint_events
    group by
        1
),
_change_dns_msgs as (
    select
        t.block_date,
        m.block_time,
        m.tx_hash,
        m.tx_lt,
        m.trace_id,
        m.body_boc,
        m.source as delegation_initiator,
        m.direction as msg_direction,
        m.destination as nft_item_address,
        domain
    from
        {{ source('ton', 'messages') }} m
    join
        _dns_items_address dns_items on m.destination = dns_items.nft_item_address
    join
        {{ source('ton', 'transactions') }} t on t.hash = m.tx_hash and t.block_date = m.block_date and m.direction = 'in'
    where
        m.opcode = from_base(substr('0x4eb1f0f9', 3), 16) -- op::change_dns_record
        and t.compute_exit_code = 0 and t.action_result_code = 0 -- check that transaction is successful
        {% if is_incremental() %}
        AND {{ incremental_predicate('m.block_date') }}
        {% endif %}
),
_result_update as (
    select try({{ ton_from_boc('body_boc', [
        ton_begin_parse(),
        ton_load_uint(32, 'op_id'),
        ton_load_uint(64, 'query_id'),
        ton_load_uint(256, 'key'),
        ton_return_if_neq('key', 105311596331855300602201538317979276640056460191511695660591596829410056223515),
        ton_load_ref(),
        ton_begin_parse(),
        ton_skip_bits(16),
        ton_load_address('wallet')
        ]) }}) as result, * 
    from 
        _change_dns_msgs
),
_result as (
    select 
        block_date,
        block_time, 
        tx_hash, 
        tx_lt,
        trace_id, 
        domain,
        nft_item_address as dns_nft_item_address,
        delegation_initiator as delegation_initiator,
        case
            when r.result.wallet = 'addr_none' then null
            else r.result.wallet
        end as delegated_to_wallet
    from 
        _result_update r
    where
        r.result is not null
        and r.result.key = CAST('105311596331855300602201538317979276640056460191511695660591596829410056223515' AS UINT256)
)
select 
    *
from 
    _result