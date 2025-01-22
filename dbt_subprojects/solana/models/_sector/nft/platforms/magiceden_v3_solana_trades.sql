{{
    config(
        schema = 'magiceden_v3_solana'
        
        , alias = 'trades'
        ,materialized = 'incremental'
        ,file_format = 'delta'
        ,incremental_strategy = 'merge'
        ,incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
        ,unique_key = ['instruction','account_merkle_tree','leaf_id','tx_id']
        ,post_hook='{{ expose_spells(\'["solana"]\',
                                    "project",
                                    "magiceden",
                                    \'["tsekityam"]\') }}'
    )
}}

with
    bubblegum_tx as (
        select
        call_tx_id as tx_id,
        call_outer_instruction_index as outer_instruction_index,
        index as leaf_id
        from {{ source('bubblegum_solana','bubblegum_call_transfer') }}
        {% if is_incremental() %}
        where {{incremental_predicate('call_block_time')}}
        {% endif %}
    ),
    fee_logs as (
        select distinct
        call_tx_id,
        call_block_slot,
        call_outer_instruction_index,
        cast(
            json_extract_scalar(json_parse(split(logs, ' ') [3]), '$.price') as double
        ) as price,
        cast(
            json_extract_scalar(json_parse(split(logs, ' ') [3]), '$.maker_fee') as double
        ) as maker_fee,
        cast(
            json_extract_scalar(json_parse(split(logs, ' ') [3]), '$.taker_fee') as double
        ) as taker_fee,
        cast(
            json_extract_scalar(
            json_parse(split(logs, ' ') [3]),
            '$.total_platform_fee'
            ) as double
        ) as total_platform_fee
        from
        (
            select
                call_tx_id,
                call_block_slot,
                call_outer_instruction_index,
                call_log_messages
            from {{ source('magic_eden_solana','m3_call_buyNow') }}
            {% if is_incremental() %}
            where {{incremental_predicate('call_block_time')}}
            {% endif %}
        )
        left join unnest (call_log_messages) as log_messages (logs) ON True
        where
        logs like 'Program log: {"price":%,"maker_fee":%,"taker_fee":%,"total_platform_fee":%}'
        and try(json_parse(split(logs, ' ') [3])) is not null
    ),
    cnft_base as (
        select
            coalesce(f.price, cast(
                json_value(t.args, 'strict $.BuyNowArgs.buyerPrice') as double
            )) as price,
            coalesce(f.taker_fee, cast(
                json_value(t.args, 'strict $.BuyNowArgs.buyerPrice') as double
            ) * cast(
                json_value(t.args, 'strict $.BuyNowArgs.takerFeeBp') as double
            ) / 10000) as taker_fee,
            coalesce(maker_fee, cast(
                json_value(t.args, 'strict $.BuyNowArgs.buyerPrice') as double
            ) * cast(
                json_value(t.args, 'strict $.BuyNowArgs.makerFeeBp') as double
            ) / 10000) as maker_fee,
            coalesce(cast(
                json_value(t.args, 'strict $.BuyNowArgs.buyerPrice') as double
            ) * cast(
                json_value(t.args, 'strict $.BuyNowArgs.buyerCreatorRoyaltyBp') as double
            ) / 10000, 0) as royalty_fee,
            t.call_instruction_name as instruction,
            'buy' as trade_category,
            t.account_merkleTree as account_merkle_tree,
            cast(
                json_value(t.args, 'strict $.BuyNowArgs.index') as bigint
            ) as leaf_id,
            t.account_buyer as buyer,
            t.account_seller as seller,
            t.call_outer_instruction_index as outer_instruction_index,
            coalesce(t.call_inner_instruction_index, 0) as inner_instruction_index,
            t.call_block_time as block_time,
            t.call_block_slot as block_slot,
            t.call_tx_id as tx_id,
            t.call_block_hash as block_hash,
            t.call_tx_index as tx_index,
            t.call_tx_signer as tx_signer
        from {{ source('magic_eden_solana','m3_call_buyNow') }} t
        left join fee_logs f on f.call_tx_id = t.call_tx_id
        and f.call_outer_instruction_index = t.call_outer_instruction_index
        {% if is_incremental() %}
        where {{incremental_predicate('call_block_time')}}
        {% endif %}
    ),
    priced_tokens as (
        select
            minute,
            price
        from
            {{ source('prices', 'usd') }} p
        where
            p.blockchain = 'solana'
            and symbol = 'SOL'
            {% if is_incremental() %}
            and {{incremental_predicate('p.minute')}}
            {% endif %}
  )
select
    'solana' as blockchain,
    'magiceden' as project,
    'v3' as version,
    t.block_time,
    'secondary' as trade_type,
    1 as number_of_items,
    t.trade_category,
    t.buyer,
    t.seller,
    t.price as amount_raw,
    t.price / 1e9 as amount_original,
    t.price / 1e9 * sol_p.price as amount_usd,
    'SOL' as currency_symbol,
    'So11111111111111111111111111111111111111112' as currency_address,
    t.account_merkle_tree,
    cast(coalesce(t.leaf_id, b.leaf_id) as bigint) as leaf_id,
    cast(null as varchar) as account_mint,
    'M3mxk5W2tt27WGT7THox7PmgRDp4m6NEhL5xvxrBfS1' as project_program_id,
    cast(null as varchar) as aggregator_name,
    cast(null as varchar) as aggregator_address,
    t.tx_id,
    t.block_slot,
    t.tx_signer,
    t.taker_fee as taker_fee_amount_raw,
    t.taker_fee / 1e9 as taker_fee_amount,
    t.taker_fee / 1e9 * sol_p.price as taker_fee_amount_usd,
    case
        when t.taker_fee = 0
        OR t.price = 0 then 0
        else t.taker_fee / t.price
    end as taker_fee_percentage,
    t.maker_fee as maker_fee_amount_raw,
    t.maker_fee / 1e9 as maker_fee_amount,
    t.maker_fee / 1e9 * sol_p.price as maker_fee_amount_usd,
    case
        when t.maker_fee = 0
        OR t.price = 0 then 0
        else t.maker_fee / t.price
    end as maker_fee_percentage,
    cast(null as double) as amm_fee_amount_raw,
    cast(null as double) as amm_fee_amount,
    cast(null as double) as amm_fee_amount_usd,
    cast(null as double) as amm_fee_percentage,
    t.royalty_fee as royalty_fee_amount_raw,
    t.royalty_fee / 1e9 as royalty_fee_amount,
    t.royalty_fee / 1e9 * sol_p.price as royalty_fee_amount_usd,
    case
        when t.royalty_fee = 0
        OR t.price = 0 then 0
        else t.royalty_fee / t.price
    end as royalty_fee_percentage,
    t.instruction,
    t.outer_instruction_index,
    t.inner_instruction_index
from
    cnft_base t
    left join priced_tokens sol_p on sol_p.minute = date_trunc('minute', t.block_time)
    left join bubblegum_tx b on b.tx_id = t.tx_id
    and b.outer_instruction_index = t.outer_instruction_index