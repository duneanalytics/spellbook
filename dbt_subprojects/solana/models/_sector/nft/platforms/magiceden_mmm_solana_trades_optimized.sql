{{
    config(
        schema = 'magiceden_mmm_solana'
        , alias = 'trades_optimized'
        ,materialized = 'incremental'
        ,file_format = 'delta'
        ,incremental_strategy = 'merge'
        ,incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
        ,unique_key = ['project','trade_category','outer_instruction_index','inner_instruction_index','account_mint','tx_id']
        ,post_hook='{{ expose_spells(\'["solana"]\',
                                    "project",
                                    "magiceden",
                                    \'["tsekityam"]\') }}'
    )
}}

with source_data as (
    select
        call_tx_id,
        call_block_slot,
        call_outer_instruction_index,
        call_inner_instruction_index,
        call_log_messages,
        call_block_time,
        call_tx_signer,
        account_owner,
        account_assetMint,
        args,
        call_instruction_name,
        call_account_arguments,
        'buy' as trade_type
    from {{ source('magic_eden_solana','mmm_call_solFulfillBuy') }}
    {% if is_incremental() %}
    where {{incremental_predicate('call_block_time')}}
    {% endif %}

    union all

    select
        call_tx_id,
        call_block_slot,
        call_outer_instruction_index,
        call_inner_instruction_index,
        call_log_messages,
        call_block_time,
        call_tx_signer,
        account_owner,
        account_assetMint,
        args,
        call_instruction_name,
        call_account_arguments,
        'sell' as trade_type
    from {{ source('magic_eden_solana','mmm_call_solFulfillSell') }}
    {% if is_incremental() %}
    where {{incremental_predicate('call_block_time')}}
    {% endif %}

    union all

    select
        call_tx_id,
        call_block_slot,
        call_outer_instruction_index,
        call_inner_instruction_index,
        call_log_messages,
        call_block_time,
        call_tx_signer,
        account_owner,
        account_assetMint,
        args,
        call_instruction_name,
        call_account_arguments,
        'buy' as trade_type
    from {{ source('magic_eden_solana','mmm_call_solMip1FulfillBuy') }}
    {% if is_incremental() %}
    where {{incremental_predicate('call_block_time')}}
    {% endif %}

    union all

    select
        call_tx_id,
        call_block_slot,
        call_outer_instruction_index,
        call_inner_instruction_index,
        call_log_messages,
        call_block_time,
        call_tx_signer,
        account_owner,
        account_assetMint,
        args,
        call_instruction_name,
        call_account_arguments,
        'sell' as trade_type
    from {{ source('magic_eden_solana','mmm_call_solMip1FulfillSell') }}
    {% if is_incremental() %}
    where {{incremental_predicate('call_block_time')}}
    {% endif %}

    union all

    select
        call_tx_id,
        call_block_slot,
        call_outer_instruction_index,
        call_inner_instruction_index,
        call_log_messages,
        call_block_time,
        call_tx_signer,
        account_owner,
        account_assetMint,
        args,
        call_instruction_name,
        call_account_arguments,
        'buy' as trade_type
    from {{ source('magic_eden_solana','mmm_call_solOcpFulfillBuy') }}
    {% if is_incremental() %}
    where {{incremental_predicate('call_block_time')}}
    {% endif %}

    union all

    select
        call_tx_id,
        call_block_slot,
        call_outer_instruction_index,
        call_inner_instruction_index,
        call_log_messages,
        call_block_time,
        call_tx_signer,
        account_owner,
        account_assetMint,
        args,
        call_instruction_name,
        call_account_arguments,
        'sell' as trade_type
    from {{ source('magic_eden_solana','mmm_call_solOcpFulfillSell') }}
    {% if is_incremental() %}
    where {{incremental_predicate('call_block_time')}}
    {% endif %}
),

trades_with_logs as (
    select
        sd.*,
        cast(json_extract_scalar(json_parse(split(logs, ' ')[3]), '$.royalty_paid') as double) as royalty_paid,
        cast(json_extract_scalar(json_parse(split(logs, ' ')[3]), '$.total_price') as double) as total_price,
        cast(json_extract_scalar(json_parse(split(logs, ' ')[3]), '$.lp_fee') as double) as lp_fee,
        row_number() over (
            partition by sd.call_tx_id
            order by sd.call_outer_instruction_index asc, sd.call_inner_instruction_index asc
        ) as log_order
    from source_data sd
    cross join unnest(sd.call_log_messages) as t(logs)
    where logs like 'Program log: {"lp_fee":%,"royalty_paid":%,"total_price":%}'
        and try(json_parse(split(logs, ' ')[3])) is not null
),

trades as (
    select
        twl.trade_type as trade_category,
        'SOL' as trade_token_symbol,
        'So11111111111111111111111111111111111111112' as trade_token_mint,
        twl.total_price as price,
        cast(json_value(twl.args, '$.SolFulfillBuyArgs.makerFeeBp') as double) / 1e4 * twl.total_price as maker_fee,
        cast(json_value(twl.args, '$.SolFulfillBuyArgs.takerFeeBp') as double) / 1e4 * twl.total_price as taker_fee,
        cast(json_value(twl.args, '$.SolFulfillBuyArgs.assetAmount') as double) as token_size,
        twl.royalty_paid as royalty_fee,
        twl.lp_fee as amm_fee,
        twl.call_instruction_name as instruction,
        twl.account_assetMint as account_tokenMint,
        case when twl.trade_type = 'buy' then twl.account_owner else twl.call_tx_signer end as account_buyer,
        case when twl.trade_type = 'sell' then twl.account_owner else twl.call_tx_signer end as account_seller,
        twl.call_outer_instruction_index as outer_instruction_index,
        twl.call_inner_instruction_index as inner_instruction_index,
        twl.call_block_time,
        twl.call_block_slot,
        twl.call_tx_id,
        twl.call_tx_signer,
        twl.call_account_arguments
    from trades_with_logs twl
),

raw_nft_trades as (
    select
        'solana' as blockchain,
        'magiceden' as project,
        'mmm' as version,
        t.call_block_time as block_time,
        'secondary' as trade_type,
        t.token_size as number_of_items,
        t.trade_category,
        t.account_buyer as buyer,
        t.account_seller as seller,
        t.price as amount_raw,
        t.price / pow(10, p.decimals) as amount_original,
        t.price / pow(10, p.decimals) * p.price as amount_usd,
        t.trade_token_symbol as currency_symbol,
        t.trade_token_mint as currency_address,
        cast(null as varchar) as account_merkle_tree,
        cast(null as bigint) as leaf_id,
        t.account_tokenMint as account_mint,
        'mmm3XBJg5gk8XJxEKBvdgptZz6SgK4tXvn36sodowMc' as project_program_id,
        cast(null as varchar) as aggregator_name,
        cast(null as varchar) as aggregator_address,
        t.call_tx_id as tx_id,
        t.call_block_slot as block_slot,
        t.call_tx_signer as tx_signer,
        t.taker_fee as taker_fee_amount_raw,
        t.taker_fee / pow(10, p.decimals) as taker_fee_amount,
        t.taker_fee / pow(10, p.decimals) * p.price as taker_fee_amount_usd,
        case when t.taker_fee = 0 or t.price = 0 then 0 else t.taker_fee / t.price end as taker_fee_percentage,
        t.maker_fee as maker_fee_amount_raw,
        t.maker_fee / pow(10, p.decimals) as maker_fee_amount,
        t.maker_fee / pow(10, p.decimals) * p.price as maker_fee_amount_usd,
        case when t.maker_fee = 0 or t.price = 0 then 0 else t.maker_fee / t.price end as maker_fee_percentage,
        t.amm_fee as amm_fee_amount_raw,
        t.amm_fee / pow(10, p.decimals) as amm_fee_amount,
        t.amm_fee / pow(10, p.decimals) * p.price as amm_fee_amount_usd,
        case when t.amm_fee = 0 or t.price = 0 then 0 else t.amm_fee / t.price end as amm_fee_percentage,
        t.royalty_fee as royalty_fee_amount_raw,
        t.royalty_fee / pow(10, p.decimals) as royalty_fee_amount,
        t.royalty_fee / pow(10, p.decimals) * p.price as royalty_fee_amount_usd,
        case when t.royalty_fee = 0 or t.price = 0 then 0 else t.royalty_fee / t.price end as royalty_fee_percentage,
        t.instruction,
        t.outer_instruction_index,
        coalesce(t.inner_instruction_index, 0) as inner_instruction_index
    from trades t
    left join {{ source('prices', 'usd') }} p on p.blockchain = 'solana'
        and to_base58(p.contract_address) = t.trade_token_mint
        and p.minute = date_trunc('minute', t.call_block_time)
        {% if is_incremental() %}
        and {{incremental_predicate('p.minute')}}
        {% endif %}
)

select * from raw_nft_trades
