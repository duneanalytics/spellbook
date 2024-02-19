{{
    config(
        schema = 'magiceden_mmm_solana'
        
        , alias = 'trades'
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

with
    royalty_logs as (
        with
            nested_logs as (
                select distinct
                    call_tx_id,
                    call_block_slot,
                    call_outer_instruction_index,
                    call_inner_instruction_index,
                    -- read all the fees from the log, if the log is truncated, no fees
                    cast(
                        json_extract_scalar(
                            json_parse(split(logs, ' ') [3]),
                            '$.royalty_paid'
                        ) as double
                    ) as royalty_paid,
                    cast(
                        json_extract_scalar(json_parse(split(logs, ' ') [3]), '$.total_price') as double
                    ) as total_price,
                    cast(
                        json_extract_scalar(json_parse(split(logs, ' ') [3]), '$.lp_fee') as double
                    ) as lp_fee,
                    logs
                from
                    (
                        (
                            select
                                call_tx_id,
                                call_block_slot,
                                call_outer_instruction_index,
                                call_inner_instruction_index,
                                call_log_messages
                            from
                                {{ source('magic_eden_solana','mmm_call_solFulfillBuy') }}
                            {% if is_incremental() %}
                            where {{incremental_predicate('call_block_time')}}
                            {% endif %}
                        )
                        union all
                        (
                            select
                                call_tx_id,
                                call_block_slot,
                                call_outer_instruction_index,
                                call_inner_instruction_index,
                                call_log_messages
                            from
                                {{ source('magic_eden_solana','mmm_call_solFulfillSell') }}
                            {% if is_incremental() %}
                            where {{incremental_predicate('call_block_time')}}
                            {% endif %}
                        )
                        union all
                        (
                            select
                                call_tx_id,
                                call_block_slot,
                                call_outer_instruction_index,
                                call_inner_instruction_index,
                                call_log_messages
                            from
                                {{ source('magic_eden_solana','mmm_call_solMip1FulfillBuy') }}
                            {% if is_incremental() %}
                            where {{incremental_predicate('call_block_time')}}
                            {% endif %}
                        )
                        union all
                        (
                            select
                                call_tx_id,
                                call_block_slot,
                                call_outer_instruction_index,
                                call_inner_instruction_index,
                                call_log_messages
                            from
                                {{ source('magic_eden_solana','mmm_call_solMip1FulfillSell') }}
                             {% if is_incremental() %}
                            where {{incremental_predicate('call_block_time')}}
                            {% endif %}
                       )
                        union all
                        (
                            select
                                call_tx_id,
                                call_block_slot,
                                call_outer_instruction_index,
                                call_inner_instruction_index,
                                call_log_messages
                            from
                                {{ source('magic_eden_solana','mmm_call_solOcpFulfillBuy') }}
                            {% if is_incremental() %}
                            where {{incremental_predicate('call_block_time')}}
                            {% endif %}
                        )
                        union all
                        (
                            select
                                call_tx_id,
                                call_block_slot,
                                call_outer_instruction_index,
                                call_inner_instruction_index,
                                call_log_messages
                            from
                                {{ source('magic_eden_solana','mmm_call_solOcpFulfillSell') }}
                            {% if is_incremental() %}
                            where {{incremental_predicate('call_block_time')}}
                            {% endif %}
                        )
                    )
                    left join unnest (call_log_messages) as log_messages (logs) ON True
                where
                    logs like 'Program log: {"lp_fee":%,"royalty_paid":%,"total_price":%}' --must log these fields. hopefully no other programs out there log them hahaha
                    and try(json_parse(split(logs, ' ') [3])) is not null --valid hex
            )
        select
            *,
            row_number() over (
                partition by
                    call_tx_id
                order by
                    call_outer_instruction_index asc,
                    call_inner_instruction_index asc
            ) as log_order
        from
            nested_logs
    ),
    priced_tokens as (
        select
            symbol,
            to_base58(contract_address) as token_mint_address
        from
            {{ ref('prices_usd_latest') }} p
        where
            p.blockchain = 'solana'
            {% if is_incremental() %}
            AND {{incremental_predicate('p.minute')}}
            {% endif %}
    ),
    trades as (
        select
            case
                when account_buyer = call_tx_signer then 'buy'
                else 'sell'
            end as trade_category,
            'SOL' as trade_token_symbol,
            'So11111111111111111111111111111111111111112' as trade_token_mint,
            total_price as price,
            makerFeeBp / 1e4 * rl.total_price as maker_fee,
            takerFeeBp / 1e4 * rl.total_price as taker_fee,
            tokenSize as token_size,
            rl.royalty_paid as royalty_fee,
            rl.lp_fee as amm_fee,
            trade.call_instruction_name as instruction,
            trade.account_tokenMint,
            trade.account_buyer,
            trade.account_seller,
            trade.call_outer_instruction_index as outer_instruction_index,
            trade.call_inner_instruction_index as inner_instruction_index,
            trade.call_block_time,
            trade.call_block_slot,
            trade.call_tx_id,
            trade.call_tx_signer
        from
            (
                (
                    select
                        call_instruction_name,
                        account_owner as account_buyer,
                        call_tx_signer as account_seller,
                        account_assetMint as account_tokenMint,
                        cast(
                            json_value(
                                args,
                                'strict $.SolFulfillBuyArgs.minPaymentAmount'
                            ) as double
                        ) as buyerPrice,
                        cast(
                            json_value(args, 'strict $.SolFulfillBuyArgs.assetAmount') as double
                        ) as tokenSize,
                        cast(
                            json_value(args, 'strict $.SolFulfillBuyArgs.makerFeeBp') as double
                        ) as makerFeeBp,
                        cast(
                            json_value(args, 'strict $.SolFulfillBuyArgs.takerFeeBp') as double
                        ) as takerFeeBp,
                        call_outer_instruction_index,
                        call_inner_instruction_index,
                        call_block_time,
                        call_block_slot,
                        call_tx_id,
                        call_tx_signer,
                        call_account_arguments,
                        row_number() over (
                            partition by
                                call_tx_id
                            order by
                                call_outer_instruction_index asc,
                                call_inner_instruction_index asc
                        ) as call_order
                    from
                        {{ source('magic_eden_solana','mmm_call_solFulfillBuy') }}
                    {% if is_incremental() %}
                    where {{incremental_predicate('call_block_time')}}
                    {% endif %}
                )
                union all
                (
                    select
                        call_instruction_name,
                        account_owner as account_buyer,
                        call_tx_signer as account_seller,
                        account_assetMint as account_tokenMint,
                        cast(
                            json_value(
                                args,
                                'strict $.SolFulfillBuyArgs.minPaymentAmount'
                            ) as double
                        ) as buyerPrice,
                        cast(
                            json_value(args, 'strict $.SolFulfillBuyArgs.assetAmount') as double
                        ) as tokenSize,
                        cast(
                            json_value(args, 'strict $.SolFulfillBuyArgs.makerFeeBp') as double
                        ) as makerFeeBp,
                        cast(
                            json_value(args, 'strict $.SolFulfillBuyArgs.takerFeeBp') as double
                        ) as takerFeeBp,
                        call_outer_instruction_index,
                        call_inner_instruction_index,
                        call_block_time,
                        call_block_slot,
                        call_tx_id,
                        call_tx_signer,
                        call_account_arguments,
                        row_number() over (
                            partition by
                                call_tx_id
                            order by
                                call_outer_instruction_index asc,
                                call_inner_instruction_index asc
                        ) as call_order
                    from
                        {{ source('magic_eden_solana','mmm_call_solMip1FulfillBuy') }}
                    {% if is_incremental() %}
                    where {{incremental_predicate('call_block_time')}}
                    {% endif %}
                )
                union all
                (
                    select
                        call_instruction_name,
                        account_owner as account_buyer,
                        call_tx_signer as account_seller,
                        account_assetMint as account_tokenMint,
                        cast(
                            json_value(
                                args,
                                'strict $.SolFulfillBuyArgs.minPaymentAmount'
                            ) as double
                        ) as buyerPrice,
                        cast(
                            json_value(args, 'strict $.SolFulfillBuyArgs.assetAmount') as double
                        ) as tokenSize,
                        cast(
                            json_value(args, 'strict $.SolFulfillBuyArgs.makerFeeBp') as double
                        ) as makerFeeBp,
                        cast(
                            json_value(args, 'strict $.SolFulfillBuyArgs.takerFeeBp') as double
                        ) as takerFeeBp,
                        call_outer_instruction_index,
                        call_inner_instruction_index,
                        call_block_time,
                        call_block_slot,
                        call_tx_id,
                        call_tx_signer,
                        call_account_arguments,
                        row_number() over (
                            partition by
                                call_tx_id
                            order by
                                call_outer_instruction_index asc,
                                call_inner_instruction_index asc
                        ) as call_order
                    from
                        {{ source('magic_eden_solana','mmm_call_solOcpFulfillBuy') }}
                    {% if is_incremental() %}
                    where {{incremental_predicate('call_block_time')}}
                    {% endif %}
                )
                union all
                (
                    select
                        call_instruction_name,
                        call_tx_signer as account_buyer,
                        account_owner as account_seller,
                        account_assetMint as account_tokenMint,
                        cast(
                            json_value(
                                args,
                                'strict $.SolFulfillSellArgs.maxPaymentAmount'
                            ) as double
                        ) as buyerPrice,
                        cast(
                            json_value(args, 'strict $.SolFulfillSellArgs.assetAmount') as double
                        ) as tokenSize,
                        cast(
                            json_value(args, 'strict $.SolFulfillSellArgs.makerFeeBp') as double
                        ) as makerFeeBp,
                        cast(
                            json_value(args, 'strict $.SolFulfillSellArgs.takerFeeBp') as double
                        ) as takerFeeBp,
                        call_outer_instruction_index,
                        call_inner_instruction_index,
                        call_block_time,
                        call_block_slot,
                        call_tx_id,
                        call_tx_signer,
                        call_account_arguments,
                        row_number() over (
                            partition by
                                call_tx_id
                            order by
                                call_outer_instruction_index asc,
                                call_inner_instruction_index asc
                        ) as call_order
                    from
                        {{ source('magic_eden_solana','mmm_call_solFulfillSell') }}
                    {% if is_incremental() %}
                    where {{incremental_predicate('call_block_time')}}
                    {% endif %}
                )
                union all
                (
                    select
                        call_instruction_name,
                        call_tx_signer as account_buyer,
                        account_owner as account_seller,
                        account_assetMint as account_tokenMint,
                        cast(
                            json_value(
                                args,
                                'strict $.SolMip1FulfillSellArgs.maxPaymentAmount'
                            ) as double
                        ) as buyerPrice,
                        cast(
                            json_value(
                                args,
                                'strict $.SolMip1FulfillSellArgs.assetAmount'
                            ) as double
                        ) as tokenSize,
                        cast(
                            json_value(
                                args,
                                'strict $.SolMip1FulfillSellArgs.makerFeeBp'
                            ) as double
                        ) as makerFeeBp,
                        cast(
                            json_value(
                                args,
                                'strict $.SolMip1FulfillSellArgs.takerFeeBp'
                            ) as double
                        ) as takerFeeBp,
                        call_outer_instruction_index,
                        call_inner_instruction_index,
                        call_block_time,
                        call_block_slot,
                        call_tx_id,
                        call_tx_signer,
                        call_account_arguments,
                        row_number() over (
                            partition by
                                call_tx_id
                            order by
                                call_outer_instruction_index asc,
                                call_inner_instruction_index asc
                        ) as call_order
                    from
                        {{ source('magic_eden_solana','mmm_call_solMip1FulfillSell') }}
                    {% if is_incremental() %}
                    where {{incremental_predicate('call_block_time')}}
                    {% endif %}
                )
                union all
                (
                    select
                        call_instruction_name,
                        call_tx_signer as account_buyer,
                        account_owner as account_seller,
                        account_assetMint as account_tokenMint,
                        cast(
                            json_value(
                                args,
                                'strict $.SolOcpFulfillSellArgs.maxPaymentAmount'
                            ) as double
                        ) as buyerPrice,
                        cast(
                            json_value(
                                args,
                                'strict $.SolOcpFulfillSellArgs.assetAmount'
                            ) as double
                        ) as tokenSize,
                        cast(
                            json_value(args, 'strict $.SolOcpFulfillSellArgs.makerFeeBp') as double
                        ) as makerFeeBp,
                        cast(
                            json_value(args, 'strict $.SolOcpFulfillSellArgs.takerFeeBp') as double
                        ) as takerFeeBp,
                        call_outer_instruction_index,
                        call_inner_instruction_index,
                        call_block_time,
                        call_block_slot,
                        call_tx_id,
                        call_tx_signer,
                        call_account_arguments,
                        row_number() over (
                            partition by
                                call_tx_id
                            order by
                                call_outer_instruction_index asc,
                                call_inner_instruction_index asc
                        ) as call_order
                    from
                        {{ source('magic_eden_solana','mmm_call_solOcpFulfillSell') }}
                    {% if is_incremental() %}
                    where {{incremental_predicate('call_block_time')}}
                    {% endif %}
                )
            ) trade
            left join royalty_logs rl ON trade.call_tx_id = rl.call_tx_id
            and trade.call_block_slot = rl.call_block_slot
            and trade.call_order = rl.log_order
            left join priced_tokens pt ON contains(
                trade.call_account_arguments,
                pt.token_mint_address
            )
    ),
    raw_nft_trades as (
        select
            'solana' as blockchain,
            'magiceden' as project,
            'mmm' as version,
            t.call_block_time as block_time,
            'secondary' as trade_type,
            token_size as number_of_items,
            t.trade_category,
            t.account_buyer as buyer,
            t.account_seller as seller,
            t.price as amount_raw,
            t.price / pow(10, p.decimals) as amount_original,
            t.price / pow(10, p.decimals) * p.price as amount_usd,
            t.trade_token_symbol as currency_symbol,
            t.trade_token_mint as currency_address,
            cast(null as varchar) as account_merkle_tree,
            cast(null as bigint) leaf_id,
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
            case
                when t.taker_fee = 0
                or t.price = 0 then 0
                else t.taker_fee / t.price
            end as taker_fee_percentage,
            t.maker_fee as maker_fee_amount_raw,
            t.maker_fee / pow(10, p.decimals) as maker_fee_amount,
            t.maker_fee / pow(10, p.decimals) * p.price as maker_fee_amount_usd,
            case
                when t.maker_fee = 0
                or t.price = 0 then 0
                else t.maker_fee / t.price
            end as maker_fee_percentage,
            t.amm_fee as amm_fee_amount_raw,
            t.amm_fee / pow(10, p.decimals) as amm_fee_amount,
            t.amm_fee / pow(10, p.decimals) * p.price as amm_fee_amount_usd,
            case
                when t.amm_fee = 0
                or t.price = 0 then 0
                else t.amm_fee / t.price
            end as amm_fee_percentage,
            t.royalty_fee as royalty_fee_amount_raw,
            t.royalty_fee / pow(10, p.decimals) as royalty_fee_amount,
            t.royalty_fee / pow(10, p.decimals) * p.price as royalty_fee_amount_usd,
            case
                when t.royalty_fee = 0
                or t.price = 0 then 0
                else t.royalty_fee / t.price
            end as royalty_fee_percentage,
            t.instruction,
            t.outer_instruction_index,
            coalesce(t.inner_instruction_index, 0) as inner_instruction_index
        from
            trades t
            left join {{ source('prices', 'usd') }} p ON p.blockchain = 'solana' 
            and to_base58(p.contract_address) = t.trade_token_mint
            and p.minute = date_trunc('minute', t.call_block_time)
            {% if is_incremental() %}
            and {{incremental_predicate('p.minute')}}
            {% endif %}
    )
select
    *
from
    raw_nft_trades
