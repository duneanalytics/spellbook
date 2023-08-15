{{ config(
        tags = ['dunesql'],
        alias = alias('erc20_rolling_day'))
}}

        select
            'ethereum' as blockchain,
            day,
            wallet_address,
            token_address,
            symbol,
            NOW() as last_updated,
            row_number() over (partition by token_address, wallet_address order by day desc) as recency_index,
            sum(case when tr.amount_positive then cast(tr.amount_raw as double) else - cast(tr.amount_raw as double) end) over (
                partition by token_address, wallet_address order by day
            ) as amount_raw,
            sum(amount) over (
                partition by token_address, wallet_address order by day
            ) as amount
        from {{ ref('transfers_ethereum_erc20_agg_day') }}
