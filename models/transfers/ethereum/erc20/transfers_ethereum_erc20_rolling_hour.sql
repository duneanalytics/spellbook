{{ config(
        alias = alias('erc20_rolling_hour')
        )
}}
/*
    note: this spell has not been migrated to dunesql, therefore is only a view on spark
        please migrate to dunesql to ensure up-to-date logic & data
*/
        select
            'ethereum' as blockchain,
            hour,
            wallet_address,
            token_address,
            symbol,
            NOW() as last_updated,
            row_number() over (partition by token_address, wallet_address order by hour desc) as recency_index,
            sum(amount_raw) over (
                partition by token_address, wallet_address order by hour
            ) as amount_raw,
            sum(amount) over (
                partition by token_address, wallet_address order by hour
            ) as amount
        from {{ ref('transfers_ethereum_erc20_agg_hour') }}