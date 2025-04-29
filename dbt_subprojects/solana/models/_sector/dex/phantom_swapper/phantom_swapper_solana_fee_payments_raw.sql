{{ config(
    schema = 'phantom_swapper_solana',
    alias = 'fee_payments_raw',
    partition_by = ['block_month'],
    materialized = 'incremental',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_month', 'tx_id', 'token_address']
   )
}}

{% set project_start_date = '2025-02-01' %} -- Not the true start date but queries time out if I go back farther
{% set blockchain = 'solana' %}
{% set wsol_token = 'So11111111111111111111111111111111111111112' %}

with
    fee_addresses as (
        -- Jupiter AND OKX transfer partner fee directly to a tokenaccount owned by 25mY,9Yj,tzv or 8ps
        SELECT fee_receiver
        FROM (
            VALUES
            ('25mYnjJ2MXHZH6NvTTdA63JvjgRVcuiaj6MRiEQNs1Dq')
            , ('9yj3zvLS3fDMqi1F8zhkaWfq8TZpZWHe6cz1Sgt7djXf')
            , ('tzvXws1qhmfdPkPcprezULCDQPAJqzPhbZ3SMrqRPNE')
            , ('8psNvWTrdNTiVRNzAgsou9kETXNJm2SXZyaKuJraVRtf')
        ) AS x (fee_receiver)
        UNION ALL 

        -- Jupiter transfers partner fee to a ReferralTokenAccount owned by a ReferralAccount owned by 25mY
        SELECT referraltokenaccount AS fee_receiver
        FROM (
            SELECT
            account_arguments[1] AS creator
            , account_arguments[3] AS referralaccount
            , account_arguments[4] AS referraltokenaccount
            FROM {{ source('solana','instruction_calls') }}
            WHERE executing_account = 'REFER4ZgmyYx9c6He5XfaTMiGfdLwRnkV4RPp9t9iF3'
            AND account_arguments[2] = '45ruCyfdRkWpRNGEqWzjCiXRHkZs8WXCLQ67Pnpye7Hp'
            AND TRY_CAST(data AS VARCHAR) like '%7d12465f%'
            AND block_time >= TIMESTAMP '2023-11-30' -- First date phantom ReferralTokenAccount created
            AND block_time < TIMESTAMP '2025-02-12' -- Last date phantom ReferralTokenAccount created
            )
        WHERE referralaccount IN (
            'CnmA6Zb8hLrG33AT4RTzKdGv1vKwRBKQQr8iNckvv8Yg'
            , '2rQZb9xqQGwoCMDkpabbzDB9wyPTjSPj9WNhJodTaRHm'
            , '9gnLg6NtVxaASvxtADLFKZ9s8yHft1jXb1Vu6gVKvh1J'
            , 'wtpXRqKLdGc7vpReogsRugv6EFCw4HBHcxm8pFcR84a'
            , 'D1NJy3Qq3RKBG29EDRj28ozbGwnhmM5yBUp8PonSYUnm'
        )
    ),
    fee_payments as (
        select
            block_time,
            cast(date_trunc('month', block_time) as date) as block_month,
            fee_receiver,
            if(
                balance_change > 0, balance_change / 1e9, token_balance_change
            ) as amount,
            if(
                balance_change > 0, '{{wsol_token}}', token_mint_address
            ) as token_address,
            tx_id
        from {{ source('solana','account_activity') }} as account_activity
        join
            fee_addresses
            on (
                (
                    fee_addresses.fee_receiver = account_activity.address
                    and balance_change > 0
                )
                or (
                    token_balance_owner = fee_addresses.fee_receiver
                    and token_balance_change > 0
                )
            )
        where
            {% if is_incremental() %} {{ incremental_predicate('block_time') }}
            {% else %} block_time >= TIMESTAMP '{{project_start_date}}'
            {% endif %} and tx_success
    ),
    -- Eliminate duplicates (e.g. both SOL + WSOL in a single transaction)
    aggregated_fee_payments_by_token_by_tx as (
        select
            block_time,
            block_month,
            token_address,
            fee_receiver,
            tx_id,
            sum(amount) as amount
        from fee_payments
        group by tx_id, token_address, fee_receiver, block_time, block_month
    )
select
    block_time,
    block_month,
    '{{blockchain}}' as blockchain,
    amount,
    token_address,
    fee_receiver,
    tx_id,
    row_number() over (
        partition by tx_id
        order by
            case when token_address = '{{wsol_token}}' then 0 else 1 end,
            token_address asc
    ) as index
from aggregated_fee_payments_by_token_by_tx
