-- Compare the legacy transactions.signer exclusion with account_activity.signed
-- over the recent 30-day window before removing the solana.transactions join.
-- Run against production or replace the source tables with the CI schema.

with fee_receivers(address) as (
    values
        ('8r2hZoDfk5hDWJ1sDujAi2Qr45ZyZw5EQxAXiMZWLKh2'),
        ('Cj297UauzMX64FU9dKJZRUBWszJ7tEWpVheasq4CfATV'),
        ('HKMh8nV3ysSofRi23LsfVGLGQKB415QAEfZT96kCcVj4'),
        ('7tQiiBdKoScWQkB1RmVuML7DBGnR31cuKPEtMM7Vy5SA'),
        ('4BBNEVRgrxVKv9f7pMNE788XM1tt379X9vNjpDH2KCL7'),
        ('47hEzz83VFR23rLTEeVm9A7eFzjJwjvdupPPmX3cePqF'),
        ('EMbqD9Y9jLXEa3RbCR8AsEW1kVa3EiJgDLVgvKh4qNFP'),
        ('Lk693UiTzQC4vobasRS1QGcYA9D6RGYLjHp1bWreQtM')
),

fee_activity as (
    select
        account_activity.block_time,
        account_activity.tx_id,
        account_activity.address as fee_receiver,
        account_activity.signed as fee_receiver_signed
    from solana.account_activity as account_activity
    join fee_receivers
        on fee_receivers.address = account_activity.address
    where account_activity.block_time >= current_timestamp - interval '30' day
        and account_activity.tx_success
        and account_activity.balance_change > 0
        and account_activity.tx_id !=
            'AT915GhHaLdGsdFkywx2uE6jqSXeyTauveYH2BQqWMyptGhUtjE6dcdr74ErELg79VY9apZ9Egiyc1VtA6Ddykb'
),

comparison as (
    select
        fee_activity.*,
        transactions.signer as primary_signer,
        coalesce(
            transactions.signer in (select address from fee_receivers),
            false
        ) as primary_signer_is_fee_receiver,
        coalesce(fee_activity.fee_receiver_signed, false)
            as fee_receiver_is_signed
    from fee_activity
    left join solana.transactions as transactions
        on transactions.id = fee_activity.tx_id
        and transactions.block_time = fee_activity.block_time
        and transactions.block_date = cast(
            date_trunc('day', fee_activity.block_time) as date
        )
)

select
    count(*) as fee_rows,
    count_if(primary_signer_is_fee_receiver) as primary_signer_fee_wallet_rows,
    count_if(fee_receiver_is_signed) as fee_receiver_signed_rows,
    count_if(
        primary_signer_is_fee_receiver = fee_receiver_is_signed
    ) as matching_rows,
    count_if(
        primary_signer_is_fee_receiver <> fee_receiver_is_signed
    ) as mismatching_rows,
    count_if(
        fee_receiver_is_signed and not primary_signer_is_fee_receiver
    ) as secondary_signer_only_rows,
    count_if(fee_receiver_signed is null) as null_signed_rows
from comparison;

-- If mismatching_rows > 0, rerun the same CTEs with this final SELECT instead:
--
-- select *
-- from comparison
-- where primary_signer_is_fee_receiver <> fee_receiver_is_signed
-- order by block_time desc
-- limit 100;
