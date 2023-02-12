{{ config(
    alias = 'loans',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "bend_dao",
                                \'["ivankitanovski", "hosuke"]\') }}'
    )
}}

with

--benddao
benddao_base as (
    select evt_tx_hash,
           evt_block_time,
           onBehalfOf                                   as borrower,
           '0xdafce4acc2703a24f29d1321adaadf5768f54642' as lender,
           nftAsset                                     as collectionContract,
           nftTokenId                                   as tokenId,
           amount                                       as principal_raw,
           reserveAsset                                 as currency,
        user, loanId
    from {{ source('bend_ethereum','LendingPoolLoan_evt_LoanCreated') }}
),

benddao_ended as (
    select loanId,
        user,
        evt_block_time,
        'repaid' as status
    from {{ source('bend_ethereum', 'LendingPoolLoan_evt_LoanRepaid') }}

    union all

    select loanId,
        user,
        evt_block_time,
        'defaulted' as status
    from {{ source('bend_ethereum','LendingPoolLoan_evt_LoanLiquidated') }}
),

benddao as (
    select *
    from (
        select l.*,
            case when status='repaid' then r.evt_block_time else null end as repay_time,
            case when status is not null then datediff(r.evt_block_time, l.evt_block_time)  else null end as duration,
            null as apr
        from benddao_base l
        left join
        benddao_ended r
            on l.loanId=r.loanId --and l.user=r.user
    ) t
)

select 'ethereum' as blockchain,
       evt_tx_hash,
       evt_block_time,
       repay_time,
       borrower,
       lender,
       collectionContract,
       tokenId,
       principal_raw,
       currency,
       apr,
       duration,
       'BendDAO'  as source
from benddao