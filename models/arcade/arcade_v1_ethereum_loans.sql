{{ config(
    schema = 'arcade_v1_ethereum',
    alias = 'loans',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "arcade",
                                \'["ivankitanovski", "hosuke"]\') }}'
    )
}}

with

-- arcade loans
arcade_v1_deposits as (
    select cast(bundleId as decimal) as bundleId,
           evt_block_time,
           tokenAddress              as collectionContract,
           tokenId,
           'v1'                      as version
    from {{ source('pawnfi_ethereum', 'AssetWrapper_evt_DepositERC721') }} --- deposit NFTs to vault V1

    union all

    select cast(bundleId as decimal) as bundleId,
           evt_block_time,
           tokenAddress              as collectionContract,
           tokenId,
           'v1.2'                    as version
    from {{ source('pawnfi_ethereum','AssetWrapperv102_evt_DepositERC721') }}
),

arcade_v1_base as (
    select *, 365 * 1.0 / duration * (interest_raw * 1.0 / principal_raw) * 100 as apr
    from (
        select c.evt_tx_hash, evt_block_time, borrower, lender, c.contract_address, p.loanId, 'v1' as version,
            get_json_object(terms, '$.collateralTokenId') as bundleId,
            get_json_object(terms, '$.payableCurrency') as currency,
            get_json_object(terms, '$.principal') as principal_raw,
            get_json_object(terms, '$.interest') as interest_raw,
            get_json_object(terms, '$.durationSecs') / 86400 as duration
        from {{ source('pawnfi_ethereum','LoanCore_call_startLoan') }} p
        left join
        {{ source('pawnfi_ethereum','LoanCore_evt_LoanCreated') }} c
        on p.call_block_time=c.evt_block_time
        and p.call_tx_hash = c.evt_tx_hash
        where call_success=true
        union all
        select c.evt_tx_hash, evt_block_time, borrower, lender, c.contract_address, p.loanId, 'v1.2' as version,
            get_json_object(terms, '$.collateralTokenId') as bundleId,
            get_json_object(terms, '$.payableCurrency') as currency,
            get_json_object(terms, '$.principal')  as principal_raw,
            get_json_object(terms, '$.interest') as interest_raw,
            get_json_object(terms, '$.durationSecs') / 86400 as duration
        from {{ source('pawnfi_v2_ethereum','LoanCore_call_startLoan') }} p
        left join
        {{ source('pawnfi_v2_ethereum','LoanCore_evt_LoanCreated') }} c
        on p.call_block_time=c.evt_block_time
        and p.call_tx_hash = c.evt_tx_hash
        where call_success=true
    ) t
),

arcade_v1 as (
    select l.*, r.evt_block_time as repay_time
    from arcade_v1_base l left join (
        select * from {{ source('pawnfi_ethereum','LoanCore_evt_LoanRepaid') }}
        union all
        select * from {{ source('pawnfi_v2_ethereum','LoanCore_evt_LoanRepaid') }}
    ) r on l.loanId=r.loanId and l.contract_address=r.contract_address
),
--v1 loans

arcade_v1_with_tokens as (
    select a.*, d.tokenId, d.collectionContract, d.evt_block_time as d_block_time
    from arcade_v1 a
    left join
    arcade_v1_deposits d
        on a.evt_block_time>d.evt_block_time
        and a.bundleId=d.bundleId and a.version=d.version
    where a.evt_block_time is not null and d.evt_block_time is not null
) ,

arcade_loans_with_tokens as (
    select evt_tx_hash,
           evt_block_time,
           borrower,
           lender,
           collectionContract,
           tokenId,
           principal_raw                                  as p,
           currency,
           apr,
           duration,
           loanId,
           version,
           repay_time,
           count(tokenId) over (partition by evt_tx_hash) as num_items
    from arcade_v1_with_tokens
),

arcade as (
    select *,
           p / num_items as principal_raw
    from arcade_loans_with_tokens
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
       'Arcade_v1'   as source
from arcade