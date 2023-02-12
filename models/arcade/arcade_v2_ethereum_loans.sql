{{ config(
    schema = 'arcade_v2_ethereum',
    alias = 'loans',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "arcade",
                                \'["ivankitanovski", "hosuke"]\') }}'
    )
}}

with

-- arcade v2 loans
arcade_v2_wrappers as ( -- arcade asset wrappers
    select * from (values ('0x6e9b4c2f6bd57b7b924d29b5dcfca1273ecc94a2'), ('0x666faa632e5f7ba20a7fce36596a6736f87133be')) t(id)
),

arcade_v2_vault_created as (
    select vault, e.tokenId as vaultId, p.to as borrower, e.evt_tx_hash
    from {{ source('pawnfi_v201_ethereum', 'VaultFactory_evt_VaultCreated') }} p
    inner join
    {{ source('erc721_ethereum', 'evt_Transfer') }} e
        on p.evt_tx_hash=e.evt_tx_hash
        and p.evt_block_time=e.evt_block_time
),

arcade_v2_vault_deposited_nfts as (
    select e.to as vault, `_1` as borrower, e.tokenId, e.contract_address, call_block_time, e.evt_tx_hash
    from {{ source('pawnfi_v201_ethereum','AssetVault_call_onERC721Received') }} p
    inner join
    {{ source('erc721_ethereum','evt_Transfer') }} e
        on p.call_block_time=e.evt_block_time
        and p.call_tx_hash=e.evt_tx_hash
),

arcade_v2_vault_withdrawn_nfts as (
    select e.`from` as vault, p.to as borrower, p.tokenId, p.token as contract_address, p.call_block_time, e.evt_tx_hash
    from {{ source('pawnfi_v201_ethereum','AssetVault_call_withdrawERC721') }} p
    inner join
    {{ source('erc721_ethereum','evt_Transfer') }}  e
        on p.call_block_time=e.evt_block_time
        and p.call_tx_hash=e.evt_tx_hash
),

arcade_v2_vault_total_nfts as (
    select *
    from (
        select d.vault, d.tokenId, d.contract_address as collectionContract, d.call_block_time as d_block_time, w.call_block_time as w_block_time,
        row_number() over (partition by d.vault, d.tokenId, d.contract_address order by w.call_block_time) as r
        from arcade_v2_vault_deposited_nfts d
        left join arcade_v2_vault_withdrawn_nfts w
            on d.call_block_time<w.call_block_time
            and d.vault=w.vault
            and d.tokenId=w.tokenId
            and d.contract_address=w.contract_address
    ) t
    where r is null or r = 1
),

arcade_v2_base as (
    select *, 365 * 1.0 / duration * (interest_rate_raw / 1e18 / 100) as apr
    from (
        select call_tx_hash as evt_tx_hash, call_block_time as evt_block_time, borrower, lender, contract_address, output_loanId as loanId, 'v2' as version,
            get_json_object(terms, '$.collateralId') as collateralId,
            get_json_object(terms, '$.collateralAddress') as collateralAddress,
            get_json_object(terms, '$.payableCurrency') as currency,
            get_json_object(terms, '$.principal') as principal_raw,
            get_json_object(terms, '$.interestRate') as interest_rate_raw,
            get_json_object(terms, '$.durationSecs') / 86400 as duration
        from {{ source('pawnfi_v201_ethereum','LoanCore_call_startLoan') }}
        where call_success=true
        union all
        select call_tx_hash as evt_tx_hash, call_block_time as evt_block_time, borrower, lender, contract_address, output_newLoanId as loanId, 'v2-r' as version,
            get_json_object(terms, '$.collateralId') as collateralId,
            get_json_object(terms, '$.collateralAddress') as collateralAddress,
            get_json_object(terms, '$.payableCurrency') as currency,
            get_json_object(terms, '$.principal') as principal_raw,
            get_json_object(terms, '$.interestRate') as interest_rate_raw,
            get_json_object(terms, '$.durationSecs') / 86400 as duration
        from {{ source('pawnfi_v201_ethereum','LoanCore_call_rollover') }}
        where call_success=true
    ) t
),

arcade_v2 as (
    select l.*, case when (r.evt_block_time is null and l.evt_block_time + interval '1 day' * duration < current_date) then l.evt_block_time else null end as repay_time
    from arcade_v2_base l
    left join {{ source('pawnfi_v201_ethereum','LoanCore_evt_LoanClaimed') }} r
        on l.loanId=r.loanId
        and l.contract_address=r.contract_address
),

arcade_v2_loans_with_vaults as (
    select l.*, v.vault
    from arcade_v2 l left join arcade_v2_vault_created v on l.collateralId=v.vaultId
    where collateralAddress in (select * from arcade_v2_wrappers)
),

arcade_loans_with_tokens as (
    select l.evt_tx_hash,
           l.evt_block_time,
           l.borrower,
           l.lender,
           t.collectionContract,
           t.tokenId,
           l.principal_raw                                  as p,
           l.currency,
           l.apr,
           l.duration,
           loanId,
           'v2'                                             as version,
           repay_time,
           count(tokenId) over (partition by l.evt_tx_hash) as num_items
    from arcade_v2_loans_with_vaults l
    inner join arcade_v2_vault_total_nfts t
        on l.vault = t.vault
        and l.evt_block_time > t.d_block_time
        and (t.w_block_time is null or l.evt_block_time < t.w_block_time)

    union all

    select evt_tx_hash,
           evt_block_time,
           borrower,
           lender,
           collateralAddress as collectionContract,
           collateralId      as tokenId,
           principal_raw     as p,
           currency,
           apr,
           duration,
           loanId,
           'v2'              as version,
           repay_time,
           1                 as num_items -- these are single item loans
    from arcade_v2
    where collateralAddress not in (select * from arcade_v2_wrappers)
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
       'Arcade_v2'   as source
from arcade