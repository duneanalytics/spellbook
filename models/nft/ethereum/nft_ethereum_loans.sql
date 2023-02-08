{{ config(
        alias ='nft_loans',
        materialized = 'table'
)}}

with nftfi_base as (
    select evt_tx_hash, evt_block_time, borrower, lender, 
        collectionContract, tokenId, loanId, contract_address,
        loanPrincipalAmount as principal_raw,  loanERC20Denomination as currency, 
        case when loanDuration >= 86400 then loanDuration / 86400 else loanDuration end as duration,
        (cast(365 as double) / case when loanDuration >= 86400 then loanDuration / 86400 else loanDuration end) * (maximumRepaymentAmount- loanPrincipalAmount) * cast(1 as double) / loanPrincipalAmount * 100 AS apr
    from (
        select
            evt_tx_hash,
            evt_block_time,
            borrower,
            lender,
            nftCollateralContract as collectionContract,
            nftCollateralId as tokenId,
            loanERC20Denomination, 
            cast(loanPrincipalAmount as decimal) as loanPrincipalAmount,
            cast(loanDuration as decimal) as loanDuration,
            cast(maximumRepaymentAmount as decimal) as maximumRepaymentAmount,
            cast(loanId as bigint) as loanId,
            contract_address
        from {{ source('nftfi_ethereum','NFTfi_evt_LoanStarted') }}
        union all
        select 
            evt_tx_hash,
            evt_block_time,
            borrower,
            lender,
            get_json_object(loanTerms, '$.nftCollateralContract') as collectionContract,
            get_json_object(loanTerms, '$.nftCollateralId') as tokenId,
            get_json_object(loanTerms, '$.loanERC20Denomination') as loanERC20Denomination, 
            get_json_object(loanTerms, '$.loanPrincipalAmount') as loanPrincipalAmount,
            get_json_object(loanTerms, '$.loanDuration') as loanDuration,
            get_json_object(loanTerms, '$.maximumRepaymentAmount') as maximumRepaymentAmount,
            loanId,
            contract_address
        from {{ source('nftfi_ethereum','DirectLoanFixedOffer_evt_LoanStarted') }}
        union all
        select 
            evt_tx_hash,
            evt_block_time,
            borrower,
            lender,
            get_json_object(loanTerms, '$.nftCollateralContract') as collectionContract,
            get_json_object(loanTerms, '$.nftCollateralId') as tokenId,
            get_json_object(loanTerms, '$.loanERC20Denomination') as loanERC20Denomination, 
            get_json_object(loanTerms, '$.loanPrincipalAmount') as loanPrincipalAmount,
            get_json_object(loanTerms, '$.loanDuration') as loanDuration,
            get_json_object(loanTerms, '$.maximumRepaymentAmount') as maximumRepaymentAmount,
            loanId,
            contract_address
        from {{ source('nftfi_ethereum','DirectLoanFixedOfferRedeploy_evt_LoanStarted') }}
        union all
        select 
            evt_tx_hash,
            evt_block_time,
            borrower,
            lender,
            get_json_object(loanTerms, '$.nftCollateralContract') as collectionContract,
            get_json_object(loanTerms, '$.nftCollateralId') as tokenId,
            get_json_object(loanTerms, '$.loanERC20Denomination') as loanERC20Denomination, 
            get_json_object(loanTerms, '$.loanPrincipalAmount') as loanPrincipalAmount,
            get_json_object(loanTerms, '$.loanDuration') as loanDuration,
            get_json_object(loanTerms, '$.maximumRepaymentAmount') as maximumRepaymentAmount,
            loanId,
            contract_address
        from {{ source('nftfi_ethereum','DirectLoanFixedCollectionOffer_evt_LoanStarted') }}
    ) n 
    where loanPrincipalAmount> 0
), 

nftfi as (
    select l.*, r.evt_block_time as repay_time
    from nftfi_base l left join (
        select cast(loanId as bigint) as loanId, contract_address, evt_block_time, amountPaidToLender from {{ source('nftfi_ethereum','NFTfi_evt_LoanRepaid') }}
        union
        select loanId, contract_address, evt_block_time, amountPaidToLender from {{ source('nftfi_ethereum','DirectLoanFixedOffer_evt_LoanRepaid') }}
        union
        select loanId, contract_address, evt_block_time, amountPaidToLender from {{ source('nftfi_ethereum','DirectLoanFixedOfferRedeploy_evt_LoanRepaid') }}
        union
        select loanId, contract_address, evt_block_time, amountPaidToLender from {{ source('nftfi_ethereum','DirectLoanFixedCollectionOffer_evt_LoanRepaid') }}
    ) r on l.loanId=r.loanId and l.contract_address=r.contract_address
),

-- x2y2 loans
x2y2_base as (
    select evt_tx_hash, evt_block_time, borrower, lender, collectionContract, tokenId, principal_raw, currency, duration,
          365 * 1.0 / duration * (repayment_raw - principal_raw) * 1.0 / principal_raw * 100 as apr,
          contract_address, loanId
    from (
        select 
            evt_tx_hash, evt_block_time, borrower, lender,
            get_json_object(loanDetail, '$.nftAsset') as collectionContract,
            get_json_object(loanDetail, '$.nftTokenId') as tokenId,
            get_json_object(loanDetail, '$.borrowAmount') as principal_raw,
            get_json_object(loanDetail, '$.borrowAsset') as currency,
            get_json_object(loanDetail, '$.repayAmount')  as repayment_raw,
            get_json_object(loanDetail, '$.loanDuration') / 86400 as duration,
            contract_address, loanId
        from {{ source('xy3_ethereum','XY3_evt_LoanStarted') }}
    ) t
),

x2y2 as (
    select l.*, r.evt_block_time as repay_time
    from x2y2_base l left join {{ source('xy3_ethereum','XY3_evt_LoanRepaid') }} r on l.loanId=r.loanId and l.contract_address=r.contract_address
),

-- arcade loans
arcade_v1_deposits as (
    select cast(bundleId as decimal) as bundleId, evt_block_time, tokenAddress as collectionContract, tokenId, 'v1' as version
    from {{ source('pawnfi_ethereum','AssetWrapper_evt_DepositERC721') }} --- deposit NFTs to vault V1
    union all
    select cast(bundleId as decimal) as bundleId, evt_block_time, tokenAddress as collectionContract, tokenId, 'v1.2' as version
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
        from {{ source('pawnfi_ethereum','LoanCore_call_startLoan') }} p left join {{ source('pawnfi_ethereum','LoanCore_evt_LoanCreated') }} c on p.call_block_time=c.evt_block_time and p.call_tx_hash = c.evt_tx_hash
        where call_success=true
        union all
        select c.evt_tx_hash, evt_block_time, borrower, lender, c.contract_address, p.loanId, 'v1.2' as version,
            get_json_object(terms, '$.collateralTokenId') as bundleId,
            get_json_object(terms, '$.payableCurrency') as currency,
            get_json_object(terms, '$.principal')  as principal_raw,
            get_json_object(terms, '$.interest') as interest_raw,
            get_json_object(terms, '$.durationSecs') / 86400 as duration
        from {{ source('pawnfi_v2_ethereum','LoanCore_call_startLoan') }} p left join {{ source('pawnfi_v2_ethereum','LoanCore_evt_LoanCreated') }} c on p.call_block_time=c.evt_block_time and p.call_tx_hash = c.evt_tx_hash
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
    from arcade_v1 a left join arcade_v1_deposits d on a.evt_block_time>d.evt_block_time and a.bundleId=d.bundleId and a.version=d.version
    where a.evt_block_time is not null and d.evt_block_time is not null
) ,

-- arcade v2 loans
arcade_v2_wrappers as ( -- arcade asset wrappers
    select * from (values ('0x6e9b4c2f6bd57b7b924d29b5dcfca1273ecc94a2'), ('0x666faa632e5f7ba20a7fce36596a6736f87133be')) t(id)
),

arcade_v2_vault_created as (
    select vault, e.tokenId as vaultId, p.to as borrower, e.evt_tx_hash
    from {{ source('pawnfi_v201_ethereum','VaultFactory_evt_VaultCreated') }} p inner join {{ source('erc721_ethereum','evt_Transfer') }} e on p.evt_tx_hash=e.evt_tx_hash and p.evt_block_time=e.evt_block_time 
),

arcade_v2_vault_deposited_nfts as (
    select e.to as vault, `_1` as borrower, e.tokenId, e.contract_address, call_block_time, e.evt_tx_hash
    from {{ source('pawnfi_v201_ethereum','AssetVault_call_onERC721Received') }} p inner join {{ source('erc721_ethereum','evt_Transfer') }}  e on p.call_block_time=e.evt_block_time and p.call_tx_hash=e.evt_tx_hash
),

arcade_v2_vault_withdrawn_nfts as (
    select e.`from` as vault, p.to as borrower, p.tokenId, p.token as contract_address, p.call_block_time, e.evt_tx_hash
    from {{ source('pawnfi_v201_ethereum','AssetVault_call_withdrawERC721') }} p inner join {{ source('erc721_ethereum','evt_Transfer') }}  e on p.call_block_time=e.evt_block_time and p.call_tx_hash=e.evt_tx_hash
),

arcade_v2_vault_total_nfts as (
    select * 
    from (
        select d.vault, d.tokenId, d.contract_address as collectionContract, d.call_block_time as d_block_time, w.call_block_time as w_block_time, 
        row_number() over (partition by d.vault, d.tokenId, d.contract_address order by w.call_block_time) as r
        from arcade_v2_vault_deposited_nfts d left join arcade_v2_vault_withdrawn_nfts w on d.call_block_time<w.call_block_time and d.vault=w.vault and d.tokenId=w.tokenId and d.contract_address=w.contract_address
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
    left join {{ source('pawnfi_v201_ethereum',' LoanCore_evt_LoanClaimed') }} r
        on l.loanId=r.loanId
        and l.contract_address=r.contract_address
),

arcade_v2_loans_with_vaults as (
    select l.*, v.vault
    from arcade_v2 l left join arcade_v2_vault_created v on l.collateralId=v.vaultId
    where collateralAddress in (select * from arcade_v2_wrappers)
),

-- all arcade loans
arcade_loans_with_tokens as (
    select evt_tx_hash, evt_block_time, borrower, lender, collectionContract, tokenId, 
            principal_raw as p, currency, apr, duration, loanId, version, repay_time,
            count(tokenId) over (partition by evt_tx_hash) as num_items
    from arcade_v1_with_tokens
    union all
    select l.evt_tx_hash, l.evt_block_time, l.borrower, l.lender, t.collectionContract, t.tokenId, 
        l.principal_raw as p, l.currency, l.apr, l.duration, loanId, 'v2' as version, repay_time,
        count(tokenId) over (partition by l.evt_tx_hash) as num_items
    from arcade_v2_loans_with_vaults l inner join arcade_v2_vault_total_nfts t on l.vault=t.vault and l.evt_block_time>t.d_block_time and (t.w_block_time is null or l.evt_block_time<t.w_block_time)
    union all
    select evt_tx_hash, evt_block_time, borrower, lender, collateralAddress as collectionContract, collateralId as tokenId, 
        principal_raw as p, currency, apr, duration, loanId, 'v2' as version, repay_time,
        1 as num_items -- these are single item loans 
    from arcade_v2
    where collateralAddress not in (select * from arcade_v2_wrappers)
), 

arcade as (
    select *, p / num_items as principal_raw
    from arcade_loans_with_tokens
),

--benddao
benddao_base as (
    select evt_tx_hash, evt_block_time, 
        onBehalfOf as borrower, '0xdafce4acc2703a24f29d1321adaadf5768f54642' as lender,
        nftAsset as collectionContract, nftTokenId as tokenId,
        cast(amount as decimal) as principal_raw, reserveAsset as currency,
        user, loanId
    from {{ source('bend_ethereum','LendingPoolLoan_evt_LoanCreated') }}
),

benddao_ended as (
    select loanId, user, evt_block_time, 'repaid' as status
    from {{ source('bend_ethereum','LendingPoolLoan_evt_LoanRepaid') }}
    union all
    select loanId, user, evt_block_time, 'defaulted' as status
    from {{ source('bend_ethereum','LendingPoolLoan_evt_LoanLiquidated') }}
),

benddao as (
    select *
    from (
        select l.*, 
            case when status='repaid' then r.evt_block_time else null end as repay_time, 
            case when status is not null then datediff(r.evt_block_time, l.evt_block_time)  else null end as duration,
            null as apr
        from benddao_base l left join benddao_ended r on l.loanId=r.loanId --and l.user=r.user
    ) t 
),

-- -- aggregated loans
loans as (
    select evt_tx_hash,
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
           'NFTFi' as source
    from nftfi
    union all
    select evt_tx_hash,
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
           'X2Y2' as source
    from x2y2
    union all
    select evt_tx_hash,
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
           'Arcade' as source
    from arcade
    union all
    select evt_tx_hash,
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
           'BendDAO' as source
    from benddao
),

loans_with_prices as (
    select evt_tx_hash,
           evt_block_time,
           borrower,
           lender,
           collectionContract,
           tokenId,
           apr,
           duration,
           source,
           principal_raw / case
                               when currency = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN 1e18
                               when currency = '0x6b175474e89094c44da98b954eedeac495271d0f' THEN 1e18 * price
                               when currency = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' THEN 1e6 * price
               end               as eth,
           principal_raw * case
                               when currency = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN price / 1e18
                               when currency = '0x6b175474e89094c44da98b954eedeac495271d0f' THEN 1.0 / 1e18
                               when currency = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' THEN 1.0 / 1e6
               end               as usd,
           case
               when not repay_time is null then 'REPAID'
               when (repay_time is null and evt_block_time + interval '1' day * duration < current_date)
                   then 'DEFAULTED'
               else 'ACTIVE' end as status,
           currency,
           principal_raw
    from loans l
    left join {{ source('prices', 'usd') }} p
        on date_trunc('minute', evt_block_time) = MINUTE and MINUTE > cast ('2020-05-15' as timestamp)
        and p.contract_address='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    where not price is null
)

select l.*, coalesce(t.name, 'Awesome NFT') as collectionName 
from loans_with_prices l
left join {{ ref('tokens_nft') }} t
    on l.collectionContract=t.contract_address