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
            loanPrincipalAmount as loanPrincipalAmount,
            loanDuration as loanDuration,
            maximumRepaymentAmount as maximumRepaymentAmount,
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
           source
    from {{ ref('x2y2_ethereum_loans') }}
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
           source
    from {{ ref('arcade_v1_ethereum_loans') }}
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
           source
    from {{ ref('arcade_v2_ethereum_loans') }}
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
    from {{ ref('bend_dao_ethereum_loans') }}
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
               else 'ACTIVE' end as status
            ,
           currency,
           principal_raw
    from loans l
    left join {{ source('prices', 'usd') }} p
        on date_trunc('minute', evt_block_time) = p.minute
        and p.minute  > cast ('2020-05-15' as timestamp)
        and p.contract_address='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    where not price is null
)

select l.*,
       coalesce(t.name, 'Awesome NFT') as collectionName
from loans_with_prices l
left join {{ ref('tokens_nft') }} t
    on l.collectionContract=t.contract_address
order by evt_block_time asc