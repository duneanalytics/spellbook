{{ config(
    schema = 'nftfi_ethereum',
    alias = 'loans',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "nftfi",
                                \'["ivankitanovski", "hosuke"]\') }}'
    )
}}

with nftfi_base as (
    select evt_tx_hash,
           evt_block_time,
           borrower,
           lender,
           collectionContract,
           tokenId,
           loanId,
           contract_address,
           loanPrincipalAmount                                                                            as principal_raw,
           loanERC20Denomination                                                                          as currency,
           case when loanDuration >= 86400 then loanDuration / 86400 else loanDuration end                as duration,
           (cast(365 as double) / case when loanDuration >= 86400 then loanDuration / 86400 else loanDuration end) *
           (maximumRepaymentAmount - loanPrincipalAmount) * cast(1 as double) / loanPrincipalAmount * 100 AS apr
    from (
        select evt_tx_hash,
               evt_block_time,
               borrower,
               lender,
               nftCollateralContract  as collectionContract,
               nftCollateralId        as tokenId,
               loanERC20Denomination,
               loanPrincipalAmount    as loanPrincipalAmount,
               loanDuration           as loanDuration,
               maximumRepaymentAmount as maximumRepaymentAmount,
               cast(loanId as bigint) as loanId,
               contract_address
        from {{ source('nftfi_ethereum', 'NFTfi_evt_LoanStarted') }}
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
        from {{ source('nftfi_ethereum', 'DirectLoanFixedOffer_evt_LoanStarted') }}
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
        from {{ source('nftfi_ethereum', 'DirectLoanFixedOfferRedeploy_evt_LoanStarted') }}
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
        select cast(loanId as bigint) as loanId, contract_address, evt_block_time, amountPaidToLender
        from {{ source('nftfi_ethereum', 'NFTfi_evt_LoanRepaid') }}
        union
        select loanId, contract_address, evt_block_time, amountPaidToLender
        from {{ source('nftfi_ethereum', 'DirectLoanFixedOffer_evt_LoanRepaid') }}
        union
        select loanId, contract_address, evt_block_time, amountPaidToLender
        from {{ source('nftfi_ethereum', 'DirectLoanFixedOfferRedeploy_evt_LoanRepaid') }}
        union
        select loanId, contract_address, evt_block_time, amountPaidToLender
        from {{ source('nftfi_ethereum','DirectLoanFixedCollectionOffer_evt_LoanRepaid') }}
    ) r on l.loanId=r.loanId and l.contract_address=r.contract_address
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
       'NFTFi'   as source
from nftfi