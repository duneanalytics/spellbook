{{ config(
    alias = 'loans',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "x2y2",
                                \'["ivankitanovski", "hosuke"]\') }}'
    )
}}

with
-- x2y2 loans
x2y2_base as (
    select evt_tx_hash,
           evt_block_time,
           borrower,
           lender,
           collectionContract,
           tokenId,
           principal_raw,
           currency,
           duration,
           365 * 1.0 / duration * (repayment_raw - principal_raw) * 1.0 / principal_raw * 100 as apr,
           contract_address,
           loanId
    from (
        select
            evt_tx_hash, evt_block_time, borrower, lender,
            get_json_object(loanDetail, '$.nftAsset') as collectionContract,
            get_json_object(loanDetail, '$.nftTokenId') as tokenId,
            get_json_object(loanDetail, '$.borrowAmount') as principal_raw,
            get_json_object(loanDetail, '$.borrowAsset') as currency,
            get_json_object(loanDetail, '$.repayAmount')  as repayment_raw,
            get_json_object(loanDetail, '$.loanDuration') / 86400 as duration,
            contract_address, loanId, 'v1' as version
        from {{ source('xy3_ethereum','XY3_evt_LoanStarted') }}

        union all

        select
            evt_tx_hash, evt_block_time, borrower, lender,
            get_json_object(loanDetail, '$.nftAsset') as collectionContract,
            get_json_object(loanDetail, '$.nftTokenId') as tokenId,
            get_json_object(loanDetail, '$.borrowAmount') as principal_raw,
            get_json_object(loanDetail, '$.borrowAsset') as currency,
            get_json_object(loanDetail, '$.repayAmount')  as repayment_raw,
            get_json_object(loanDetail, '$.loanDuration') / 86400 as duration,
            contract_address, loanId, 'v2' as version
        from {{ source('xy3_ethereum','XY3_V2_evt_LoanStarted') }}

    ) t
),

x2y2 as (
    select l.*,
           r.evt_block_time as repay_time
    from x2y2_base l left join (
        select * from {{ source('xy3_ethereum','XY3_evt_LoanRepaid') }}
        {% if is_incremental() %}
        where evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        union all
        select * from {{ source('xy3_ethereum','XY3_V2_evt_LoanRepaid') }}
    ) r
    on l.loanId = r.loanId
    and l.contract_address = r.contract_address
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
       'X2Y2' as source
from x2y2