-- trades between block 15432470 and 15432500
WITH trades as (
    select block_number,tx_hash,trade_category,nft_contract_address,token_id,amount_original,platform_fee_amount
    from {{ ref('nft_ethereum_trades_beta_ported') }}
    where project = 'sudoswap' and block_number >= 15432470 and block_number < 15432500
)
, examples as (
    select * from {{ ref('sudoswap_ethereum_example_trades') }}
)

, matched as (
    select
    coalesce(t.block_number, ex.block_number) as block_number
    , coalesce(t.tx_hash, ex.tx_hash) as tx_hash
    , coalesce(t.nft_contract_address, ex.nft_contract_address) as nft_contract_Address
    , coalesce(t.token_id, ex.token_id) as token_id
    , case when (t.amount_original = ex.amount_original) then true else false end as correct_amt_orig
    , case when (t.platform_fee_amount = ex.platform_fee_amount) then true else false end as correct_platform_fee
    , t.amount_original as reported_amount_original
    , ex.amount_original as seed_amount_original
    , t.platform_fee_amount as reported_platform_fee_amount
    , ex.platform_fee_amount as seed_platform_fee_amount
    from trades t
    full outer join examples ex
    on ex.block_number = t.block_number and ex.tx_hash=t.tx_hash
    and ex.nft_contract_address=t.nft_contract_address and ex.token_id=t.token_id
)

select * from matched
where not (correct_amt_orig and correct_platform_fee)
