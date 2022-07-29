-- Check manually collected sample of rows validated against etherscan
-- There should always be the same number of samples in the join as the seed
-- The test returns one row, if the left and right counts don't match


with count_sampled_rows as
(select
    count(d.wallet_address) as left_count,
    count(d.wallet_address) as right_count
from {{ ref('balances_ethereum_erc721_day') }} d
left join {{ ref('balances_ethereum_erc721_day_manual_seed') }} s
on d.wallet_address = s.wallet_address
and d.token_address = s.token_address
and d.day = s.day
and d.blockchain = s.blockchain
and d.tokenId = s.tokenId)


select *
from count_sampled_rows
where left_count!=right_count