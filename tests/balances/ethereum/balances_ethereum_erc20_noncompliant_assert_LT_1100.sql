-- There were 1005 noncompliant tokens at time of model creation
-- This tests confirms there are not more then 1100.
-- If this threshold is exceeded, we should investigate as to why.

select count(*) as count
from {{ ref('balances_ethereum_erc20_noncompliant') }}
having count(*) > 1100
