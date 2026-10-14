-- Native-price coverage for tokens_arc_transfers.
--
-- Purpose:
-- price_usd and amount_usd are nullable by design -- an unpriced token is a legitimate state
-- -- so no uniqueness or not_null test on this model can tell "correctly unpriced" from "the
-- price join silently stopped matching". That second case is the one worth catching, and on
-- Arc it is easy to reach: native rows are keyed on dune.blockchains' token_address for the
-- chain (0x0000...0000) rather than a real contract, so a change to that address, to the
-- price feed's minute granularity, or to arc's coverage in prices.usd_with_native leaves the
-- chain's dominant asset unpriced without erroring. Symbol, decimals and amount are safe
-- from this -- tokens.erc20 lists 0x0000...0000 for arc too, and the coalesce prefers it -- so
-- price_usd and amount_usd are the only columns that go null, and nothing else signals it.
--
-- Native rows are the ones that must stay priced: Arc's native asset is USDC, priced from a
-- real coinpaprika id (usdc-usd-coin). So assert the weakest sound thing -- if the window
-- holds native rows at all, at least one carries a price. Deliberately not a per-row
-- assertion: an individual minute can legitimately have no mark.
--
-- Passes trivially on an empty window, so it does not fail before Arc has traffic.
--
-- Failure interpretation:
-- A row here means every native transfer in the window is unpriced. Check, in order: that
-- prices.usd_with_native still carries arc / 0x0000...0000 at 18 decimals (it comes from
-- prices_native_tokens' ('arc', 'usdc-usd-coin') entry joined to dune.blockchains); that its
-- minute granularity still matches the model's date_trunc('minute') join; and that arc still
-- has rows in the upstream price feed at all.

with native_price_coverage as (
  select
    count(*) as native_rows,
    count(price_usd) as priced_native_rows
  from {{ ref('tokens_arc_transfers') }}
  where token_standard = 'native'
    and {{ incremental_predicate('block_time') }}
)

select
  native_rows,
  priced_native_rows
from native_price_coverage
where native_rows > 0
  and priced_native_rows = 0
