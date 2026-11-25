-- A liquidation reduces the borrower's debt once. Markets that route a liquidation through their
-- repayment path emit both a repayment event and a liquidation event for it, and a base borrow model
-- must report that reduction as `borrow_liquidation` only.
-- This test returns every `repay` row that restates a `borrow_liquidation` row later in the same
-- transaction: same market, same borrower, same raw amount.
-- match_repayer: also require the repay row's `repayer` to be the liquidator. Set it to false for
-- protocols whose repayment event does not name the payer.
{% test lending_base_borrow_liquidation_counted_once(model, match_repayer=true) %}

select
  r.tx_hash,
  r.project_contract_address,
  r.borrower,
  r.amount_raw,
  r.evt_index as repay_evt_index,
  l.evt_index as liquidation_evt_index
from {{ model }} r
  inner join {{ model }} l
    on l.block_time = r.block_time
    and l.tx_hash = r.tx_hash
    and l.project_contract_address = r.project_contract_address
    and l.borrower = r.borrower
    and l.amount_raw = r.amount_raw
    and l.evt_index > r.evt_index
    {%- if match_repayer %}
    and l.liquidator = r.repayer
    {%- endif %}
where r.transaction_type = 'repay'
  and l.transaction_type = 'borrow_liquidation'

{% endtest %}
