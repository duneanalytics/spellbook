-- evt_index for 1inch LOP fills: numbered over ALL limits fills of a tx, BEFORE any
-- exclusion, so kept rows retain their dex_<blockchain>_trades merge keys when fills
-- are reclassified (merge can only upsert, never delete).
-- Both sides of the dex.trades / dex_aggregator.trades split must agree on this
-- numbering: oneinch_lop_own_trades computes it at query time, while
-- oneinch_lop_venue_settled_fills persists it for oneinch_lop_aggregator_trades.

{% macro oneinch_lop_evt_index() -%}
row_number() over(partition by tx_hash order by call_trace_address)
{%- endmacro %}
