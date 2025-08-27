{%- macro transfers_traces(blockchain) -%}
-- it's splitted to 2 operations and fetching from pre-materialized table to prevent doubling full-scan of traces 


select * from {{ ref('tokens_' ~ blockchain ~ '_base_transfers_traces') }}

union all
-- the wrapper deposit includes two transfers: native and wrapped, so we should add second one manually reversing from/to
select * from {{ ref('tokens_' ~ blockchain ~ '_base_transfers_traces_wrapped_token_deposit') }}



{%- endmacro -%}