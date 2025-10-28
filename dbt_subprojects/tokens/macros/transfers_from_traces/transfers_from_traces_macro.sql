{%- macro transfers_from_traces_macro(blockchain) -%}
-- it's splitted to 2 operations and fetching from pre-materialized table to prevent doubling full-scan of traces 


select * from {{ ref('tokens_' ~ blockchain ~ '_transfers_from_traces_base') }}

union all
-- the wrapper deposit includes two transfers: native and wrapped, so need to add second one manually reversing from/to
select * from {{ ref('tokens_' ~ blockchain ~ '_transfers_from_traces_base_wrapper_deposits') }}



{%- endmacro -%}