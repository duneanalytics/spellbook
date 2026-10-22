{{
  config(
    schema = 'addresses_robinhood',
    alias = 'stats',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain', 'address'],
    filtering_columns = []
  )
}}

-- Build hourly addresses_events_robinhood.first_funded_by before the initial daily run.
-- The cross-project source lookup does not enforce this deployment dependency.
{{ addresses_stats('robinhood') }}
