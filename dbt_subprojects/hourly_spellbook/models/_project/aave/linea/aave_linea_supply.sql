{{
  config(
    schema = 'aave_linea',
    alias = 'supply',
    materialized = 'view'
    , post_hook='{{ hide_spells() }}'
  )
}}

{{
  lending_aave_compatible_supply_view(
    blockchain = 'linea'
  )
}} 