{% set blockchain = 'arc' %}

{{ config(
    schema = 'bridges_' + blockchain
    , alias = 'withdrawals'
    , materialized = 'incremental'
    , file_format = 'delta'
    , filtering_columns = ['block_date']
    , incremental_strategy = 'merge'
    , unique_key = ['block_month', 'tx_hash', 'evt_index', 'bridge_transfer_id']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
) }}

{% set bridges_platforms = ['bridges_arc_aero_v1_withdrawals'] %}

{{ bridges_withdrawals(blockchain, bridges_platforms) }}
