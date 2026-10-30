{{
    config(
        schema = 'aero_arc',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{ aero_trades(blockchain = 'arc', cl_factory = '0x7f83235643181e440433400a027618cd50879751') }}
