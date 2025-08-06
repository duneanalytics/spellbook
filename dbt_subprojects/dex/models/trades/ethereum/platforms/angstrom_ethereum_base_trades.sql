{{ config(
    schema = 'angstrom_ethereum'
    , alias = 'base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}


WITH dexs AS
(
    {{
    angstrom_bundle_volume_events(
        angstrom_contract_addr = '0x0000000aa232009084Bd71A5797d089AA4Edfad4',
        controller_v1_contract_addr = '0xFE77113460CF1833c4440FD17B4463f472010e10',
        blockchain = 'ethereum',
        project = 'angstrom',
        version = '1'
    )
    }}
)

SELECT * FROM dexs