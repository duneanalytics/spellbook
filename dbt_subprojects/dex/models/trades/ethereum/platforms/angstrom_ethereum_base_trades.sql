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
        controller_v1_contract_addr = '0x1746484EA5e11C75e009252c102C8C33e0315fD4',
        earliest_block = '22971781',
        blockchain = 'ethereum',
        project = 'angstrom',
        version = '1'
    )
    }}
)

SELECT * FROM dexs