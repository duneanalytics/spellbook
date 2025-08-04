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
        angstrom_contract_addr = '0xb9c4cE42C2e29132e207d29Af6a7719065Ca6AeC'
        , blockchain = 'ethereum'
        , project = 'angstrom'
        , version = '2'
        
    )
    }}
)

SELECT * FROM dexs