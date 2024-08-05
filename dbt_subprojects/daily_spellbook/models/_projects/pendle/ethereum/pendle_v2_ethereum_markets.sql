{% set blockchain = 'ethereum' %}


{{
    config(
        schema = 'pendle' + blockchain,
        alias = 'swaps_raw',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain','project','version','market', 'pt', 'sy', 'yt']
    )
}}


{{
    pendle_markets(
        blockchain = blockchain,
        project = 'pendle',
        version = '2',
        project_decoded_as = 'pendle'
        create_market_table = 'PendleMarketFactory_evt_CreateNewMarket',
        create_yield_table = 'PendleYieldContractFactory_evt_CreateYieldContract',
        start_date = '2022-11-23'
    )
}}
