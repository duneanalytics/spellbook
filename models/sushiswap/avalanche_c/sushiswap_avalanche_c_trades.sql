{{ config(
    schema = 'sushiswap_avalanche_c'
    ,alias = 'trades'
    ,partition_by = ['block_date']
    ,materialized = 'incremental'
    ,file_format = 'delta'
    ,incremental_strategy = 'merge'
    ,unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address']
    ,post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                      "project",
                                      "sushiswap",
                                    \'["hosuke"]\') }}'
    )
}}