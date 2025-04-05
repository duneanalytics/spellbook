{{ 
    config(
        schema = 'safe_goerli',
        alias = 'eth_transfers',
        partition_by = ['block_month'],
        on_schema_change='fail',
        materialized='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key = ['block_date', 'address', 'tx_hash', 'trace_address'],
        post_hook='{{ expose_spells(blockchains = \'["goerli"]\',
                                    spell_type = "project",
                                    spell_name = "safe",
                                    contributors = \'["tschubotz", "hosuke"]\') }}'
    )
}}

{{
    safe_native_transfers(
        blockchain = 'goerli',
        native_token_symbol = 'ETH',
        project_start_date = '2019-09-03'
    )
}}
